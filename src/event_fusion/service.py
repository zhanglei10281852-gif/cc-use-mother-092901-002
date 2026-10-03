"""事件可信合成服务：接入、发布策略、历史重演。

发布策略要点：
* 重复报文（同事件+来源+外部编号）直接去重，绝不重复触发发布；
* 只有判断发生实质变化（状态/类型/车道变化、置信度显著变化、封存）才发布；
* 每次发布绑定当时判断、全部证据（含被排除与撤回项及原因）与规则版本/指纹。
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .clock import Clock, SystemClock
from .contracts import EventObservation, ObservationKind, SourceRef
from .engine import (
    ACCEPTED,
    DUPLICATE,
    EXCLUDED,
    RECURRENCE,
    WITHDRAWN,
    FusionEngine,
    IngestResult,
    observation_id,
)
from .models import Judgment, JudgmentStatus, TrafficSnapshot
from .rules import RuleRegistry
from .sources import SourceRegistry
from .store import EventLog, RecordType


@dataclass(frozen=True)
class IngestResponse:
    observation_id: str
    classification: str
    reason: str
    published: TrafficSnapshot | None
    judgment: Judgment | None

    def to_dict(self) -> dict:
        return {
            "observation_id": self.observation_id,
            "classification": self.classification,
            "reason": self.reason,
            "published": self.published.to_dict() if self.published else None,
            "judgment": self.judgment.to_dict() if self.judgment else None,
        }


class FusionService:
    def __init__(
        self,
        clock: Clock | None = None,
        log_path: str | Path | None = None,
        sources: SourceRegistry | None = None,
        rules: RuleRegistry | None = None,
    ) -> None:
        self.clock = clock or SystemClock()
        self.sources = sources or SourceRegistry.default()
        self.rules = rules or RuleRegistry.default()
        self.log = EventLog(log_path)
        self.engine = FusionEngine(self.sources, self.rules)
        self._publications: list[TrafficSnapshot] = []
        self._sequences: dict[str, int] = {}
        if log_path is not None:
            self._rebuild_from_log()

    # ------------------------------------------------------------ 写入路径

    def ingest_observation(
        self, obs: EventObservation, received_at: datetime | None = None
    ) -> IngestResponse:
        now = received_at or self.clock.now()
        if now.tzinfo is None:
            raise ValueError("接收时刻必须携带时区信息")
        oid = observation_id(obs)
        if self.engine.observation(oid) is not None:
            # 服务层双保险：日志已存在的编号也不允许再触发任何副作用。
            result = IngestResult(
                oid, DUPLICATE, "重复报文已去重，不触发合成与发布", self.engine.judgment(obs.event_key)
            )
            return IngestResponse(oid, result.classification, result.reason, None, result.judgment)

        self.log.append(
            now,
            RecordType.OBSERVATION,
            {
                "observation_id": oid,
                "event_key": obs.event_key,
                "kind": obs.kind.value,
                "location_code": obs.location_code,
                "confidence": obs.confidence,
                "source": {
                    "source_type": obs.source.source_type,
                    "external_id": obs.source.external_id,
                    "observed_at": obs.source.observed_at.isoformat(),
                },
            },
        )
        result = self.engine.ingest(obs, now)
        return self._after_change(obs.event_key, result, now)

    def revise_observation(
        self,
        event_key: str,
        source_type: str,
        external_id: str,
        new_kind: ObservationKind,
        new_location_code: str,
        new_confidence: float,
        new_observed_at: datetime | None = None,
        received_at: datetime | None = None,
    ) -> IngestResponse:
        """来源对自己旧报文的修订：撤回旧报文并以修订版重新接入，两者均留痕。"""
        now = received_at or self.clock.now()
        revision_no = 2
        while self.engine.observation(
            f"{event_key}|{source_type}|{external_id}#rev{revision_no}"
        ) is not None:
            revision_no += 1
        revised_external_id = f"{external_id}#rev{revision_no}"

        self.withdraw(
            event_key, source_type, external_id,
            note="来源修订，撤回旧报文", received_at=now,
        )
        revised = EventObservation(
            event_key=event_key,
            kind=new_kind,
            location_code=new_location_code,
            source=SourceRef(source_type, revised_external_id, new_observed_at or now),
            confidence=new_confidence,
        )
        return self.ingest_observation(revised, received_at=now)

    def withdraw(
        self,
        event_key: str,
        source_type: str,
        external_id: str,
        note: str = "",
        received_at: datetime | None = None,
    ) -> IngestResponse | None:
        now = received_at or self.clock.now()
        oid = f"{event_key}|{source_type}|{external_id}"
        self.log.append(
            now,
            RecordType.WITHDRAWAL,
            {
                "observation_id": oid,
                "event_key": event_key,
                "source_type": source_type,
                "external_id": external_id,
                "note": note,
            },
        )
        result = self.engine.withdraw(event_key, source_type, external_id, now)
        if result is None:
            return None  # 先于观测到达的撤回，已登记，待观测到达时生效
        return self._after_change(event_key, result, now)

    def _after_change(
        self, event_key: str, result: IngestResult, now: datetime
    ) -> IngestResponse:
        published = None
        # 撤回可能使判断降级，需要尝试发布；被排除与重复报文绝不触发发布。
        if result.classification in (ACCEPTED, RECURRENCE, WITHDRAWN) or result.sealed:
            published = self._maybe_publish(event_key, now)
        return IngestResponse(
            result.observation_id,
            result.classification,
            result.reason,
            published,
            result.judgment,
        )

    # ------------------------------------------------------------ 发布

    def _maybe_publish(self, event_key: str, now: datetime) -> TrafficSnapshot | None:
        judgment = self.engine.judgment(event_key)
        if judgment is None or judgment.status is JudgmentStatus.NONE:
            return None
        latest = self._latest_publication(event_key)
        # 从无发布基线时，不确定结论不向下游下发（没有需要纠正的旧结论）。
        if latest is None and judgment.status is JudgmentStatus.UNCERTAIN:
            return None
        if latest is not None and not self._is_material_change(latest.judgment, judgment, now):
            return None
        return self._publish(event_key, judgment, now, sealed=judgment.status is JudgmentStatus.CLEARED)

    def _is_material_change(
        self, previous: Judgment, current: Judgment, now: datetime
    ) -> bool:
        if current.status is JudgmentStatus.CLEARED and previous.status is not JudgmentStatus.CLEARED:
            return True
        if current.status != previous.status:
            return True
        if current.kind != previous.kind:
            return True
        if current.location_code != previous.location_code:
            return True
        if current.episode != previous.episode:
            return True  # 复发开启新片段必须下发
        rules = self.rules.version_at(now)
        return abs(current.confidence - previous.confidence) >= rules.significant_confidence_delta

    def _publish(
        self, event_key: str, judgment: Judgment, now: datetime, sealed: bool
    ) -> TrafficSnapshot:
        rules = self.rules.version_at(now)
        sequence = self._sequences.get(event_key, 0) + 1
        self._sequences[event_key] = sequence
        previous = self._latest_publication(event_key)
        publication = TrafficSnapshot(
            publication_id=f"PUB-{event_key}-{sequence:04d}",
            event_key=event_key,
            sequence=sequence,
            published_at=now,
            judgment=judgment,
            evidence=self.engine.evidence(event_key),
            rules_version=rules.version,
            rules_fingerprint=rules.fingerprint,
            supersedes=previous.publication_id if previous else None,
            sealed=sealed,
        )
        self._publications.append(publication)
        self.log.append(
            now,
            RecordType.PUBLICATION,
            {"snapshot": publication.to_dict()},
        )
        return publication

    def _latest_publication(self, event_key: str) -> TrafficSnapshot | None:
        for snapshot in reversed(self._publications):
            if snapshot.event_key == event_key:
                return snapshot
        return None

    # ------------------------------------------------------------ 查询

    def current_judgment(self, event_key: str) -> Judgment | None:
        return self.engine.judgment(event_key)

    def evidence(self, event_key: str):
        return self.engine.evidence(event_key)

    def publications(self, event_key: str | None = None) -> list[TrafficSnapshot]:
        if event_key is None:
            return list(self._publications)
        return [p for p in self._publications if p.event_key == event_key]

    def event_keys(self) -> list[str]:
        return sorted(
            {p.event_key for p in self._publications} | set(self.engine.all_event_keys())
        )

    # ------------------------------------------------------------ 重演

    def replay_at(self, as_of: datetime) -> "ReplayView":
        """按指定时点在隔离引擎中重演日志，绝不触碰线上状态与历史发布。"""
        if as_of.tzinfo is None:
            raise ValueError("重演时点必须携带时区信息")
        view_engine = FusionEngine(self.sources, self.rules)

        def apply_observation(record) -> None:
            payload = record.payload
            source = payload["source"]
            obs = EventObservation(
                event_key=payload["event_key"],
                kind=ObservationKind(payload["kind"]),
                location_code=payload["location_code"],
                source=SourceRef(
                    source["source_type"],
                    source["external_id"],
                    datetime.fromisoformat(source["observed_at"]),
                ),
                confidence=payload["confidence"],
            )
            view_engine.ingest(obs, record.recorded_at)

        def apply_withdrawal(record) -> None:
            payload = record.payload
            view_engine.withdraw(
                payload["event_key"],
                payload["source_type"],
                payload["external_id"],
                record.recorded_at,
            )

        self.log.replay(
            {
                RecordType.OBSERVATION: apply_observation,
                RecordType.WITHDRAWAL: apply_withdrawal,
            },
            cutoff=as_of,
        )
        # 以重演时点为"现在"重算未封存片段（新鲜度按当时计算）。
        view_engine.refresh_open_episodes(as_of)

        publications_as_of = [
            _snapshot_from_record(record)
            for record in self.log.records
            if record.type == RecordType.PUBLICATION and record.recorded_at <= as_of
        ]
        later_evidence = {
            oid: envelope
            for oid, envelope in self.engine.all_envelopes().items()
            if envelope.received_at > as_of
        }
        return ReplayView(
            as_of=as_of,
            engine=view_engine,
            publications=publications_as_of,
            live_engine=self.engine,
            later_evidence=later_evidence,
            rules_registry=self.rules,
        )

    # ------------------------------------------------------------ 重建

    def _rebuild_from_log(self) -> None:
        """进程重启后由仅追加日志重建内存状态；发布记录原样恢复，不重新发布。"""

        def apply_observation(record) -> None:
            payload = record.payload
            source = payload["source"]
            obs = EventObservation(
                event_key=payload["event_key"],
                kind=ObservationKind(payload["kind"]),
                location_code=payload["location_code"],
                source=SourceRef(
                    source["source_type"],
                    source["external_id"],
                    datetime.fromisoformat(source["observed_at"]),
                ),
                confidence=payload["confidence"],
            )
            self.engine.ingest(obs, record.recorded_at)

        def apply_withdrawal(record) -> None:
            payload = record.payload
            self.engine.withdraw(
                payload["event_key"],
                payload["source_type"],
                payload["external_id"],
                record.recorded_at,
            )

        def apply_publication(record) -> None:
            self._publications.append(_snapshot_from_record(record))
            snapshot = self._publications[-1]
            self._sequences[snapshot.event_key] = max(
                self._sequences.get(snapshot.event_key, 0), snapshot.sequence
            )

        self.log.replay(
            {
                RecordType.OBSERVATION: apply_observation,
                RecordType.WITHDRAWAL: apply_withdrawal,
                RecordType.PUBLICATION: apply_publication,
            }
        )


@dataclass(frozen=True)
class ReplayView:
    as_of: datetime
    engine: FusionEngine
    publications: list[TrafficSnapshot]
    live_engine: FusionEngine
    later_evidence: dict
    rules_registry: RuleRegistry

    def compare(self, event_key: str) -> dict:
        then_judgment = self.engine.judgment(event_key)
        now_judgment = self.live_engine.judgment(event_key)
        later = [
            {
                "observation_id": oid,
                "received_at": envelope.received_at.isoformat(),
                "kind": envelope.obs.kind.value,
                "location_code": envelope.obs.location_code,
                "source_type": envelope.obs.source.source_type,
                "external_id": envelope.obs.source.external_id,
                "withdrawn": envelope.withdrawn,
                "excluded_reason": envelope.excluded_reason,
            }
            for oid, envelope in self.later_evidence.items()
            if envelope.obs.event_key == event_key
        ]
        rules_then = self.rules_registry.version_at(self.as_of)
        return {
            "event_key": event_key,
            "as_of": self.as_of.isoformat(),
            "rules_version_then": rules_then.version,
            "rules_fingerprint_then": rules_then.fingerprint,
            "judgment_then": then_judgment.to_dict() if then_judgment else None,
            "judgment_now": now_judgment.to_dict() if now_judgment else None,
            "changed_fields": _diff_judgments(then_judgment, now_judgment),
            "evidence_arrived_after": later,
            "publications_at_then": self.publications_for(event_key),
            "history_unchanged": True,
            "note": "重演结果只用于对比；截至该时点的发布快照取自不可变日志，未被改写。",
        }

    def publications_for(self, event_key: str) -> list[dict]:
        return [p.to_dict() for p in self.publications if p.event_key == event_key]


def _diff_judgments(then: Judgment | None, now: Judgment | None) -> dict:
    if then is None and now is None:
        return {}
    if then is None:
        return {"created": now.to_dict()}
    if now is None:
        return {"removed": True}
    fields = ("status", "kind", "location_code", "score", "confidence", "episode", "cleared_at")
    diff: dict = {}
    for name in fields:
        left = getattr(then, name)
        right = getattr(now, name)
        left_value = left.value if hasattr(left, "value") else (left.isoformat() if hasattr(left, "isoformat") else left)
        right_value = right.value if hasattr(right, "value") else (right.isoformat() if hasattr(right, "isoformat") else right)
        if left_value != right_value:
            diff[name] = {"then": left_value, "now": right_value}
    return diff


def _snapshot_from_record(record) -> TrafficSnapshot:
    """发布记录是不可变事实，重建时直接还原，不重新计算。"""
    from .models import EvidenceItem

    data = record.payload["snapshot"]
    judgment_data = data["judgment"]
    judgment = Judgment(
        event_key=judgment_data["event_key"],
        status=JudgmentStatus(judgment_data["status"]),
        episode=judgment_data["episode"],
        kind=judgment_data["kind"],
        location_code=judgment_data["location_code"],
        score=judgment_data["score"],
        confidence=judgment_data["confidence"],
        opposition_score=judgment_data["opposition_score"],
        active_since=(
            datetime.fromisoformat(judgment_data["active_since"])
            if judgment_data["active_since"]
            else None
        ),
        last_observed_at=(
            datetime.fromisoformat(judgment_data["last_observed_at"])
            if judgment_data["last_observed_at"]
            else None
        ),
        cleared_at=(
            datetime.fromisoformat(judgment_data["cleared_at"])
            if judgment_data["cleared_at"]
            else None
        ),
        rationale=tuple(judgment_data["rationale"]),
        evidence_ids=tuple(judgment_data["evidence_ids"]),
    )

    def _evidence(item: dict) -> EvidenceItem:
        return EvidenceItem(
            observation_id=item["observation_id"],
            kind=item["kind"],
            location_code=item["location_code"],
            source_type=item["source_type"],
            external_id=item["external_id"],
            observed_at=datetime.fromisoformat(item["observed_at"]),
            received_at=datetime.fromisoformat(item["received_at"]),
            confidence=item["confidence"],
            capability=item["capability"],
            age_seconds=item["age_seconds"],
            weight=item["weight"],
            supports_winner=item["supports_winner"],
            withdrawn=item["withdrawn"],
            excluded_reason=item["excluded_reason"],
        )

    return TrafficSnapshot(
        publication_id=data["publication_id"],
        event_key=data["event_key"],
        sequence=data["sequence"],
        published_at=datetime.fromisoformat(data["published_at"]),
        judgment=judgment,
        evidence=tuple(_evidence(item) for item in data["evidence"]),
        rules_version=data["rules_version"],
        rules_fingerprint=data["rules_fingerprint"],
        supersedes=data.get("supersedes"),
        sealed=data.get("sealed", False),
    )
