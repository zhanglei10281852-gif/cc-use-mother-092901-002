"""面向下游发布的路况快照账本。

账本只增不改：快照一旦写入，其内容、证据清单、规则版本与前序哈希即被冻结，
后续任何修订、撤回、迟到证据或规则升级都只会产生新快照，不能改写历史。
"""

import hashlib
import json
from dataclasses import dataclass

from .engine import EventJudgment


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    sequence: int
    published_at: object  # datetime
    sealed: bool
    rules_version: str
    as_of: object  # datetime
    journal_seq_max: int
    event_key: str
    episode_id: str
    status: str
    location_code: str | None
    confidence: float
    active_since: object | None
    cleared_at: object | None
    judgment_fingerprint: str
    evidence_report_ids: tuple[str, ...]
    excluded_report_ids: tuple[str, ...]
    content_hash: str
    prev_snapshot_id: str | None
    chain_hash: str


class SnapshotLedger:
    def __init__(self) -> None:
        self._snapshots: list[Snapshot] = []
        self._latest_fingerprint: dict[str, str] = {}

    def add(
        self,
        judgment: EventJudgment,
        *,
        published_at,
        journal_seq_max: int,
        sealed: bool,
        force: bool = False,
    ) -> Snapshot | None:
        """追加快照；同一片段内容指纹未变则不重复发布。

        force 用于封存等运营动作：即使结论指纹未变，也要留下一条独立、
        带封存标记的新快照，而不是回改既有快照。
        """
        if not force and self._latest_fingerprint.get(judgment.episode_id) == judgment.fingerprint:
            return None

        sequence = len(self._snapshots) + 1
        prev = self._snapshots[-1] if self._snapshots else None
        body = {
            "event_key": judgment.event_key,
            "episode_id": judgment.episode_id,
            "status": judgment.status.value,
            "location_code": judgment.location_code,
            "confidence": judgment.confidence,
            "active_since": _iso(judgment.active_since),
            "cleared_at": _iso(judgment.cleared_at),
            "fingerprint": judgment.fingerprint,
            "rules_version": judgment.rules_version,
            "as_of": judgment.computed_as_of.isoformat(),
            "journal_seq_max": journal_seq_max,
            "sealed": sealed,
            "evidence": [
                {
                    "report_id": item.report_id,
                    "source_type": item.source_type,
                    "source_id": item.source_id,
                    "kind": item.kind,
                    "observed_at": item.observed_at.isoformat(),
                    "intrinsic_weight": item.intrinsic_weight,
                    "effective_weight": item.effective_weight,
                    "role": item.role,
                    "stale": item.stale,
                }
                for item in judgment.evidence
            ],
            "excluded": [
                {"report_id": item.report_id, "reason": item.reason, "detail": item.detail}
                for item in judgment.excluded
            ],
            "lane_conflicts": judgment.lane_conflicts,
            "occurrence_spread_s": judgment.occurrence_spread_s,
        }
        content_hash = hashlib.sha256(
            json.dumps(body, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        prev_chain = prev.chain_hash if prev else ""
        chain_hash = hashlib.sha256(f"{prev_chain}:{content_hash}".encode("utf-8")).hexdigest()

        snapshot = Snapshot(
            snapshot_id=f"snap-{sequence:06d}",
            sequence=sequence,
            published_at=published_at,
            sealed=sealed,
            rules_version=judgment.rules_version,
            as_of=judgment.computed_as_of,
            journal_seq_max=journal_seq_max,
            event_key=judgment.event_key,
            episode_id=judgment.episode_id,
            status=judgment.status.value,
            location_code=judgment.location_code,
            confidence=judgment.confidence,
            active_since=judgment.active_since,
            cleared_at=judgment.cleared_at,
            judgment_fingerprint=judgment.fingerprint,
            evidence_report_ids=tuple(item.report_id for item in judgment.evidence),
            excluded_report_ids=tuple(item.report_id for item in judgment.excluded),
            content_hash=content_hash,
            prev_snapshot_id=prev.snapshot_id if prev else None,
            chain_hash=chain_hash,
        )
        self._snapshots.append(snapshot)
        self._latest_fingerprint[judgment.episode_id] = judgment.fingerprint
        return snapshot

    def all(self) -> list[Snapshot]:
        return list(self._snapshots)

    def get(self, snapshot_id: str) -> Snapshot | None:
        return next((s for s in self._snapshots if s.snapshot_id == snapshot_id), None)

    def for_event(self, event_key: str) -> list[Snapshot]:
        return [s for s in self._snapshots if s.event_key == event_key]

    def verify_chain(self) -> bool:
        """逐条重算哈希链，供运维巡检发现账本是否被篡改。"""
        prev_chain = ""
        prev_id: str | None = None
        for snapshot in self._snapshots:
            if snapshot.prev_snapshot_id != prev_id:
                return False
            expect = hashlib.sha256(
                f"{prev_chain}:{snapshot.content_hash}".encode("utf-8")
            ).hexdigest()
            if expect != snapshot.chain_hash:
                return False
            prev_chain = snapshot.chain_hash
            prev_id = snapshot.snapshot_id
        return True


def _iso(value) -> str | None:
    return value.isoformat() if value is not None else None
