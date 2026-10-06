"""事件合成引擎。

输入只增日志截至某时点的报文，输出每个事件的当前判断。设计要点：

* 修订链：amend 使目标报文失效并以新报文接续；withdraw 使目标报文失效；
  只允许来源修订或撤回自己的报文。乱序到达采用两趟解析闭合链路。
* 立场模型：同一来源对同一事件只保留观测时刻最新的立场，来源翻证
  （先报占道、后报解除）自动对冲自己早先的观测，累计权重不会随报数虚高。
* 加权：来源能力 × 上报置信 × 新鲜度衰减；独立来源数量带来交叉印证增益，
  单源结论受单源限速约束。
* 片段(episode)切分：按观测时刻扫描，反侧来源的加权立场净胜正侧达解除
  裕度即关闭片段；关闭后再次出现的正侧观测，按观测时刻与解除时刻的间隔
  区分复发（新片段）与旧消息重放（留在已关闭片段，不参与判断）。
* 迟到判定：只有 received_at 不晚于判定时点的报文才可见。
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .contracts import EventStatus, ObservationKind, ReportAction, SourceReport
from .journal import JournalEntry
from .rules import LaneConflictPolicy, RuleSet

_POSITIVE = frozenset({ObservationKind.LANE_CLOSED, ObservationKind.OBSTACLE})
_NEGATIVE = frozenset({ObservationKind.LANE_OPEN, ObservationKind.CLEARED})


@dataclass(frozen=True)
class EvidenceItem:
    report_id: str
    source_type: str
    source_id: str
    kind: str
    observed_at: datetime
    intrinsic_weight: float
    effective_weight: float
    role: str  # positive / negative
    stale: bool = False


@dataclass(frozen=True)
class ExcludedItem:
    report_id: str
    reason: str
    # duplicate / superseded / withdrawn / replay / link_violation /
    # stance_updated / no_active_episode
    detail: str = ""
    event_key: str | None = None


@dataclass(frozen=True)
class EventJudgment:
    event_key: str
    episode_id: str
    status: EventStatus
    location_code: str | None
    confidence: float
    active_since: datetime | None
    cleared_at: datetime | None
    evidence: tuple[EvidenceItem, ...]
    excluded: tuple[ExcludedItem, ...]
    lane_conflicts: dict[str, float]
    occurrence_spread_s: float | None
    rules_version: str
    computed_as_of: datetime
    fingerprint: str


@dataclass(frozen=True)
class SynthesisResult:
    as_of: datetime
    rules_version: str
    seq_max: int
    judgments: dict[str, EventJudgment]


@dataclass
class _Node:
    report: SourceReport
    inactive: bool = False
    inactive_reason: str | None = None


@dataclass
class _Stance:
    report: SourceReport
    role: str
    weight: float


@dataclass
class _Episode:
    ordinal: int
    opened_at: datetime
    # 来源 -> 当前立场；随观测时刻更新，翻证即对冲旧立场。
    stances: dict[tuple[str, str], _Stance] = field(default_factory=dict)
    replay: list[SourceReport] = field(default_factory=list)
    # 仅属于本片段的证据排除记录（同源立场更新、旧消息重放）。
    excluded: list[ExcludedItem] = field(default_factory=list)
    cleared_at: datetime | None = None


def _intrinsic_weight(report: SourceReport, rules: RuleSet) -> float:
    obs = report.observation
    return rules.capability(report.source.source_type) * obs.confidence


def _effective_weight(report: SourceReport, as_of: datetime, rules: RuleSet) -> tuple[float, bool]:
    obs = report.observation
    age = max(0.0, (as_of - obs.source.observed_at).total_seconds())
    fresh = rules.freshness(age)
    return (
        rules.capability(report.source.source_type) * obs.confidence * fresh,
        fresh <= 0.0,
    )


def _role(kind: ObservationKind) -> str:
    return "positive" if kind in _POSITIVE else "negative"


class Engine:
    def __init__(self, rules: RuleSet) -> None:
        self._rules = rules

    @property
    def rules(self) -> RuleSet:
        return self._rules

    def synthesize(self, entries: list[JournalEntry], as_of: datetime) -> SynthesisResult:
        rules = self._rules
        nodes, excluded = self._resolve_chains(entries)

        by_event: dict[str, list[_Node]] = {}
        active_by_event: dict[str, list[_Node]] = {}
        for node in nodes.values():
            obs = node.report.observation
            if obs.source.observed_at > as_of:
                # 自称来自未来的观测在该时点尚不可用。
                continue
            by_event.setdefault(obs.event_key, []).append(node)
            if not node.inactive:
                active_by_event.setdefault(obs.event_key, []).append(node)

        # 即使事件的全部证据都被撤回/修订，也保留一个 unknown 判断，
        # 使下游能够收到“撤销下发”的快照，而不是让事件无声消失。
        judgments = {
            key: self._judge_event(
                key, active_by_event.get(key, []), members, excluded, as_of
            )
            for key, members in by_event.items()
        }
        seq_max = max((e.seq for e in entries), default=0)
        return SynthesisResult(as_of=as_of, rules_version=rules.version, seq_max=seq_max, judgments=judgments)

    # ---- 修订链 ----------------------------------------------------------

    def _resolve_chains(self, entries: list[JournalEntry]) -> tuple[dict[str, _Node], list[ExcludedItem]]:
        nodes: dict[str, _Node] = {}
        excluded: list[ExcludedItem] = []
        live_entries = [e for e in entries if e.duplicate_of is None]

        # 第一趟：登记全部首发观测节点。
        for entry in live_entries:
            report = entry.report
            if report.action is ReportAction.OBSERVE:
                if report.report_id in nodes:
                    excluded.append(
                        ExcludedItem(report.report_id, "duplicate", "同编号报文再次出现",
                                     report.observation.event_key if report.observation else None)
                    )
                    continue
                nodes[report.report_id] = _Node(report=report)

        # 第二趟：按接收顺序应用修订与撤回（目标可能晚于操作到达）。
        for entry in live_entries:
            report = entry.report
            if report.action is ReportAction.OBSERVE:
                continue
            target = nodes.get(report.target_report_id)
            target_event = (
                target.report.observation.event_key
                if target is not None and target.report.observation is not None
                else (report.observation.event_key if report.observation is not None else None)
            )
            same_source = (
                target is not None
                and target.report.source.source_type == report.source.source_type
                and target.report.source.external_id == report.source.external_id
            )
            if target is None:
                excluded.append(
                    ExcludedItem(report.report_id, "link_violation",
                                 "目标报文在该时点不可见，暂不生效", target_event)
                )
                continue
            if not same_source:
                excluded.append(
                    ExcludedItem(report.report_id, "link_violation",
                                 "只允许修订或撤回同一来源自己的报文", target_event)
                )
                continue
            if target.inactive:
                excluded.append(
                    ExcludedItem(report.report_id, "link_violation",
                                 "目标报文已失效，重复操作不再生效", target_event)
                )
                continue

            if report.action is ReportAction.AMEND:
                if report.report_id in nodes:
                    excluded.append(
                        ExcludedItem(report.report_id, "duplicate", "修订报文编号已存在", target_event)
                    )
                    continue
                target.inactive = True
                target.inactive_reason = "superseded"
                nodes[report.report_id] = _Node(report=report)
                excluded.append(
                    ExcludedItem(report.target_report_id, "superseded",
                                 f"由 {report.report_id} 修订", target_event)
                )
            else:  # WITHDRAW
                target.inactive = True
                target.inactive_reason = "withdrawn"
                excluded.append(
                    ExcludedItem(report.target_report_id, "withdrawn",
                                 f"由 {report.report_id} 撤回", target_event)
                )

        for entry in entries:
            if entry.duplicate_of is not None:
                event_key = (
                    entry.report.observation.event_key
                    if entry.report.observation is not None
                    else None
                )
                excluded.append(
                    ExcludedItem(entry.report.report_id, "duplicate",
                                 f"首次接收序号 #{entry.duplicate_of}", event_key)
                )
        return nodes, excluded

    # ---- 单事件判断 -------------------------------------------------------

    def _judge_event(
        self,
        event_key: str,
        active_nodes: list[_Node],
        member_nodes: list[_Node],
        excluded: list[ExcludedItem],
        as_of: datetime,
    ) -> EventJudgment:
        rules = self._rules
        reports = sorted(
            (n.report for n in active_nodes),
            key=lambda r: (r.observation.source.observed_at, r.received_at),
        )
        if reports:
            episodes = self._segment(reports)
        else:
            # 全部证据失效：用事件最早的观测时刻占位，输出 unknown。
            earliest = min(
                n.report.observation.source.observed_at for n in member_nodes
            )
            episodes = [_Episode(ordinal=1, opened_at=earliest)]
        latest = episodes[-1]
        local_excluded = [
            item for item in excluded
            if item.event_key == event_key
        ] + list(latest.excluded)

        evidence: list[EvidenceItem] = []
        pos_sources: dict[tuple[str, str], float] = {}
        neg_sources: dict[tuple[str, str], float] = {}
        lane_votes: dict[str, list[SourceReport]] = {}
        pos_times: list[datetime] = []

        for stance in latest.stances.values():
            report = stance.report
            obs = report.observation
            weight, stale = _effective_weight(report, as_of, rules)
            evidence.append(
                EvidenceItem(
                    report_id=report.report_id,
                    source_type=report.source.source_type,
                    source_id=report.source.external_id,
                    kind=obs.kind.value,
                    observed_at=obs.source.observed_at,
                    intrinsic_weight=round(stance.weight, 6),
                    effective_weight=round(weight, 6),
                    role=stance.role,
                    stale=stale,
                )
            )
            key = (report.source.source_type, report.source.external_id)
            if stance.role == "positive":
                pos_sources[key] = max(pos_sources.get(key, 0.0), weight)
                lane_votes.setdefault(obs.location_code, []).append(report)
                pos_times.append(obs.source.observed_at)
            else:
                neg_sources[key] = max(neg_sources.get(key, 0.0), weight)

        pos_score = self._side_score(pos_sources)
        neg_score = self._side_score(neg_sources)
        location, lane_conflicts = self._lane_verdict(lane_votes, as_of)
        occurrence_spread = (
            (max(pos_times) - min(pos_times)).total_seconds() if len(pos_times) >= 2 else None
        )

        if latest.cleared_at is not None:
            status = EventStatus.CLEARED
            confidence = neg_score
        elif pos_score >= rules.active_threshold and pos_score - neg_score > -rules.clear_margin:
            status = EventStatus.ACTIVE
            confidence = pos_score
        else:
            status = EventStatus.UNKNOWN
            confidence = max(pos_score, neg_score)

        judgment = EventJudgment(
            event_key=event_key,
            episode_id=f"{event_key}#E{latest.ordinal}",
            status=status,
            location_code=location,
            confidence=round(min(1.0, confidence), 6),
            active_since=latest.opened_at if status is EventStatus.ACTIVE else None,
            cleared_at=latest.cleared_at,
            evidence=tuple(evidence),
            excluded=tuple(local_excluded),
            lane_conflicts=lane_conflicts,
            occurrence_spread_s=occurrence_spread,
            rules_version=rules.version,
            computed_as_of=as_of,
            fingerprint="",
        )
        return self._with_fingerprint(judgment)

    def _side_score(self, per_source: dict[tuple[str, str], float]) -> float:
        alive = {k: v for k, v in per_source.items() if v > 0}
        if not alive:
            return 0.0
        score = sum(alive.values())
        if len(alive) > 1:
            score += self._rules.corroboration_bonus * (len(alive) - 1)
        else:
            score = min(score, self._rules.single_source_cap)
        return round(min(1.0, score), 6)

    def _segment(self, reports: list[SourceReport]) -> list[_Episode]:
        """按观测时刻推进立场状态机，返回带本片段排除记录的片段列表。"""
        rules = self._rules
        episodes: list[_Episode] = []
        loose: list[ExcludedItem] = []
        current: _Episode | None = None
        gap = timedelta(seconds=rules.recurrence_gap_s)

        def totals(episode: _Episode) -> tuple[float, float]:
            pos = sum(s.weight for s in episode.stances.values() if s.role == "positive")
            neg = sum(s.weight for s in episode.stances.values() if s.role == "negative")
            return pos, neg

        for report in reports:
            obs = report.observation
            key = (report.source.source_type, report.source.external_id)
            observed_at = obs.source.observed_at
            role = _role(obs.kind)
            weight = _intrinsic_weight(report, rules)

            if role == "positive":
                if current is None:
                    current = _Episode(ordinal=1, opened_at=observed_at)
                    episodes.append(current)
                elif current.cleared_at is not None:
                    if observed_at >= current.cleared_at + gap:
                        current = _Episode(ordinal=current.ordinal + 1, opened_at=observed_at)
                        episodes.append(current)
                    else:
                        current.replay.append(report)
                        current.excluded.append(
                            ExcludedItem(
                                report.report_id,
                                "replay",
                                f"观测时刻 {observed_at.isoformat()} 落在解除时刻后的"
                                f" {rules.recurrence_gap_s:.0f} 秒重放窗口内，按旧消息重放处理",
                                obs.event_key,
                            )
                        )
                        continue

                previous = current.stances.get(key)
                if previous is not None:
                    current.excluded.append(
                        ExcludedItem(
                            previous.report.report_id,
                            "stance_updated",
                            f"被同源更新观测 {report.report_id} 的立场替代",
                            obs.event_key,
                        )
                    )
                current.stances[key] = _Stance(report, "positive", weight)

            else:  # negative
                if current is None:
                    loose.append(
                        ExcludedItem(report.report_id, "no_active_episode",
                                     "无活跃片段可解除", obs.event_key)
                    )
                    continue
                if current.cleared_at is not None and observed_at >= current.cleared_at + gap:
                    current.excluded.append(
                        ExcludedItem(report.report_id, "no_active_episode",
                                     "片段已解除且超出重放窗口，无活跃片段可解除",
                                     obs.event_key)
                    )
                    continue

                previous = current.stances.get(key)
                if previous is not None:
                    current.excluded.append(
                        ExcludedItem(
                            previous.report.report_id,
                            "stance_updated",
                            f"被同源更新观测 {report.report_id} 的立场替代（来源翻证）",
                            obs.event_key,
                        )
                    )
                current.stances[key] = _Stance(report, "negative", weight)

                if current.cleared_at is None:
                    pos_total, neg_total = totals(current)
                    if neg_total - pos_total >= rules.clear_margin:
                        current.cleared_at = observed_at

        if not episodes:
            # 只有解除类观测、从未出现活跃片段：给出证据不足的占位判断，
            # 使这些解除观测及其排除原因仍可在快照中解释。
            placeholder = _Episode(ordinal=1, opened_at=reports[0].observation.source.observed_at)
            placeholder.excluded.extend(loose)
            episodes.append(placeholder)
        return episodes

    def _lane_verdict(
        self, votes: dict[str, list[SourceReport]], as_of: datetime
    ) -> tuple[str | None, dict[str, float]]:
        rules = self._rules
        per_lane: dict[str, tuple[float, datetime | None]] = {}
        for lane, lane_reports in votes.items():
            per_source: dict[tuple[str, str], float] = {}
            newest: datetime | None = None
            for report in lane_reports:
                key = (report.source.source_type, report.source.external_id)
                if rules.lane_conflict_policy is LaneConflictPolicy.CAPABILITY_FIRST:
                    weight = _intrinsic_weight(report, rules)
                else:
                    weight, _ = _effective_weight(report, as_of, rules)
                per_source[key] = max(per_source.get(key, 0.0), weight)
                observed_at = report.observation.source.observed_at
                if newest is None or observed_at > newest:
                    newest = observed_at
            per_lane[lane] = (round(sum(per_source.values()), 6), newest)
        if not per_lane:
            return None, {}
        winner = max(per_lane, key=lambda lane: (per_lane[lane][0], per_lane[lane][1]))
        conflicts = {lane: weight for lane, (weight, _) in per_lane.items() if lane != winner}
        return winner, conflicts

    @staticmethod
    def _with_fingerprint(judgment: EventJudgment) -> EventJudgment:
        import hashlib
        import json

        payload = {
            "event_key": judgment.event_key,
            "episode_id": judgment.episode_id,
            "status": judgment.status.value,
            "location_code": judgment.location_code,
            "confidence": judgment.confidence,
            "active_since": judgment.active_since.isoformat() if judgment.active_since else None,
            "cleared_at": judgment.cleared_at.isoformat() if judgment.cleared_at else None,
            "evidence_reports": [
                (item.report_id, item.role, round(item.effective_weight, 4))
                for item in judgment.evidence
            ],
            "excluded_reports": [(item.report_id, item.reason) for item in judgment.excluded],
            "rules_version": judgment.rules_version,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()[:16]
        object.__setattr__(judgment, "fingerprint", digest)
        return judgment
