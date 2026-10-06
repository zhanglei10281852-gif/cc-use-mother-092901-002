"""车路云事件可信合成服务。

编排只增日志、合成引擎与快照账本，对外提供：

* ingest：原始报文摄入（重复 report_id 幂等，不重复触发发布）；
* publish：按判定时刻合成并发布新快照，判断无变化则不发；
* seal：封存某事件当前片段，迟到报文此后不得改写该片段判断；
* replay：按指定时点重演合成，并与当前判断、事后新增证据对比，
  重演只读取账本，绝不写入、不改写历史发布记录。
"""

from dataclasses import dataclass

from .contracts import SourceReport
from .engine import Engine, EventJudgment, SynthesisResult
from .journal import Journal, ReceiptResult
from .rules import RuleSet, get_rules, latest_rules
from .snapshots import Snapshot, SnapshotLedger


@dataclass(frozen=True)
class IngestOutcome:
    receipt: ReceiptResult
    published: tuple[Snapshot, ...]


@dataclass(frozen=True)
class SealRecord:
    event_key: str
    episode_id: str
    sealed_at: object
    journal_seq: int
    snapshot_id: str


@dataclass(frozen=True)
class ReplayComparison:
    as_of: object
    rules_version: str
    historical: dict[str, EventJudgment]
    current: dict[str, EventJudgment]
    new_reports: tuple[dict, ...]
    changes: dict[str, dict]
    published_snapshots_untouched: bool


class FusionService:
    def __init__(self, rules: RuleSet | None = None, *, now=None) -> None:
        self._rules = rules or latest_rules()
        self._journal = Journal()
        self._engine = Engine(self._rules)
        self._ledger = SnapshotLedger()
        # 事件 -> {片段编号: 封存记录}，复发新片段的封存不覆盖旧片段。
        self._seals: dict[str, dict[str, SealRecord]] = {}
        self._now = now

    @property
    def rules(self) -> RuleSet:
        return self._rules

    def rules_version(self) -> str:
        return self._rules.version

    # ---- 摄入与发布 -------------------------------------------------------

    def ingest(self, report: SourceReport, *, auto_publish: bool = True) -> IngestOutcome:
        receipt = self._journal.append(report)
        published: tuple[Snapshot, ...] = ()
        if not receipt.duplicated and auto_publish:
            published = self.publish(report.received_at, event_keys=self._event_keys_of(report))
        return IngestOutcome(receipt=receipt, published=published)

    def publish(self, as_of, *, event_keys: set[str] | None = None) -> tuple[Snapshot, ...]:
        """合成 received_at 不晚于 as_of 的全部报文并发布变化快照。

        摄入路径只重算被新报文触及的事件，避免无关事件因判定时刻推移
        被重复发布；手工发布不传 event_keys 时重算全部事件。
        """
        visible = [e for e in self._journal.entries if e.report.received_at <= as_of]
        result = self._engine.synthesize(visible, as_of)
        emitted: list[Snapshot] = []
        for key, judgment in result.judgments.items():
            if event_keys is not None and key not in event_keys:
                continue
            seal = self._seals.get(key, {}).get(judgment.episode_id)
            if seal is not None:
                sealed_snapshot = self._ledger.get(seal.snapshot_id)
                # 封存冻结当时的结论：迟到证据不得改写车道、置信等内容；
                # 但片段生命周期的合法推进（如 active→cleared）允许发布新快照，
                # 封存快照本身保持不可变。
                if sealed_snapshot is None or judgment.status.value == sealed_snapshot.status:
                    continue
            snapshot = self._ledger.add(
                judgment,
                published_at=as_of,
                journal_seq_max=result.seq_max,
                sealed=seal is not None,
            )
            if snapshot is not None:
                emitted.append(snapshot)
        return tuple(emitted)

    def _event_keys_of(self, report: SourceReport) -> set[str]:
        if report.observation is not None:
            return {report.observation.event_key}
        target = self._journal.report_by_id(report.target_report_id or "")
        if target is not None and target.observation is not None:
            return {target.observation.event_key}
        return set()

    def seal_event(self, event_key: str, sealed_at) -> Snapshot:
        """封存事件当前片段并发布封存快照；封存点之后不可被迟到证据推翻。

        同一事件解除后复发形成的新片段可再次封存；已封存的当前片段不可重复封存。
        """
        visible = [e for e in self._journal.entries if e.report.received_at <= sealed_at]
        result = self._engine.synthesize(visible, sealed_at)
        if event_key not in result.judgments:
            raise ValueError(f"该时点没有可封存的事件判断：{event_key}")
        judgment = result.judgments[event_key]
        episode_seals = self._seals.setdefault(event_key, {})
        if judgment.episode_id in episode_seals:
            raise ValueError(f"事件片段已封存：{judgment.episode_id}")
        snapshot = self._ledger.add(
            judgment,
            published_at=sealed_at,
            journal_seq_max=result.seq_max,
            sealed=True,
            force=True,
        )
        episode_seals[judgment.episode_id] = SealRecord(
            event_key=event_key,
            episode_id=judgment.episode_id,
            sealed_at=sealed_at,
            journal_seq=result.seq_max,
            snapshot_id=snapshot.snapshot_id,
        )
        return snapshot

    # ---- 查询 -------------------------------------------------------------

    @property
    def journal(self) -> Journal:
        return self._journal

    @property
    def ledger(self) -> SnapshotLedger:
        return self._ledger

    def seals(self) -> dict[str, dict[str, SealRecord]]:
        return {key: dict(by_episode) for key, by_episode in self._seals.items()}

    def current_judgment(self, as_of) -> SynthesisResult:
        visible = [e for e in self._journal.entries if e.report.received_at <= as_of]
        return self._engine.synthesize(visible, as_of)

    # ---- 时点重演 ----------------------------------------------------------

    def replay(self, as_of, *, rules_version: str | None = None) -> ReplayComparison:
        """按 as_of 重演当时合成，再与截至最新报文的当前判断逐项对比。

        重演使用独立引擎实例与当时可见报文，不触碰快照账本，
        因此历史发布记录不会被改写。
        """
        historical_entries = [
            e for e in self._journal.entries if e.report.received_at <= as_of
        ]

        replay_rules = self._rules if rules_version is None else get_rules(rules_version)
        historical = Engine(replay_rules).synthesize(historical_entries, as_of)

        # 当前判断使用最后一份报文的接收时刻，使新鲜度衰减反映事后视角，
        # 而 as_of 重演保持当时视角。
        latest_rx = max((e.report.received_at for e in self._journal.entries), default=as_of)
        current = self._engine.synthesize(self._journal.entries, latest_rx)

        # 事后新增证据按“接收时刻晚于重演时点”认定，序号在乱序送达时不可靠。
        new_reports = tuple(
            {
                "seq": e.seq,
                "report_id": e.report.report_id,
                "action": e.report.action.value,
                "event_key": (
                    e.report.observation.event_key if e.report.observation is not None else None
                ),
                "received_at": e.report.received_at.isoformat(),
                "duplicate_of": e.duplicate_of,
            }
            for e in self._journal.entries
            if e.report.received_at > as_of
        )

        changes: dict[str, dict] = {}
        keys = set(historical.judgments) | set(current.judgments)
        for key in sorted(keys):
            old = historical.judgments.get(key)
            new = current.judgments.get(key)
            if old is None or new is None or _judgment_signature(old) != _judgment_signature(new):
                changes[key] = {
                    "then": _compact(old),
                    "now": _compact(new),
                }

        return ReplayComparison(
            as_of=as_of,
            rules_version=replay_rules.version,
            historical=historical.judgments,
            current=current.judgments,
            new_reports=new_reports,
            changes=changes,
            published_snapshots_untouched=True,
        )


def _judgment_signature(judgment: EventJudgment) -> tuple:
    return (
        judgment.episode_id,
        judgment.status.value,
        judgment.location_code,
        judgment.confidence,
        judgment.fingerprint,
    )


def _compact(judgment: EventJudgment | None) -> dict | None:
    if judgment is None:
        return None
    return {
        "episode_id": judgment.episode_id,
        "status": judgment.status.value,
        "location_code": judgment.location_code,
        "confidence": judgment.confidence,
        "evidence": [item.report_id for item in judgment.evidence],
        "fingerprint": judgment.fingerprint,
        "rules_version": judgment.rules_version,
    }
