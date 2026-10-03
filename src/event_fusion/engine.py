"""车路云事件可信合成引擎。

职责：
* 完整保留每个来源的观测、修订与撤回；
* 依据来源能力、时间新鲜度、跨来源交叉印证形成当前判断；
* 解除结论封存发生片段（episode），迟到消息只能进入未封存片段；
* 区分解除后的复发与旧消息重放；
* 不直接发布，只给出判断与证据明细；发布策略见 service.py。
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .contracts import EventObservation
from .rules import RuleRegistry
from .sources import SourceRegistry
from .models import (
    ASSERTIVE_KINDS,
    CLEAR_KINDS,
    EvidenceItem,
    Judgment,
    JudgmentStatus,
)

RECURRENCE = "recurrence"
DUPLICATE = "duplicate"
ACCEPTED = "accepted"
EXCLUDED = "excluded"
WITHDRAWN = "withdrawn"


@dataclass
class IngestResult:
    observation_id: str
    classification: str
    reason: str
    judgment: Judgment | None
    sealed: bool = False


@dataclass
class _Envelope:
    obs: EventObservation
    observation_id: str
    received_at: datetime
    episode: int | None = None
    excluded_reason: str | None = None
    withdrawn: bool = False


@dataclass
class _Episode:
    index: int
    started_at: datetime
    anchor_observed_at: datetime
    sealed: bool = False
    sealed_at: datetime | None = None
    clear_observed_at: datetime | None = None
    members: list[str] = field(default_factory=list)


def observation_id(obs: EventObservation) -> str:
    return f"{obs.event_key}|{obs.source.source_type}|{obs.source.external_id}"


class FusionEngine:
    def __init__(
        self,
        sources: SourceRegistry | None = None,
        rules: RuleRegistry | None = None,
    ) -> None:
        self.sources = sources or SourceRegistry.default()
        self.rules = rules or RuleRegistry.default()
        self._store: dict[str, _Envelope] = {}
        self._episodes: dict[str, list[_Episode]] = {}
        self._judgments: dict[str, Judgment] = {}
        self._evidence: dict[str, tuple[EvidenceItem, ...]] = {}
        self._pending_withdrawn: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------ 接入

    def ingest(self, obs: EventObservation, received_at: datetime) -> IngestResult:
        oid = observation_id(obs)
        existing = self._store.get(oid)
        if existing is not None:
            return IngestResult(
                oid,
                DUPLICATE,
                "相同来源外部编号的报文已接收，重复报文不参与合成",
                self._judgments.get(obs.event_key),
            )

        envelope = _Envelope(obs, oid, received_at)
        pending_key = (obs.event_key, obs.source.source_type, obs.source.external_id)
        if pending_key in self._pending_withdrawn:
            self._pending_withdrawn.discard(pending_key)
            envelope.withdrawn = True
            envelope.excluded_reason = "该报文在到达前已被来源撤回"
        self._store[oid] = envelope

        kind = obs.kind.value
        opened_episode = False
        if envelope.excluded_reason is None:
            if kind in ASSERTIVE_KINDS:
                opened_episode = self._classify_assertive(envelope, received_at)
            elif kind in CLEAR_KINDS:
                self._classify_clear(envelope, received_at)
            else:  # pragma: no cover - 契约枚举已封闭
                envelope.excluded_reason = f"未知观测种类 {kind}"

        if envelope.excluded_reason is None and not envelope.withdrawn:
            sealed = self._reevaluate(obs.event_key, received_at)
            classification = RECURRENCE if opened_episode else ACCEPTED
            reason = (
                "解除后的复发：观测时刻与前次解除间隔超过阈值，开启新发生片段"
                if opened_episode
                else "证据已纳入未封存片段"
            )
            return IngestResult(
                oid,
                classification,
                reason,
                self._judgments.get(obs.event_key),
                sealed=sealed,
            )
        return IngestResult(
            oid,
            WITHDRAWN if envelope.withdrawn else EXCLUDED,
            envelope.excluded_reason or "该报文已被来源撤回",
            self._judgments.get(obs.event_key),
        )

    def withdraw(
        self, event_key: str, source_type: str, external_id: str, recorded_at: datetime
    ) -> IngestResult | None:
        oid = f"{event_key}|{source_type}|{external_id}"
        envelope = self._store.get(oid)
        if envelope is None:
            # 观测可能尚未到达，先登记撤回意图。
            self._pending_withdrawn.add((event_key, source_type, external_id))
            return None
        if envelope.withdrawn:
            return IngestResult(
                oid, DUPLICATE, "撤回指令重复", self._judgments.get(event_key)
            )
        envelope.withdrawn = True
        open_episode = self._open_episode(event_key)
        sealed = False
        if open_episode is not None and envelope.episode == open_episode.index:
            sealed = self._reevaluate(event_key, recorded_at)
        return IngestResult(
            oid,
            WITHDRAWN,
            "撤回仅影响未封存片段" if open_episode else "片段已封存，撤回留痕但不改判",
            self._judgments.get(event_key),
            sealed=sealed,
        )

    def _classify_assertive(self, envelope: _Envelope, now: datetime) -> bool:
        """返回该报文是否在解除之后开启了新的发生片段（即复发）。"""
        obs = envelope.obs
        episodes = self._episodes.setdefault(obs.event_key, [])
        open_episode = next((ep for ep in episodes if not ep.sealed), None)
        if open_episode is not None:
            stale = self._stale_to_sealed_predecessor(episodes, open_episode, obs.source.observed_at, now)
            if stale:
                envelope.excluded_reason = stale
                return False
            envelope.episode = open_episode.index
            open_episode.members.append(oid_from(envelope))
            return False
        last = episodes[-1] if episodes else None
        rules = self.rules.version_at(now)
        if last is not None:
            assert last.clear_observed_at is not None
            gap = (obs.source.observed_at - last.clear_observed_at).total_seconds()
            if gap < 0:
                envelope.excluded_reason = (
                    f"迟到消息：观测时刻早于片段{last.index}的解除时刻"
                    f"（{last.clear_observed_at.isoformat()}），所反映片段已封存，"
                    "不得影响当前判断"
                )
                return False
            if gap <= rules.recurrence_gap_seconds:
                envelope.excluded_reason = (
                    f"旧消息重放：观测时刻距片段{last.index}解除仅{gap:.0f}秒，"
                    f"未超过复发间隔{rules.recurrence_gap_seconds:.0f}秒，"
                    f"所反映片段{last.index}已封存，不得影响当前判断"
                )
                return False
        episode = _Episode(
            index=len(episodes) + 1,
            started_at=now,
            anchor_observed_at=obs.source.observed_at,
            members=[oid_from(envelope)],
        )
        envelope.episode = episode.index
        episodes.append(episode)
        return last is not None

    def _classify_clear(self, envelope: _Envelope, now: datetime) -> None:
        obs = envelope.obs
        episodes = self._episodes.setdefault(obs.event_key, [])
        open_episode = next((ep for ep in episodes if not ep.sealed), None)
        rules = self.rules.version_at(now)
        if open_episode is None:
            envelope.excluded_reason = "当前没有进行中的发生片段，解除报文按旧消息重放处理"
            return
        stale = self._stale_to_sealed_predecessor(
            episodes, open_episode, obs.source.observed_at, now
        )
        if stale:
            envelope.excluded_reason = stale
            return
        earliest = self._earliest_assertive_observed_at(obs.event_key, open_episode)
        if earliest is not None:
            lead = (earliest - obs.source.observed_at).total_seconds()
            if lead > rules.recurrence_gap_seconds:
                envelope.excluded_reason = (
                    f"解除观测早于片段{open_episode.index}的最早发生时刻{lead:.0f}秒，按过期报文排除"
                )
                return
        envelope.episode = open_episode.index
        open_episode.members.append(oid_from(envelope))

    # ------------------------------------------------------------------ 仲裁

    def _reevaluate(self, event_key: str, now: datetime) -> bool:
        """重算未封存片段的当前判断；返回本次是否触发了封存。"""
        episodes = self._episodes.get(event_key, [])
        open_episode = next((ep for ep in episodes if not ep.sealed), None)
        if open_episode is None:
            return False
        rules = self.rules.version_at(now)

        all_envelopes = [self._store[m] for m in open_episode.members]
        valid_envelopes = [
            e for e in all_envelopes if not e.withdrawn and e.excluded_reason is None
        ]
        # 权威来源（能力达到解除权威门槛）的解除结论，取代严格更早的占道观测：
        # 路侧/人工到场确认已恢复通行，旧占道报文即使多源也不能压过解除。
        authoritative_clear_at: datetime | None = None
        for envelope in valid_envelopes:
            obs = envelope.obs
            if obs.kind.value not in CLEAR_KINDS:
                continue
            if self.sources.capability(obs.source.source_type) < rules.clear_authority_capability:
                continue
            if (
                authoritative_clear_at is None
                or obs.source.observed_at > authoritative_clear_at
            ):
                authoritative_clear_at = obs.source.observed_at

        usable = []
        superseded: set[str] = set()
        for envelope in valid_envelopes:
            obs = envelope.obs
            if (
                authoritative_clear_at is not None
                and obs.kind.value in ASSERTIVE_KINDS
                and obs.source.observed_at < authoritative_clear_at
            ):
                superseded.add(envelope.observation_id)
            else:
                usable.append(envelope)
        usable_ids = {e.observation_id for e in usable}
        items: list[EvidenceItem] = []
        assert_weight: dict[str, float] = {}
        clear_weight: dict[str, float] = {}
        assert_sources: set[str] = set()
        clear_sources: set[str] = set()
        kind_weight: dict[str, float] = {}
        loc_weight: dict[str, float] = {}
        latest_assert: datetime | None = None
        clear_best: tuple[float, _Envelope] | None = None

        for envelope in usable:
            obs = envelope.obs
            capability = self.sources.capability(obs.source.source_type)
            age = max(0.0, (now - obs.source.observed_at).total_seconds())
            freshness = 0.5 ** (age / rules.freshness_half_life_seconds)
            weight = capability * obs.confidence * freshness
            kind_value = obs.kind.value
            in_assert = kind_value in ASSERTIVE_KINDS
            bucket = assert_weight if in_assert else clear_weight
            sources = assert_sources if in_assert else clear_sources
            bucket[envelope.observation_id] = weight
            sources.add(obs.source.source_type)
            if in_assert:
                kind_weight[kind_value] = kind_weight.get(kind_value, 0.0) + weight
                loc_weight[obs.location_code] = (
                    loc_weight.get(obs.location_code, 0.0) + weight
                )
                if latest_assert is None or obs.source.observed_at > latest_assert:
                    latest_assert = obs.source.observed_at
            elif clear_best is None or weight > clear_best[0]:
                clear_best = (weight, envelope)
            items.append(
                EvidenceItem(
                    observation_id=envelope.observation_id,
                    kind=kind_value,
                    location_code=obs.location_code,
                    source_type=obs.source.source_type,
                    external_id=obs.source.external_id,
                    observed_at=obs.source.observed_at,
                    received_at=envelope.received_at,
                    confidence=obs.confidence,
                    capability=capability,
                    age_seconds=age,
                    weight=weight,
                    supports_winner=False,  # 胜者确定后回填
                )
            )

        # 被排除、撤回或被权威解除取代的证据同样随快照留痕，只是不参与计分。
        for member_id in open_episode.members:
            envelope = self._store[member_id]
            if envelope.observation_id in usable_ids:
                continue
            obs = envelope.obs
            if envelope.excluded_reason:
                detail_reason = envelope.excluded_reason
            elif envelope.observation_id in superseded:
                detail_reason = (
                    f"已被{authoritative_clear_at.isoformat()}的权威来源解除结论取代，"
                    "不参与解除后计分"
                )
            else:
                detail_reason = "该报文已被来源撤回"
            items.append(
                EvidenceItem(
                    observation_id=envelope.observation_id,
                    kind=obs.kind.value,
                    location_code=obs.location_code,
                    source_type=obs.source.source_type,
                    external_id=obs.source.external_id,
                    observed_at=obs.source.observed_at,
                    received_at=envelope.received_at,
                    confidence=obs.confidence,
                    capability=self.sources.capability(obs.source.source_type),
                    age_seconds=max(0.0, (now - obs.source.observed_at).total_seconds()),
                    weight=0.0,
                    supports_winner=False,
                    withdrawn=envelope.withdrawn,
                    excluded_reason=detail_reason,
                )
            )

        w_assert = sum(assert_weight.values())
        w_clear = sum(clear_weight.values())
        score_assert = w_assert * (
            1
            + rules.cross_confirm_bonus
            * min(max(0, len(assert_sources) - 1), rules.cross_confirm_max_extra_sources)
        )
        score_clear = w_clear * (
            1
            + rules.cross_confirm_bonus
            * min(max(0, len(clear_sources) - 1), rules.cross_confirm_max_extra_sources)
        )

        rationale: list[str] = []
        if not usable:
            judgment = Judgment(
                event_key=event_key,
                status=JudgmentStatus.UNCERTAIN,
                episode=open_episode.index,
                kind=None,
                location_code=None,
                score=0.0,
                confidence=0.0,
                opposition_score=0.0,
                active_since=open_episode.anchor_observed_at,
                last_observed_at=None,
                cleared_at=None,
                rationale=("片段内有效证据均已撤回或排除，判断降级为不确定",),
                evidence_ids=tuple(),
            )
            self._judgments[event_key] = judgment
            self._evidence[event_key] = tuple(items)
            return False

        threshold = rules.publish_score_threshold
        margin_ok = (
            max(score_assert, score_clear) == 0.0
            or abs(score_assert - score_clear) / max(score_assert, score_clear)
            >= rules.winner_margin_ratio
        )
        over_threshold = max(score_assert, score_clear) >= threshold

        winner: str | None = None
        if over_threshold and margin_ok:
            winner = "assert" if score_assert >= score_clear else "clear"

        if winner is None:
            status = JudgmentStatus.UNCERTAIN
            rationale.append(
                f"占道证据组合得分{score_assert:.3f}、解除证据{score_clear:.3f}，"
                f"门槛{threshold:.2f}、最小领先比例{rules.winner_margin_ratio:.2f}，"
                "证据不足或相互冲突，保持不确定"
            )
            score = max(score_assert, score_clear)
            kind = None
            location = None
            cleared_at = None
        elif winner == "clear":
            status = JudgmentStatus.CLEARED
            kind = None
            location = None
            best_clear_env = clear_best[1] if clear_best else None
            cleared_at = (
                best_clear_env.obs.source.observed_at if best_clear_env else now
            )
            score = score_clear
            rationale.append(
                f"解除证据组合得分{score_clear:.3f}高于占道得分{score_assert:.3f}，"
                f"由{len(clear_sources)}类来源印证，封存片段{open_episode.index}"
            )
        else:
            status = JudgmentStatus.ACTIVE
            kind = max(kind_weight, key=lambda k: kind_weight[k])
            location = max(loc_weight, key=lambda k: loc_weight[k])
            cleared_at = None
            score = score_assert
            rationale.append(
                f"占道证据组合得分{score_assert:.3f}高于解除得分{score_clear:.3f}，"
                f"由{len(assert_sources)}类来源（{','.join(sorted(assert_sources))}）交叉印证"
            )

        for item in items:
            item_is_assert = item.kind in ASSERTIVE_KINDS
            if winner is None:
                item_supports = False
            else:
                item_supports = item_is_assert == (winner == "assert")
            object.__setattr__(item, "supports_winner", bool(item_supports))

        confidence = min(1.0, score / rules.confidence_full_scale)
        judgment = Judgment(
            event_key=event_key,
            status=status,
            episode=open_episode.index,
            kind=kind,
            location_code=location,
            score=score,
            confidence=confidence,
            opposition_score=min(score_assert, score_clear),
            active_since=open_episode.anchor_observed_at,
            last_observed_at=latest_assert,
            cleared_at=cleared_at,
            rationale=tuple(rationale),
            evidence_ids=tuple(
                envelope.observation_id
                for envelope in usable
                if (envelope.obs.kind.value in ASSERTIVE_KINDS) == (winner != "clear")
            ) if winner else tuple(e.observation_id for e in usable),
        )
        self._judgments[event_key] = judgment
        self._evidence[event_key] = tuple(items)

        if status is JudgmentStatus.CLEARED:
            open_episode.sealed = True
            open_episode.sealed_at = now
            open_episode.clear_observed_at = cleared_at
            return True
        return False

    # ------------------------------------------------------------------ 查询

    def judgment(self, event_key: str) -> Judgment | None:
        return self._judgments.get(event_key)

    def evidence(self, event_key: str) -> tuple[EvidenceItem, ...]:
        return self._evidence.get(event_key, ())

    def episodes(self, event_key: str) -> tuple[_Episode, ...]:
        return tuple(self._episodes.get(event_key, ()))

    def observation(self, observation_id: str) -> _Envelope | None:
        return self._store.get(observation_id)

    def all_observation_ids(self) -> tuple[str, ...]:
        return tuple(self._store)

    def all_envelopes(self) -> dict[str, _Envelope]:
        return dict(self._store)

    def all_event_keys(self) -> tuple[str, ...]:
        return tuple(self._episodes)

    def refresh_open_episodes(self, now: datetime) -> None:
        """按给定"当前时刻"重算所有未封存片段（重演视图使用）。"""
        for event_key in list(self._episodes):
            self._reevaluate(event_key, now)

    # ------------------------------------------------------------------ 内部

    def _stale_to_sealed_predecessor(
        self,
        episodes: list[_Episode],
        open_episode: _Episode,
        observed_at: datetime,
        now: datetime,
    ) -> str | None:
        """进行中的是第 N 片段时，观测时刻落在第 N-1 片段解除界内的报文属迟到。"""
        if open_episode.index <= 1:
            return None
        predecessor = episodes[open_episode.index - 2]
        if predecessor.clear_observed_at is None:
            return None
        rules = self.rules.version_at(now)
        boundary = predecessor.clear_observed_at + timedelta(
            seconds=rules.recurrence_gap_seconds
        )
        if observed_at <= boundary:
            return (
                f"迟到消息：观测时刻不晚于片段{predecessor.index}解除后"
                f"{rules.recurrence_gap_seconds:.0f}秒，所反映片段已封存，"
                "不得影响当前未封存判断"
            )
        return None

    def _open_episode(self, event_key: str) -> _Episode | None:
        return next(
            (ep for ep in self._episodes.get(event_key, []) if not ep.sealed), None
        )

    def _earliest_assertive_observed_at(
        self, event_key: str, episode: _Episode
    ) -> datetime | None:
        earliest: datetime | None = None
        for member_id in episode.members:
            envelope = self._store[member_id]
            obs = envelope.obs
            if obs.kind.value in ASSERTIVE_KINDS and not envelope.withdrawn:
                if earliest is None or obs.source.observed_at < earliest:
                    earliest = obs.source.observed_at
        return earliest


def oid_from(envelope: _Envelope) -> str:
    return envelope.observation_id
