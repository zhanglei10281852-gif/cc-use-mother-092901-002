"""版本化的融合规则。

每次下游发布都绑定规则版本与参数指纹；运维按历史时点重演时，取当时生效
的规则版本，从而保证"当时判断"可复算、可审计。
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class FusionRules:
    version: str = "1.0.0"
    # 新鲜度半衰期：观测年龄每经过该秒数，证据权重衰减一半。
    freshness_half_life_seconds: float = 120.0
    # 每增加一个不同来源类型的印证，组合得分的加成比例。
    cross_confirm_bonus: float = 0.15
    # 交叉印证加成最多计几个额外来源，避免无限堆车端报文压过权威来源。
    cross_confirm_max_extra_sources: int = 2
    # 发布门槛：胜方组合得分不得低于该值，防止单一弱证据直接下发。
    publish_score_threshold: float = 0.35
    # 胜方相对负方的最小领先幅度（比例）。
    winner_margin_ratio: float = 0.15
    # 组合得分折算判断置信度时的满量程。
    confidence_full_scale: float = 0.80
    # 复发判定间隔：解除之后，观测时刻晚于解除时刻超过该间隔才算新一次发生，
    # 否则视为旧消息重放（同时承担时钟偏差容忍带的作用）。
    recurrence_gap_seconds: float = 60.0
    # 解除权威门槛：能力不低于该值的来源（路侧、人工）作出解除时，
    # 其解除结论可取代严格更早的占道观测；普通车辆解除不具备该效力。
    clear_authority_capability: float = 0.80
    # 相邻两次发布之间，置信度变化小于该值视为无实质变化，不再重复发布。
    significant_confidence_delta: float = 0.10

    def params(self) -> dict[str, object]:
        return {
            "freshness_half_life_seconds": self.freshness_half_life_seconds,
            "cross_confirm_bonus": self.cross_confirm_bonus,
            "cross_confirm_max_extra_sources": self.cross_confirm_max_extra_sources,
            "publish_score_threshold": self.publish_score_threshold,
            "winner_margin_ratio": self.winner_margin_ratio,
            "confidence_full_scale": self.confidence_full_scale,
            "recurrence_gap_seconds": self.recurrence_gap_seconds,
            "clear_authority_capability": self.clear_authority_capability,
            "significant_confidence_delta": self.significant_confidence_delta,
        }

    @property
    def fingerprint(self) -> str:
        body = json.dumps(
            {"version": self.version, "params": self.params()},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class _VersionedRules:
    effective_from: datetime
    rules: FusionRules


class RuleRegistry:
    """按生效时间管理规则版本，未登记时回退到内置 1.0.0。"""

    def __init__(self, versions: list[_VersionedRules] | None = None) -> None:
        self._versions = sorted(versions or [], key=lambda v: v.effective_from)

    @classmethod
    def default(cls) -> "RuleRegistry":
        return cls([_VersionedRules(datetime.min.replace(tzinfo=timezone.utc), FusionRules())])

    def add_version(self, effective_from: datetime, rules: FusionRules) -> None:
        if effective_from.tzinfo is None:
            raise ValueError("生效时间必须携带时区信息")
        self._versions.append(_VersionedRules(effective_from, rules))
        self._versions.sort(key=lambda v: v.effective_from)

    def version_at(self, instant: datetime) -> FusionRules:
        chosen = self._versions[0].rules if self._versions else FusionRules()
        for item in self._versions:
            if item.effective_from <= instant:
                chosen = item.rules
            else:
                break
        return chosen

    def current(self) -> FusionRules:
        return self._versions[-1].rules
