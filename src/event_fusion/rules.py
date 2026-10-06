"""版本化的合成规则。

规则集不可变、带版本号；任何一次合成判断都记录所使用的规则版本，
快照因此可以随规则演进复算并与历史结论对照。
"""

from dataclasses import dataclass
from enum import StrEnum


class LaneConflictPolicy(StrEnum):
    """车道编号在来源间不一致时的取舍策略。"""

    FRESH_CAPABLE = "fresh_capable"  # 优先取更新鲜且能力更强的来源
    CAPABILITY_FIRST = "capability_first"  # 能力优先，新鲜度仅作同权破平


@dataclass(frozen=True)
class RuleSet:
    version: str
    # 来源类型 -> 能力权重（0~1），体现 RSU/车辆/人工的固有可信度。
    source_capability: dict[str, float]
    # 观测距判定时刻多久（秒）开始线性衰减，以及完全失效时长。
    fresh_horizon_s: float
    stale_horizon_s: float
    # 每个独立来源印证时的置信增益（封顶于 1）。
    corroboration_bonus: float
    # 仅凭单一来源时允许给出的最高置信，防止单源误报直接下发。
    single_source_cap: float
    # 判定为活跃所需的最低综合置信；低于该值结论为 unknown。
    active_threshold: float
    # “解除”观测要压过“活跃”观测所需的加权净胜幅度。
    clear_margin: float
    # 解除后再次出现活跃观测，观测时刻晚于解除时刻多少秒才算复发，
    # 否则视为迟到的旧消息重放。
    recurrence_gap_s: float
    lane_conflict_policy: LaneConflictPolicy
    description: str = ""

    def capability(self, source_type: str) -> float:
        return self.source_capability.get(source_type, 0.3)

    def freshness(self, age_s: float) -> float:
        """观测年龄 -> 新鲜度系数，线性衰减并截断到 [0,1]。"""
        if age_s <= self.fresh_horizon_s:
            return 1.0
        if age_s >= self.stale_horizon_s:
            return 0.0
        span = self.stale_horizon_s - self.fresh_horizon_s
        return (self.stale_horizon_s - age_s) / span


RULES_V1 = RuleSet(
    version="fusion-rules-v1",
    source_capability={
        "rsu": 0.90,       # 路侧单元：定点、校准好
        "inspector": 0.85,  # 人工巡检：判断准但更新慢
        "vehicle": 0.65,   # 车辆：覆盖广但个体误报多
    },
    fresh_horizon_s=60.0,
    stale_horizon_s=600.0,
    corroboration_bonus=0.15,
    single_source_cap=0.75,
    active_threshold=0.5,
    clear_margin=0.12,
    recurrence_gap_s=30.0,
    lane_conflict_policy=LaneConflictPolicy.FRESH_CAPABLE,
    description="首版：能力权重 + 线性新鲜度 + 独立来源交叉印证 + 单源限速",
)


_REGISTRY: dict[str, RuleSet] = {RULES_V1.version: RULES_V1}


def register_rules(rules: RuleSet) -> None:
    """登记新版本规则；版本号一经登记不可覆盖。"""
    if rules.version in _REGISTRY:
        raise ValueError(f"规则版本已存在且不可覆盖：{rules.version}")
    _REGISTRY[rules.version] = rules


def get_rules(version: str) -> RuleSet:
    try:
        return _REGISTRY[version]
    except KeyError:
        raise KeyError(f"未知规则版本：{version}") from None


def latest_rules() -> RuleSet:
    return RULES_V1
