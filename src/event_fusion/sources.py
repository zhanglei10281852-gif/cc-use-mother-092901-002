"""来源能力登记。

融合时"来源能力"是第一类权重依据：路侧单元（rsu）设备校准好、位置固定，
能力最高；人工巡检（inspector）到场核实结论可靠；普通车辆（vehicle）
视角受限，能力较低。未知来源采用保守默认值。
"""

from dataclasses import dataclass


DEFAULT_CAPABILITIES: dict[str, float] = {
    "rsu": 0.90,
    "inspector": 0.85,
    "vehicle": 0.60,
}


@dataclass(frozen=True)
class SourceRegistry:
    capabilities: dict[str, float]
    default_capability: float = 0.30

    @classmethod
    def default(cls) -> "SourceRegistry":
        return cls(dict(DEFAULT_CAPABILITIES), 0.30)

    def capability(self, source_type: str) -> float:
        return float(self.capabilities.get(source_type, self.default_capability))
