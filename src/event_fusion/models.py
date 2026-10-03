"""合成判断与发布快照的领域模型。"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class JudgmentStatus(StrEnum):
    # 当前有未解除的占道/障碍
    ACTIVE = "active"
    # 事件已解除，对应发生片段已封存
    CLEARED = "cleared"
    # 证据冲突或不足，不能给出确定结论
    UNCERTAIN = "uncertain"
    # 尚无任何可成立判断
    NONE = "none"


# 断言事件存在的观测种类。
ASSERTIVE_KINDS = frozenset({"lane_closed", "obstacle"})
# 断言事件解除的观测种类。
CLEAR_KINDS = frozenset({"lane_open", "cleared"})


@dataclass(frozen=True)
class EvidenceItem:
    observation_id: str
    kind: str
    location_code: str
    source_type: str
    external_id: str
    observed_at: datetime
    received_at: datetime
    confidence: float
    capability: float
    age_seconds: float
    weight: float
    supports_winner: bool
    withdrawn: bool = False
    excluded_reason: str | None = None  # 重放/迟到/撤回时给出原因

    def to_dict(self) -> dict[str, Any]:
        return {
            "observation_id": self.observation_id,
            "kind": self.kind,
            "location_code": self.location_code,
            "source_type": self.source_type,
            "external_id": self.external_id,
            "observed_at": self.observed_at.isoformat(),
            "received_at": self.received_at.isoformat(),
            "confidence": self.confidence,
            "capability": self.capability,
            "age_seconds": round(self.age_seconds, 3),
            "weight": round(self.weight, 6),
            "supports_winner": self.supports_winner,
            "withdrawn": self.withdrawn,
            "excluded_reason": self.excluded_reason,
        }


@dataclass(frozen=True)
class Judgment:
    event_key: str
    status: JudgmentStatus
    episode: int
    kind: str | None
    location_code: str | None
    score: float
    confidence: float
    opposition_score: float
    active_since: datetime | None
    last_observed_at: datetime | None
    cleared_at: datetime | None
    rationale: tuple[str, ...]
    evidence_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_key": self.event_key,
            "status": self.status.value,
            "episode": self.episode,
            "kind": self.kind,
            "location_code": self.location_code,
            "score": round(self.score, 6),
            "confidence": round(self.confidence, 6),
            "opposition_score": round(self.opposition_score, 6),
            "active_since": self.active_since.isoformat() if self.active_since else None,
            "last_observed_at": (
                self.last_observed_at.isoformat() if self.last_observed_at else None
            ),
            "cleared_at": self.cleared_at.isoformat() if self.cleared_at else None,
            "rationale": list(self.rationale),
            "evidence_ids": list(self.evidence_ids),
        }


@dataclass(frozen=True)
class TrafficSnapshot:
    """一次面向下游的路况发布；一经生成不可修改。"""

    publication_id: str
    event_key: str
    sequence: int
    published_at: datetime
    judgment: Judgment
    evidence: tuple[EvidenceItem, ...]
    rules_version: str
    rules_fingerprint: str
    supersedes: str | None = None
    sealed: bool = False  # 解除发布封存当前发生片段

    def to_dict(self) -> dict[str, Any]:
        return {
            "publication_id": self.publication_id,
            "event_key": self.event_key,
            "sequence": self.sequence,
            "published_at": self.published_at.isoformat(),
            "sealed": self.sealed,
            "supersedes": self.supersedes,
            "rules_version": self.rules_version,
            "rules_fingerprint": self.rules_fingerprint,
            "judgment": self.judgment.to_dict(),
            "evidence": [item.to_dict() for item in self.evidence],
        }
