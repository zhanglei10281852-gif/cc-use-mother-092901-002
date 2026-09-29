"""车路云观测与事件判断所需的数据契约。"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class ObservationKind(StrEnum):
    LANE_CLOSED = "lane_closed"
    LANE_OPEN = "lane_open"
    OBSTACLE = "obstacle"
    CLEARED = "cleared"


@dataclass(frozen=True)
class SourceRef:
    source_type: str
    external_id: str
    observed_at: datetime


@dataclass(frozen=True)
class EventObservation:
    event_key: str
    kind: ObservationKind
    location_code: str
    source: SourceRef
    confidence: float

    def __post_init__(self) -> None:
        if not 0 <= self.confidence <= 1:
            raise ValueError("可信度必须位于零到一之间")
