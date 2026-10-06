"""车路云观测与事件判断所需的数据契约。"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class ObservationKind(StrEnum):
    LANE_CLOSED = "lane_closed"
    LANE_OPEN = "lane_open"
    OBSTACLE = "obstacle"
    CLEARED = "cleared"


class ReportAction(StrEnum):
    """上游报文的动作：首发观测、修订既有报文、撤回既有报文。"""

    OBSERVE = "observe"
    AMEND = "amend"
    WITHDRAW = "withdraw"


class EventStatus(StrEnum):
    """合成判断结论：事件活跃、已解除、证据不足。"""

    ACTIVE = "active"
    CLEARED = "cleared"
    UNKNOWN = "unknown"


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


@dataclass(frozen=True)
class SourceReport:
    """上游来源的一次完整报文。

    report_id 为来源侧的幂等编号；amend/withdraw 通过 target_report_id
    指向同一来源此前发出的报文。received_at 为平台接收时刻，
    观测本身的发生时刻以 source.observed_at 为准。
    """

    report_id: str
    action: ReportAction
    source: SourceRef
    received_at: datetime
    observation: EventObservation | None = None
    target_report_id: str | None = None

    def __post_init__(self) -> None:
        if self.action is ReportAction.WITHDRAW:
            if self.target_report_id is None:
                raise ValueError("撤回报文必须指向被撤回的报文编号")
        else:
            if self.observation is None:
                raise ValueError("观测与修订报文必须携带观测内容")
            if self.source != self.observation.source:
                raise ValueError("报文来源必须与观测来源一致")
        if self.action is ReportAction.AMEND and self.target_report_id is None:
            raise ValueError("修订报文必须指向被修订的报文编号")
