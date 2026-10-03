"""可注入的时钟，便于测试与按指定时点重演。"""

from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """返回带时区信息的当前时刻。"""


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class ManualClock:
    """测试用固定时钟，可手动推进。"""

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("时刻必须携带时区信息")
        self._now = start

    def now(self) -> datetime:
        return self._now

    def set(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            raise ValueError("时刻必须携带时区信息")
        self._now = instant

    def advance(self, seconds: float) -> datetime:
        self._now = self._now + timedelta(seconds=seconds)
        return self._now
