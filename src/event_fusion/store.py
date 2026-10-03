"""仅追加事件日志。

观测、撤回、发布都作为不可变记录落盘；任何当前判断都可以由日志重放得到，
历史发布记录因此不可被事后新增证据改写。
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable


class RecordType:
    OBSERVATION = "observation"
    WITHDRAWAL = "withdrawal"
    PUBLICATION = "publication"


@dataclass(frozen=True)
class EventRecord:
    id: int
    recorded_at: datetime
    type: str
    payload: dict[str, Any]


def _parse_instant(value: str) -> datetime:
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None:
        raise ValueError("日志中的时刻必须携带时区信息")
    return instant


class EventLog:
    """内存日志，可选 JSONL 持久化；记录序号单调递增。"""

    def __init__(self, path: str | Path | None = None) -> None:
        self._path = Path(path) if path else None
        self._records: list[EventRecord] = []
        if self._path and self._path.exists():
            self._load()

    @property
    def records(self) -> tuple[EventRecord, ...]:
        return tuple(self._records)

    def append(self, recorded_at: datetime, type_: str, payload: dict[str, Any]) -> EventRecord:
        record = EventRecord(len(self._records) + 1, recorded_at, type_, dict(payload))
        self._records.append(record)
        if self._path:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "id": record.id,
                            "recorded_at": record.recorded_at.isoformat(),
                            "type": record.type,
                            "payload": record.payload,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        return record

    def replay(
        self,
        handlers: dict[str, Callable[[EventRecord], None]],
        cutoff: datetime | None = None,
    ) -> None:
        for record in self._records:
            if cutoff is not None and record.recorded_at > cutoff:
                break
            handler = handlers.get(record.type)
            if handler:
                handler(record)

    def find(self, predicate: Callable[[EventRecord], bool]) -> EventRecord | None:
        for record in self._records:
            if predicate(record):
                return record
        return None

    def filter(self, predicate: Callable[[EventRecord], bool]) -> Iterable[EventRecord]:
        return (record for record in self._records if predicate(record))

    def _load(self) -> None:
        assert self._path is not None
        with self._path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                raw = json.loads(line)
                self._records.append(
                    EventRecord(
                        id=raw["id"],
                        recorded_at=_parse_instant(raw["recorded_at"]),
                        type=raw["type"],
                        payload=raw["payload"],
                    )
                )
