"""只增报文日志。

每一份到达平台的原始报文都按接收顺序登记，包括被判定为重复的报文
（以 duplicate_of 指向首次接收序号）。日志只增不改，是撤回、修订、
迟到判定与历史重演的唯一事实来源。
"""

from dataclasses import dataclass

from .contracts import SourceReport


@dataclass(frozen=True)
class JournalEntry:
    seq: int
    report: SourceReport
    # 非空表示这是此前已收报文的重复送达，值为首次接收的序号。
    duplicate_of: int | None = None


@dataclass(frozen=True)
class ReceiptResult:
    entry: JournalEntry
    duplicated: bool


class Journal:
    def __init__(self) -> None:
        self._entries: list[JournalEntry] = []
        self._first_seq: dict[str, int] = {}

    def append(self, report: SourceReport) -> ReceiptResult:
        """登记一份原始报文；重复 report_id 只追加一条重复记录。"""
        seq = len(self._entries) + 1
        original = self._first_seq.get(report.report_id)
        entry = JournalEntry(
            seq=seq,
            report=report,
            duplicate_of=None if original is None else original,
        )
        if original is None:
            self._first_seq[report.report_id] = seq
        self._entries.append(entry)
        return ReceiptResult(entry=entry, duplicated=original is not None)

    @property
    def entries(self) -> list[JournalEntry]:
        return list(self._entries)

    def entries_until_seq(self, seq: int) -> list[JournalEntry]:
        return [e for e in self._entries if e.seq <= seq]

    def last_seq(self) -> int:
        return len(self._entries)

    def first_seen_seq(self, report_id: str) -> int | None:
        return self._first_seq.get(report_id)

    def report_by_id(self, report_id: str) -> SourceReport | None:
        seq = self._first_seq.get(report_id)
        if seq is None:
            return None
        return self._entries[seq - 1].report
