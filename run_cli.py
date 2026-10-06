"""施工占道场景冒烟演示（离线，无需启动 HTTP 服务）。

时间线（UTC）：
  02:00:02 车辆首先上报 2 号车道封闭（单源，受单源限速，先发布为 lane-2）
  02:00:20 人工巡检确认 3 号车道封闭，02:00:25 送达后纠正为 lane-3
  02:00:00 路侧单元 RSU 对 3 号车道的报文延迟，02:00:30 才送达，与巡检交叉印证
  02:00:40 值班员封存当前判断
  02:00:45 车辆迟到的 2 号车道报文在 02:01:10 才送达（封存后，只留痕不改判）
  02:03:00 同一地点再次出现障碍，RSU 与巡检交叉印证，判定为复发新片段 E2

并在 02:00:15 时点重演：当时只有车辆单源，看到的是 2 号车道；
事后新增证据把结论纠正为 3 号车道——历史发布记录不变。
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from event_fusion.contracts import EventObservation, ObservationKind, ReportAction, SourceRef, SourceReport
from event_fusion.serialization import snapshot_to_dict
from event_fusion.service import FusionService

T0 = datetime(2026, 10, 21, 2, 0, 0, tzinfo=timezone.utc)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def make_report(
    rid, source_type, source_id, observed_at, kind, location, confidence, received_at
) -> SourceReport:
    source = SourceRef(source_type, source_id, observed_at)
    observation = EventObservation("EV-CONSTR-7", kind, location, source, confidence)
    return SourceReport(
        report_id=rid,
        action=ReportAction.OBSERVE,
        source=source,
        received_at=received_at,
        observation=observation,
    )


def show(title, payload) -> None:
    print(f"\n===== {title} =====")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    svc = FusionService()

    first = svc.ingest(make_report(
        "veh-2001", "vehicle", "car-9", at(2),
        ObservationKind.LANE_CLOSED, "lane-2", 0.85, at(4)))
    show("车辆单源先到：临时发布 lane-2（置信受单源限速）",
         [{"snapshot": s.snapshot_id, "lane": s.location_code,
           "confidence": s.confidence} for s in first.published])

    svc.ingest(make_report(
        "pat-3001", "inspector", "patrol-2", at(20),
        ObservationKind.LANE_CLOSED, "lane-3", 0.90, at(25)))
    third = svc.ingest(make_report(
        "rsu-1001", "rsu", "rsu-7", at(0),
        ObservationKind.LANE_CLOSED, "lane-3", 0.95, at(30)))
    show("迟到的 RSU 与巡检交叉印证：纠正为 lane-3（车辆 lane-2 列为冲突）",
         [snapshot_to_dict(s) for s in third.published])

    # 车辆报文重复送达：只登记，不发布。
    duplicate = svc.ingest(make_report(
        "veh-2001", "vehicle", "car-9", at(2),
        ObservationKind.LANE_CLOSED, "lane-2", 0.85, at(4)))
    show("重复报文幂等", {"duplicated": duplicate.receipt.duplicated,
                          "new_published": len(duplicate.published)})

    sealed = svc.seal_event("EV-CONSTR-7", at(40))
    show("封存快照（证据与规则版本绑定）",
         {"snapshot": sealed.snapshot_id, "sealed": sealed.sealed,
          "lane": sealed.location_code, "rules_version": sealed.rules_version,
          "evidence": list(sealed.evidence_report_ids)})

    late = svc.ingest(make_report(
        "veh-2009", "vehicle", "car-9", at(45),
        ObservationKind.LANE_CLOSED, "lane-2", 0.99, at(70)))
    show("封存后迟到报文：只进日志，不触发发布",
         {"journal_seq": late.receipt.entry.seq, "new_published": len(late.published)})

    # 事件解除后复发：RSU 与巡检再次交叉印证。
    svc.ingest(make_report(
        "rsu-1090", "rsu", "rsu-7", at(80),
        ObservationKind.CLEARED, "lane-3", 0.95, at(85)))
    svc.ingest(make_report(
        "pat-3090", "inspector", "patrol-2", at(95),
        ObservationKind.CLEARED, "lane-3", 0.95, at(99)))
    recurrence = svc.ingest(make_report(
        "rsu-1200", "rsu", "rsu-7", at(180),
        ObservationKind.OBSTACLE, "lane-3", 0.95, at(184)))
    show("复发开启新片段 E2（而非旧消息重放）",
         [{"snapshot": s.snapshot_id, "episode": s.episode_id,
           "status": s.status, "lane": s.location_code} for s in recurrence.published])

    replay = svc.replay(at(15))
    show("时点重演对比：02:00:15 的当时判断 vs 事后新增证据",
         {
             "then": {
                 k: {"status": j.status.value, "lane": j.location_code,
                     "confidence": j.confidence}
                 for k, j in replay.historical.items()
             },
             "now": {
                 k: {"status": j.status.value, "lane": j.location_code,
                      "episode": j.episode_id}
                 for k, j in replay.current.items()
             },
             "new_reports_after_then": [r["report_id"] for r in replay.new_reports],
             "historical_snapshots_untouched": replay.published_snapshots_untouched,
             "chain_valid": svc.ledger.verify_chain(),
         })

    print("\n演示完成。")


if __name__ == "__main__":
    main()
