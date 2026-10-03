"""车路云事件可信合成服务离线冒烟演示。

不启动 HTTP，直接驱动 FusionService，串演：
矛盾上报与仲裁 → 重复去重 → 解除封存 → 迟到/重放排除 → 复发 → 时点重演对比。
运行：python3 run_cli.py
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from event_fusion.clock import ManualClock
from event_fusion.contracts import EventObservation, ObservationKind, SourceRef
from event_fusion.service import FusionService

T0 = datetime(2026, 10, 21, 2, 0, 0, tzinfo=timezone.utc)


def make_obs(key, kind, source_type, external_id, minute, location, confidence):
    return EventObservation(
        event_key=key,
        kind=ObservationKind(kind),
        location_code=location,
        source=SourceRef(
            source_type, external_id, T0 + timedelta(minutes=minute)
        ),
        confidence=confidence,
    )


def main() -> None:
    clock = ManualClock(T0)
    svc = FusionService(clock=clock)
    key = "ROAD-施工占道-A7"

    def feed(label, obs, at_minute):
        clock.set(T0 + timedelta(minutes=at_minute))
        result = svc.ingest_observation(obs)
        published = result.published.publication_id if result.published else "—"
        judgment = result.judgment
        tail = f"{judgment.status.value}/{judgment.location_code or '-'}" if judgment else "无判断"
        print(f"{label:<34} -> {result.classification:<10} 发布:{published:<36} 判断:{tail}")

    print("== 现场时间线（接收顺序） ==")
    feed("车辆报 lane-2 (02:00)", make_obs(key, "obstacle", "vehicle", "veh-88", 0, "lane-2", 0.7), 0)
    feed("重复报文(内容被篡改)", make_obs(key, "obstacle", "vehicle", "veh-88", 9, "lane-9", 0.99), 1)
    feed("路侧报 lane-3 (02:01)", make_obs(key, "lane_closed", "rsu", "rsu-3", 1, "lane-3", 0.95), 1)
    feed("人工确认 lane-3 (02:02)", make_obs(key, "obstacle", "inspector", "patrol-2", 2, "lane-3", 0.9), 2)
    feed("人工解除 (02:05)", make_obs(key, "lane_open", "inspector", "patrol-clear", 5, "lane-3", 0.95), 5)
    feed("迟到报文声称02:03", make_obs(key, "lane_closed", "vehicle", "veh-late", 3, "lane-3", 0.8), 15)
    feed("02:06占道=重放", make_obs(key, "obstacle", "rsu", "rsu-replay", 6, "lane-3", 0.9), 7)
    feed("02:10再次占道=复发", make_obs(key, "lane_closed", "rsu", "rsu-7", 10, "lane-1", 0.95), 12)

    print("\n== 发布链（不可变，绑定证据与规则版本） ==")
    for snapshot in svc.publications(key):
        print(
            f"  {snapshot.publication_id} {snapshot.published_at:%H:%M} "
            f"{snapshot.judgment.status.value} ep{snapshot.judgment.episode} "
            f"封存={snapshot.sealed} 规则={snapshot.rules_version}({snapshot.rules_fingerprint}) "
            f"证据={len(snapshot.evidence)}条"
        )

    print("\n== 时点重演：以 02:07:30 回看 ==")
    view = svc.replay_at(T0 + timedelta(minutes=7, seconds=30))
    comparison = view.compare(key)
    then = comparison["judgment_then"]
    now = comparison["judgment_now"]
    print(f"  当时: {then['status']} ep{then['episode']}；现在: {now['status']} ep{now['episode']} {now['location_code']}")
    print(f"  差异字段: {list(comparison['changed_fields'])}")
    print(f"  事后新增证据: {[e['external_id'] for e in comparison['evidence_arrived_after']]}")
    print(f"  历史发布未被改写: {comparison['history_unchanged']}")

    sealed_snapshot = svc.publications(key)[3]
    print("\n== 封存快照中的证据留痕 ==")
    print(json.dumps(sealed_snapshot.to_dict()["evidence"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
