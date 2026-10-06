"""合成引擎层测试：冲突权衡、修订撤回、片段切分、复发与重放。"""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from event_fusion.contracts import (
    EventObservation,
    EventStatus,
    ObservationKind,
    ReportAction,
    SourceRef,
    SourceReport,
)
from event_fusion.engine import Engine
from event_fusion.journal import Journal
from event_fusion.rules import RULES_V1

T0 = datetime(2026, 10, 21, 2, 0, 0, tzinfo=timezone.utc)


def t(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def report(
    rid,
    action,
    source_type,
    source_id,
    observed_at,
    kind=None,
    event="EV-1",
    location="lane-3",
    confidence=0.9,
    received_at=None,
    target=None,
):
    source = SourceRef(source_type, source_id, observed_at)
    observation = None
    if kind is not None:
        observation = EventObservation(event, kind, location, source, confidence)
    return SourceReport(
        report_id=rid,
        action=action,
        source=source,
        received_at=received_at or observed_at + timedelta(seconds=5),
        observation=observation,
        target_report_id=target,
    )


def build(seq):
    journal = Journal()
    for r in seq:
        journal.append(r)
    return journal


class FusionConflictTests(unittest.TestCase):
    def setUp(self):
        self.engine = Engine(RULES_V1)

    def test_cross_source_corroboration_beats_single_source_cap(self):
        seq = [
            report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0), ObservationKind.LANE_CLOSED, confidence=0.95),
            report("r2", ReportAction.OBSERVE, "vehicle", "car-9", t(5), ObservationKind.LANE_CLOSED,
                   location="lane-2", confidence=0.8),
            report("r3", ReportAction.OBSERVE, "inspector", "patrol-2", t(20),
                   ObservationKind.LANE_CLOSED, confidence=0.9),
        ]
        result = self.engine.synthesize(build(seq).entries, t(40))
        j = result.judgments["EV-1"]
        self.assertEqual(j.status, EventStatus.ACTIVE)
        # RSU 与人工巡检相互印证，车道应判为 lane-3，车辆的 lane-2 列为冲突。
        self.assertEqual(j.location_code, "lane-3")
        self.assertIn("lane-2", j.lane_conflicts)
        # 两个独立来源带来印证增益，结论置信高于单源限速 0.75。
        self.assertGreater(j.confidence, RULES_V1.single_source_cap)

    def test_single_source_is_capped(self):
        seq = [
            report("r1", ReportAction.OBSERVE, "vehicle", "car-9", t(0),
                   ObservationKind.LANE_CLOSED, confidence=0.99),
        ]
        j = self.engine.synthesize(build(seq).entries, t(10)).judgments["EV-1"]
        self.assertEqual(j.status, EventStatus.ACTIVE)
        self.assertLessEqual(j.confidence, RULES_V1.single_source_cap)

    def test_capability_freshness_resolve_time_conflict(self):
        # 车辆称 60 秒前封道，人工巡检刚刚报告已解除，能力与新鲜度都站在巡检一侧。
        seq = [
            report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(0),
                   ObservationKind.LANE_CLOSED, confidence=0.9, received_at=t(2)),
            report("i1", ReportAction.OBSERVE, "inspector", "patrol-2", t(90),
                   ObservationKind.CLEARED, confidence=0.95, received_at=t(95)),
        ]
        j = self.engine.synthesize(build(seq).entries, t(100)).judgments["EV-1"]
        self.assertEqual(j.status, EventStatus.CLEARED)
        self.assertEqual(j.cleared_at, t(90))

    def test_withdraw_removes_evidence(self):
        seq = [
            report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(0),
                   ObservationKind.OBSTACLE, confidence=0.9),
            report("v1w", ReportAction.WITHDRAW, "vehicle", "car-9", t(30),
                   received_at=t(35), target="v1"),
        ]
        j = self.engine.synthesize(build(seq).entries, t(40)).judgments["EV-1"]
        # 证据全部失效时事件不消失，而是给出 unknown，便于下游收到撤销结论。
        self.assertEqual(j.status, EventStatus.UNKNOWN)
        self.assertEqual(j.evidence, ())
        self.assertTrue(any(e.reason == "withdrawn" and e.report_id == "v1" for e in j.excluded))

    def test_amend_replaces_observation(self):
        seq = [
            report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(0),
                   ObservationKind.LANE_CLOSED, location="lane-2", confidence=0.9),
            report("v1a", ReportAction.AMEND, "vehicle", "car-9", t(10),
                   ObservationKind.LANE_CLOSED, location="lane-3", confidence=0.55,
                   received_at=t(15), target="v1"),
        ]
        j = self.engine.synthesize(build(seq).entries, t(40)).judgments["EV-1"]
        self.assertEqual(j.location_code, "lane-3")
        reasons = {e.report_id: e.reason for e in j.excluded}
        self.assertEqual(reasons.get("v1"), "superseded")

    def test_cannot_withdraw_other_sources_report(self):
        seq = [
            report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0), ObservationKind.LANE_CLOSED),
            report("x1", ReportAction.WITHDRAW, "vehicle", "car-9", t(20),
                   received_at=t(25), target="r1"),
        ]
        j = self.engine.synthesize(build(seq).entries, t(40)).judgments["EV-1"]
        self.assertEqual(j.status, EventStatus.ACTIVE)
        self.assertTrue(any(e.reason == "link_violation" for e in j.excluded))


class EpisodeRecurrenceTests(unittest.TestCase):
    def setUp(self):
        self.engine = Engine(RULES_V1)

    def _cleared_sequence(self):
        return [
            report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                   ObservationKind.LANE_CLOSED, confidence=0.95),
            report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(5),
                   ObservationKind.LANE_CLOSED, location="lane-2", confidence=0.7),
            report("i1", ReportAction.OBSERVE, "inspector", "patrol-2", t(20),
                   ObservationKind.LANE_CLOSED, confidence=0.9),
            report("r2", ReportAction.OBSERVE, "rsu", "rsu-7", t(60),
                   ObservationKind.CLEARED, confidence=0.95, received_at=t(65)),
            report("i2", ReportAction.OBSERVE, "inspector", "patrol-2", t(90),
                   ObservationKind.CLEARED, confidence=0.95, received_at=t(95)),
        ]

    def test_two_independent_clears_close_episode(self):
        j = self.engine.synthesize(build(self._cleared_sequence()).entries, t(100)).judgments["EV-1"]
        self.assertEqual(j.status, EventStatus.CLEARED)
        self.assertEqual(j.episode_id, "EV-1#E1")

    def test_old_message_after_clear_is_replay_not_recurrence(self):
        seq = self._cleared_sequence() + [
            # 车辆晚到的旧观测：观测时刻在解除时刻+复发间隔之内。
            report("v2", ReportAction.OBSERVE, "vehicle", "car-9", t(95),
                   ObservationKind.LANE_CLOSED, confidence=0.9, received_at=t(130)),
        ]
        j = self.engine.synthesize(build(seq).entries, t(140)).judgments["EV-1"]
        self.assertEqual(j.status, EventStatus.CLEARED)
        self.assertEqual(j.episode_id, "EV-1#E1")
        excluded = {e.report_id: e.reason for e in j.excluded}
        self.assertEqual(excluded.get("v2"), "replay")

    def test_new_occurrence_opens_new_episode(self):
        seq = self._cleared_sequence() + [
            report("r3", ReportAction.OBSERVE, "rsu", "rsu-7", t(300),
                   ObservationKind.OBSTACLE, confidence=0.95, received_at=t(305)),
            report("i3", ReportAction.OBSERVE, "inspector", "patrol-2", t(310),
                   ObservationKind.OBSTACLE, confidence=0.9, received_at=t(315)),
        ]
        j = self.engine.synthesize(build(seq).entries, t(320)).judgments["EV-1"]
        self.assertEqual(j.status, EventStatus.ACTIVE)
        self.assertEqual(j.episode_id, "EV-1#E2")
        self.assertEqual(j.active_since, t(300))


class SealingTests(unittest.TestCase):
    def test_late_message_cannot_change_sealed_episode(self):
        from event_fusion.service import FusionService

        svc = FusionService()
        rsu_close = report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                           ObservationKind.LANE_CLOSED, confidence=0.95, received_at=t(5))
        svc.ingest(rsu_close)
        sealed = svc.seal_event("EV-1", t(10))
        self.assertTrue(sealed.sealed)

        # 封存后车辆的迟到撤回到达：只进日志，不产生新快照，不改封存结论。
        late = report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(2),
                      ObservationKind.LANE_CLOSED, confidence=0.2, received_at=t(40))
        outcome = svc.ingest(late)
        self.assertEqual(outcome.published, ())
        snaps = svc.ledger.for_event("EV-1")
        self.assertEqual(snaps[-1].snapshot_id, sealed.snapshot_id)
        self.assertEqual(snaps[-1].status, "active")
        self.assertTrue(snaps[-1].sealed)

    def test_recurrent_episode_can_be_sealed_again(self):
        from event_fusion.service import FusionService

        svc = FusionService()
        svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                          ObservationKind.LANE_CLOSED, confidence=0.95, received_at=t(5)))
        first = svc.seal_event("EV-1", t(10))
        svc.ingest(report("r2", ReportAction.OBSERVE, "rsu", "rsu-7", t(60),
                          ObservationKind.CLEARED, confidence=0.95, received_at=t(65)))
        svc.ingest(report("i2", ReportAction.OBSERVE, "inspector", "patrol-2", t(90),
                          ObservationKind.CLEARED, confidence=0.95, received_at=t(95)))
        svc.ingest(report("r3", ReportAction.OBSERVE, "rsu", "rsu-7", t(300),
                          ObservationKind.OBSTACLE, confidence=0.95, received_at=t(305)))
        svc.ingest(report("i3", ReportAction.OBSERVE, "inspector", "patrol-2", t(310),
                          ObservationKind.OBSTACLE, confidence=0.9, received_at=t(315)))
        second = svc.seal_event("EV-1", t(320))
        self.assertNotEqual(second.episode_id, first.episode_id)
        self.assertEqual(second.episode_id, "EV-1#E2")
        self.assertTrue(svc.ledger.verify_chain())


if __name__ == "__main__":
    unittest.main()
