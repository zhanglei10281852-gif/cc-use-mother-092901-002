"""服务层测试：摄入幂等、快照证据绑定、封存冻结与时点重演。"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from event_fusion.contracts import ObservationKind, ReportAction
from event_fusion.rules import RULES_V1
from event_fusion.service import FusionService

from test_engine import report, t


class IngestIdempotencyTests(unittest.TestCase):
    def test_duplicate_report_does_not_republish(self):
        svc = FusionService()
        r1 = report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                    ObservationKind.LANE_CLOSED, confidence=0.95, received_at=t(5))
        first = svc.ingest(r1)
        self.assertFalse(first.receipt.duplicated)
        self.assertEqual(len(first.published), 1)

        again = svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                                  ObservationKind.LANE_CLOSED, confidence=0.95, received_at=t(5)))
        self.assertTrue(again.receipt.duplicated)
        self.assertEqual(again.receipt.entry.duplicate_of, first.receipt.entry.seq)
        self.assertEqual(again.published, ())
        # 重复报文仍逐字留在只增日志中。
        self.assertEqual(len(svc.journal.entries), 2)
        self.assertEqual(len(svc.ledger.for_event("EV-1")), 1)

    def test_no_change_no_new_snapshot(self):
        svc = FusionService()
        svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                          ObservationKind.LANE_CLOSED, confidence=0.95, received_at=t(5)))
        # 未触及 EV-1 的另一事件报文，不应让 EV-1 重新发布。
        svc.ingest(report("o1", ReportAction.OBSERVE, "rsu", "rs-8", t(0),
                          ObservationKind.OBSTACLE, event="EV-OTHER",
                          confidence=0.9, received_at=t(6)))
        self.assertEqual(len(svc.ledger.for_event("EV-1")), 1)


class SnapshotBindingTests(unittest.TestCase):
    def test_snapshot_binds_evidence_and_rules_version(self):
        svc = FusionService()
        svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                          ObservationKind.LANE_CLOSED, confidence=0.95, received_at=t(5)))
        svc.ingest(report("i1", ReportAction.OBSERVE, "inspector", "patrol-2", t(8),
                          ObservationKind.LANE_CLOSED, confidence=0.9, received_at=t(12)))
        snaps = svc.ledger.for_event("EV-1")
        latest = snaps[-1]
        self.assertEqual(latest.rules_version, RULES_V1.version)
        self.assertIn("r1", latest.evidence_report_ids)
        self.assertIn("i1", latest.evidence_report_ids)
        self.assertEqual(len(latest.content_hash), 64)
        self.assertIsNotNone(latest.chain_hash)
        self.assertTrue(svc.ledger.verify_chain())


class LateEvidenceTests(unittest.TestCase):
    def test_late_report_before_seal_still_counts(self):
        svc = FusionService()
        # 车辆观测发生很早，但在封存前才上报；封存点尚未冻结，应纳入判断。
        late = report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(0),
                      ObservationKind.LANE_CLOSED, location="lane-2",
                      confidence=0.55, received_at=t(50))
        svc.ingest(late)
        judgment = svc.current_judgment(t(55)).judgments["EV-1"]
        self.assertEqual(judgment.location_code, "lane-2")

    def test_report_arriving_after_seal_is_journaled_but_ignored(self):
        svc = FusionService()
        svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                          ObservationKind.LANE_CLOSED, location="lane-3",
                          confidence=0.95, received_at=t(5)))
        svc.seal_event("EV-1", t(10))
        before = len(svc.ledger.all())

        svc.ingest(report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(2),
                          ObservationKind.LANE_CLOSED, location="lane-2",
                          confidence=0.9, received_at=t(60)))
        self.assertEqual(len(svc.ledger.all()), before)
        sealed = svc.ledger.for_event("EV-1")[-1]
        self.assertEqual(sealed.location_code, "lane-3")
        # 迟到报文本身仍完整保留。
        self.assertEqual(svc.journal.last_seq(), 2)

    def test_late_evidence_cannot_alter_confidence_after_seal(self):
        svc = FusionService()
        svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                          ObservationKind.LANE_CLOSED, location="lane-3",
                          confidence=0.95, received_at=t(5)))
        svc.ingest(report("i1", ReportAction.OBSERVE, "inspector", "patrol-2", t(3),
                          ObservationKind.LANE_CLOSED, location="lane-3",
                          confidence=0.9, received_at=t(7)))
        sealed = svc.seal_event("EV-1", t(10))
        # 车辆迟到但同结论的高置信报文，不得引发重新发布。
        outcome = svc.ingest(report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(2),
                                    ObservationKind.LANE_CLOSED, location="lane-3",
                                    confidence=0.99, received_at=t(50)))
        self.assertEqual(outcome.published, ())
        self.assertEqual(svc.ledger.for_event("EV-1")[-1].snapshot_id, sealed.snapshot_id)

    def test_legitimate_clearance_after_seal_is_published(self):
        svc = FusionService()
        svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                          ObservationKind.LANE_CLOSED, location="lane-3",
                          confidence=0.95, received_at=t(5)))
        sealed = svc.seal_event("EV-1", t(10))
        self.assertEqual(sealed.status, "active")

        # 封存后两个独立来源确认解除：生命周期推进，发布 cleared 快照。
        svc.ingest(report("r2", ReportAction.OBSERVE, "rsu", "rsu-7", t(60),
                          ObservationKind.CLEARED, location="lane-3",
                          confidence=0.95, received_at=t(65)))
        outcome = svc.ingest(report("i2", ReportAction.OBSERVE, "inspector", "patrol-2", t(62),
                                    ObservationKind.CLEARED, location="lane-3",
                                    confidence=0.95, received_at=t(68)))
        self.assertEqual(len(outcome.published), 1)
        self.assertEqual(outcome.published[0].status, "cleared")
        # 封存快照原封不动。
        self.assertEqual(svc.ledger.get(sealed.snapshot_id).status, "active")
        self.assertTrue(svc.ledger.verify_chain())


class ReplayTests(unittest.TestCase):
    def _feed(self, svc):
        svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(0),
                          ObservationKind.LANE_CLOSED, confidence=0.95, received_at=t(5)))
        svc.ingest(report("i1", ReportAction.OBSERVE, "inspector", "patrol-2", t(20),
                          ObservationKind.LANE_CLOSED, confidence=0.9, received_at=t(25)))

    def test_replay_compares_then_and_now_without_rewriting(self):
        svc = FusionService()
        # t(3) 时还没有任何报文到达。
        self._feed(svc)
        snaps_before = len(svc.ledger.all())

        comparison = svc.replay(t(3))
        self.assertEqual(comparison.historical, {})
        self.assertIn("EV-1", comparison.current)
        self.assertEqual(comparison.current["EV-1"].status.value, "active")
        self.assertIn("EV-1", comparison.changes)
        self.assertEqual(
            [r["report_id"] for r in comparison.new_reports],
            ["r1", "i1"],
        )
        self.assertTrue(comparison.published_snapshots_untouched)
        # 重演不产生任何快照。
        self.assertEqual(len(svc.ledger.all()), snaps_before)
        self.assertTrue(svc.ledger.verify_chain())

    def test_replay_at_moment_conflicts_were_unresolved(self):
        svc = FusionService()
        svc.ingest(report("v1", ReportAction.OBSERVE, "vehicle", "car-9", t(0),
                          ObservationKind.LANE_CLOSED, location="lane-2",
                          confidence=0.8, received_at=t(5)))
        at_t6 = svc.replay(t(6))
        self.assertEqual(at_t6.historical["EV-1"].location_code, "lane-2")

        # 事后更有能力的来源认定是 lane-3。
        svc.ingest(report("r1", ReportAction.OBSERVE, "rsu", "rsu-7", t(10),
                          ObservationKind.LANE_CLOSED, location="lane-3",
                          confidence=0.95, received_at=t(15)))
        svc.ingest(report("i1", ReportAction.OBSERVE, "inspector", "patrol-2", t(12),
                          ObservationKind.LANE_CLOSED, location="lane-3",
                          confidence=0.9, received_at=t(18)))
        compare = svc.replay(t(6))
        self.assertEqual(compare.changes["EV-1"]["then"]["location_code"], "lane-2")
        self.assertEqual(compare.changes["EV-1"]["now"]["location_code"], "lane-3")

    def test_replay_uses_requested_rules_version(self):
        svc = FusionService()
        self._feed(svc)
        comparison = svc.replay(t(30), rules_version=RULES_V1.version)
        self.assertEqual(comparison.rules_version, RULES_V1.version)


if __name__ == "__main__":
    unittest.main()
