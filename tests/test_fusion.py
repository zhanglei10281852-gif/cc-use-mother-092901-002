"""可信合成核心场景测试。

覆盖：冲突仲裁、去重、撤回/修订、封存与迟到消息、复发与重放区分、
发布证据绑定、时点重演不改写历史、重启后日志重建。
"""

import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from event_fusion.clock import ManualClock
from event_fusion.contracts import EventObservation, ObservationKind, SourceRef
from event_fusion.engine import DUPLICATE, EXCLUDED, RECURRENCE
from event_fusion.models import JudgmentStatus
from event_fusion.rules import FusionRules, RuleRegistry
from event_fusion.service import FusionService
from event_fusion.sources import SourceRegistry

T0 = datetime(2026, 10, 21, 2, 0, 0, tzinfo=timezone.utc)


def obs(
    event_key,
    kind,
    source_type,
    external_id,
    observed_at,
    location="lane-3",
    confidence=0.9,
):
    return EventObservation(
        event_key=event_key,
        kind=ObservationKind(kind),
        location_code=location,
        source=SourceRef(source_type, external_id, observed_at),
        confidence=confidence,
    )


class ServiceTestBase(unittest.TestCase):
    def make_service(self, log_path=None, clock=None, rules=None):
        return FusionService(
            clock=clock or ManualClock(T0),
            log_path=log_path,
            sources=SourceRegistry.default(),
            rules=rules or RuleRegistry.default(),
        )


class ConflictArbitrationTests(ServiceTestBase):
    def test_cross_confirmed_authoritative_sources_win_and_publish_binds_evidence(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        # 车辆先报 2 车道，路侧与人工随后一致报 3 车道。
        r1 = svc.ingest_observation(
            obs("EV-1", "lane_closed", "vehicle", "v-7", T0, "lane-2", 0.8)
        )
        self.assertIsNotNone(r1.published)
        self.assertEqual(r1.published.judgment.location_code, "lane-2")

        clock.advance(5)
        r2 = svc.ingest_observation(
            obs("EV-1", "lane_closed", "rsu", "r-1", T0 + timedelta(seconds=5), "lane-3", 0.95)
        )
        # 权威路侧翻盘到 3 车道，构成实质变化必须发布。
        self.assertIsNotNone(r2.published)
        snap = r2.published
        self.assertEqual(snap.judgment.status, JudgmentStatus.ACTIVE)
        self.assertEqual(snap.judgment.location_code, "lane-3")
        # 快照绑定规则版本与当时全部证据，包括不支持胜方的车辆报文。
        self.assertEqual(snap.rules_version, "1.0.0")
        self.assertTrue(snap.rules_fingerprint)
        ids = {e.observation_id for e in snap.evidence}
        self.assertEqual(ids, {"EV-1|vehicle|v-7", "EV-1|rsu|r-1"})
        vehicle_item = next(e for e in snap.evidence if e.source_type == "vehicle")
        # 车辆同样断言"事件发生"，但它主张的 2 车道与仲裁结论 3 车道的
        # 分歧原样保留在证据明细中，可审计、可追责。
        self.assertTrue(vehicle_item.supports_winner)
        self.assertEqual(vehicle_item.location_code, "lane-2")
        self.assertEqual(snap.judgment.location_code, "lane-3")

        clock.advance(5)
        r3 = svc.ingest_observation(
            obs("EV-1", "obstacle", "inspector", "i-1", T0 + timedelta(seconds=10), "lane-3", 0.9)
        )
        # 结论未发生实质变化：不重复发布。
        self.assertIsNone(r3.published)
        # 但当前判断由三类来源交叉印证，理由中可追溯。
        current = svc.current_judgment("EV-1")
        self.assertIn("3类来源", current.rationale[0])
        self.assertEqual(len(svc.evidence("EV-1")), 3)

    def test_conflicting_clear_vs_close_without_margin_stays_uncertain(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        svc.ingest_observation(obs("EV-2", "lane_closed", "rsu", "r-1", T0, confidence=0.9))
        clock.advance(30)
        # 人工对同一时刻给出等强相反结论（同为 T0 观测，30 秒后送达），
        # 双方得分差不足最小领先比例 → 不确定，不下发占道或解除。
        result = svc.ingest_observation(
            obs("EV-2", "lane_open", "inspector", "i-1", T0, confidence=0.95)
        )
        self.assertEqual(result.judgment.status, JudgmentStatus.UNCERTAIN)
        # 已有 active 基线后转为不确定，属实质变化，应发布以阻止下游继续按占道行驶。
        self.assertEqual(result.published.judgment.status, JudgmentStatus.UNCERTAIN)


class DedupAndWithdrawalTests(ServiceTestBase):
    def test_duplicate_report_never_triggers_second_publication(self):
        svc = self.make_service()
        first = svc.ingest_observation(obs("EV-3", "lane_closed", "rsu", "r-9", T0))
        self.assertEqual(first.classification, "accepted")
        pubs_before = len(svc.publications("EV-3"))
        again = svc.ingest_observation(obs("EV-3", "lane_closed", "rsu", "r-9", T0))
        self.assertEqual(again.classification, DUPLICATE)
        self.assertIsNone(again.published)
        self.assertEqual(len(svc.publications("EV-3")), pubs_before)
        # 即使报文内容（车道、时间）不同，同来源外部编号仍按重复处理。
        mutated = obs("EV-3", "lane_closed", "rsu", "r-9", T0 + timedelta(hours=2), "lane-1")
        third = svc.ingest_observation(mutated)
        self.assertEqual(third.classification, DUPLICATE)
        self.assertEqual(len(svc.publications("EV-3")), pubs_before)

    def test_withdrawal_downgrades_judgment_and_is_recorded_in_evidence(self):
        svc = self.make_service()
        svc.ingest_observation(obs("EV-4", "lane_closed", "rsu", "r-1", T0))
        resp = svc.withdraw("EV-4", "rsu", "r-1", note="设备误报")
        self.assertEqual(resp.classification, "withdrawn")
        self.assertEqual(resp.judgment.status, JudgmentStatus.UNCERTAIN)
        snap = resp.published
        item = next(e for e in snap.evidence if e.external_id == "r-1")
        self.assertTrue(item.withdrawn)
        self.assertEqual(item.weight, 0.0)

    def test_withdrawal_before_observation_arrives_takes_effect_on_arrival(self):
        svc = self.make_service()
        self.assertIsNone(svc.withdraw("EV-5", "vehicle", "v-1"))
        resp = svc.ingest_observation(obs("EV-5", "lane_closed", "vehicle", "v-1", T0))
        self.assertEqual(resp.classification, "withdrawn")
        self.assertIn("撤回", resp.reason)
        self.assertIsNone(resp.published)
        self.assertIsNone(svc.current_judgment("EV-5"))

    def test_revision_replaces_old_report_both_versions_retained(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        svc.ingest_observation(
            obs("EV-6", "lane_closed", "rsu", "r-1", T0, "lane-2", 0.9)
        )
        clock.advance(20)
        resp = svc.revise_observation(
            event_key="EV-6",
            source_type="rsu",
            external_id="r-1",
            new_kind=ObservationKind.LANE_CLOSED,
            new_location_code="lane-4",
            new_confidence=0.92,
            new_observed_at=T0 + timedelta(seconds=20),
        )
        self.assertIsNotNone(resp.published)
        self.assertEqual(resp.published.judgment.location_code, "lane-4")
        # 旧报文撤回留痕，修订版是独立证据。
        snap = resp.published
        old = next(e for e in snap.evidence if e.external_id == "r-1")
        self.assertTrue(old.withdrawn)
        self.assertTrue(any(e.external_id == "r-1#rev2" for e in snap.evidence))


class SealingAndLateMessageTests(ServiceTestBase):
    def _sealed_event(self, svc, clock, key="EV-7"):
        svc.ingest_observation(obs(key, "lane_closed", "rsu", "r-1", T0))
        clock.advance(60)
        cleared = svc.ingest_observation(
            obs(key, "lane_open", "inspector", "i-1", T0 + timedelta(seconds=60), confidence=0.95)
        )
        self.assertTrue(cleared.published.sealed)
        self.assertEqual(cleared.published.judgment.status, JudgmentStatus.CLEARED)
        return cleared.published

    def test_clear_seals_episode_and_publishes_clearance(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        snap = self._sealed_event(svc, clock)
        self.assertIsNotNone(snap.supersedes)

    def test_late_message_cannot_affect_sealed_judgment(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        self._sealed_event(svc, clock)
        pubs_after_seal = len(svc.publications("EV-7"))
        # 200 秒后才到一条声称时刻为 T0+10 的占道报文 → 迟到，封存判断不变。
        clock.advance(200)
        late = svc.ingest_observation(
            obs("EV-7", "obstacle", "vehicle", "v-late", T0 + timedelta(seconds=10))
        )
        self.assertEqual(late.classification, EXCLUDED)
        self.assertIn("迟到", late.reason)
        self.assertIsNone(late.published)
        self.assertEqual(len(svc.publications("EV-7")), pubs_after_seal)
        self.assertEqual(svc.current_judgment("EV-7").status, JudgmentStatus.CLEARED)

    def test_clear_for_open_episode_with_assertive_after_it_is_excluded(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        # 先收到声称 T0+120 的解除，但占道发生时刻是 T0+180（解除早于发生）。
        bad_clear = svc.ingest_observation(
            obs("EV-8", "lane_open", "rsu", "r-clear", T0 + timedelta(seconds=120))
        )
        self.assertEqual(bad_clear.classification, EXCLUDED)  # 无进行中片段

    def test_withdrawal_inside_sealed_episode_does_not_change_judgment(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        self._sealed_event(svc, clock, "EV-9b")
        pubs = len(svc.publications("EV-9b"))
        resp = svc.withdraw("EV-9b", "rsu", "r-1", note="事后发现误报")
        self.assertEqual(resp.classification, "withdrawn")
        self.assertIsNone(resp.published)  # 封存片段不重开、不改判
        self.assertEqual(len(svc.publications("EV-9b")), pubs)
        self.assertEqual(svc.current_judgment("EV-9b").status, JudgmentStatus.CLEARED)

    def test_late_clear_during_second_episode_is_excluded(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        self._sealed_event(svc, clock, "EV-9")  # 解除观测时刻 T0+60
        # 复发：T0+300 再次占道。
        clock.advance(240)
        rec = svc.ingest_observation(
            obs("EV-9", "lane_closed", "rsu", "r-2", T0 + timedelta(seconds=300))
        )
        self.assertEqual(rec.classification, RECURRENCE)
        # 迟到的、观测时刻为 T0+70 的"解除"（属于片段1时间窗）不能碰片段2。
        stale = svc.ingest_observation(
            obs("EV-9", "lane_open", "vehicle", "v-old", T0 + timedelta(seconds=70))
        )
        self.assertEqual(stale.classification, EXCLUDED)
        self.assertIn("封存", stale.reason)
        self.assertEqual(svc.current_judgment("EV-9").episode, 2)
        self.assertEqual(svc.current_judgment("EV-9").status, JudgmentStatus.ACTIVE)


class RecurrenceVsReplayTests(ServiceTestBase):
    def test_replayed_old_message_after_clear_is_excluded(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        svc.ingest_observation(obs("EV-10", "lane_closed", "rsu", "r-1", T0))
        clock.advance(60)
        svc.ingest_observation(
            obs("EV-10", "cleared", "inspector", "i-1", T0 + timedelta(seconds=60))
        )
        # 间隔仅 30 秒（< 默认复发间隔 60 秒）：判定为旧消息重放。
        clock.advance(120)
        replay = svc.ingest_observation(
            obs("EV-10", "lane_closed", "vehicle", "v-old", T0 + timedelta(seconds=90))
        )
        self.assertEqual(replay.classification, EXCLUDED)
        self.assertIn("重放", replay.reason)
        self.assertIsNone(replay.published)

    def test_recurrence_opens_new_episode_and_publishes(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        svc.ingest_observation(obs("EV-11", "lane_closed", "rsu", "r-1", T0))
        clock.advance(60)
        svc.ingest_observation(
            obs("EV-11", "lane_open", "inspector", "i-1", T0 + timedelta(seconds=60))
        )
        # 5 分钟后同一地点再次占道 → 复发，新片段 episode=2。
        clock.advance(240)
        resp = svc.ingest_observation(
            obs("EV-11", "obstacle", "rsu", "r-2", T0 + timedelta(seconds=300))
        )
        self.assertEqual(resp.classification, RECURRENCE)
        self.assertEqual(resp.published.judgment.episode, 2)
        self.assertEqual(resp.published.judgment.status, JudgmentStatus.ACTIVE)
        # 发布链延续到片段 1 的封存快照。
        clear_pub = svc.publications("EV-11")[-2]
        self.assertTrue(clear_pub.sealed)
        self.assertEqual(resp.published.supersedes, clear_pub.publication_id)


class ReplayHistoryTests(ServiceTestBase):
    def test_replay_at_recomputes_then_view_without_touching_history(self):
        clock = ManualClock(T0)
        svc = self.make_service(clock=clock)
        svc.ingest_observation(
            obs("EV-12", "lane_closed", "vehicle", "v-1", T0, "lane-2", 0.7)
        )
        pubs_at_t0 = list(svc.publications("EV-12"))
        checkpoint = T0 + timedelta(seconds=30)
        clock.set(checkpoint)

        # 事后新增更强证据：车道应为 lane-3。
        clock.set(T0 + timedelta(seconds=120))
        svc.ingest_observation(
            obs("EV-12", "lane_closed", "rsu", "r-1", T0 + timedelta(seconds=120), "lane-3", 0.97)
        )
        svc.ingest_observation(
            obs("EV-12", "obstacle", "inspector", "i-1", T0 + timedelta(seconds=125), "lane-3", 0.9)
        )

        view = svc.replay_at(checkpoint)
        comparison = view.compare("EV-12")
        self.assertEqual(comparison["judgment_then"]["location_code"], "lane-2")
        self.assertEqual(comparison["judgment_now"]["location_code"], "lane-3")
        self.assertIn("location_code", comparison["changed_fields"])
        self.assertTrue(
            any(e["external_id"] == "r-1" for e in comparison["evidence_arrived_after"])
        )
        # 当时的发布快照原样可取，且历史发布列表未被重演改写。
        then_pubs = view.publications_for("EV-12")
        self.assertEqual(len(then_pubs), len(pubs_at_t0))
        self.assertEqual(then_pubs[0]["publication_id"], pubs_at_t0[0].publication_id)
        # 重演不产生任何新发布、不改写历史记录。
        pubs_after_replay = svc.publications("EV-12")
        self.assertEqual(
            [p.publication_id for p in pubs_after_replay][: len(pubs_at_t0)],
            [p.publication_id for p in pubs_at_t0],
        )
        self.assertEqual(comparison["history_unchanged"], True)

    def test_replay_uses_rules_version_effective_at_then(self):
        clock = ManualClock(T0)
        registry = RuleRegistry.default()
        registry.add_version(
            T0 + timedelta(minutes=10),
            FusionRules(version="2.0.0", freshness_half_life_seconds=30.0),
        )
        svc = self.make_service(clock=clock, rules=registry)
        svc.ingest_observation(obs("EV-13", "lane_closed", "rsu", "r-1", T0))
        view = svc.replay_at(T0 + timedelta(minutes=5))
        self.assertEqual(view.compare("EV-13")["rules_version_then"], "1.0.0")
        view2 = svc.replay_at(T0 + timedelta(minutes=15))
        self.assertEqual(view2.compare("EV-13")["rules_version_then"], "2.0.0")

    def test_rebuild_from_log_after_restart_preserves_publications(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            clock = ManualClock(T0)
            svc = FusionService(clock=clock, log_path=path)
            svc.ingest_observation(obs("EV-14", "lane_closed", "rsu", "r-1", T0))
            clock.advance(60)
            svc.ingest_observation(
                obs("EV-14", "lane_open", "inspector", "i-1", T0 + timedelta(seconds=60))
            )
            expected_ids = [p.publication_id for p in svc.publications("EV-14")]

            restarted = FusionService(clock=ManualClock(T0 + timedelta(hours=1)), log_path=path)
            got_ids = [p.publication_id for p in restarted.publications("EV-14")]
            self.assertEqual(got_ids, expected_ids)
            self.assertEqual(restarted.current_judgment("EV-14").status, JudgmentStatus.CLEARED)
            # 重启后重复报文依旧被去重。
            dup = restarted.ingest_observation(obs("EV-14", "lane_closed", "rsu", "r-1", T0))
            self.assertEqual(dup.classification, DUPLICATE)


class ThresholdTests(ServiceTestBase):
    def test_single_weak_vehicle_report_below_threshold_publishes_nothing(self):
        svc = self.make_service()
        # vehicle 能力 0.6 × 可信度 0.4 × 新鲜度 1.0 = 0.24 < 门槛 0.35
        resp = svc.ingest_observation(
            obs("EV-15", "obstacle", "vehicle", "v-1", T0, confidence=0.4)
        )
        self.assertIsNone(resp.published)
        self.assertEqual(resp.judgment.status, JudgmentStatus.UNCERTAIN)


class RulesVersionTests(ServiceTestBase):
    def test_publication_after_rule_upgrade_binds_new_version_and_fingerprint(self):
        clock = ManualClock(T0)
        registry = RuleRegistry.default()
        svc = self.make_service(clock=clock, rules=registry)
        first = svc.ingest_observation(obs("EV-16", "lane_closed", "rsu", "r-1", T0))
        self.assertEqual(first.published.rules_version, "1.0.0")

        v2 = FusionRules(version="2.0.0", publish_score_threshold=0.50)
        registry.add_version(T0 + timedelta(minutes=10), v2)
        clock.advance(600)
        # 人工的新证据带来车道变化，触发一次发布，必须绑定 2.0.0 与新指纹。
        later = svc.ingest_observation(
            obs("EV-16", "obstacle", "inspector", "i-9",
                T0 + timedelta(minutes=10), "lane-1", 0.95)
        )
        self.assertIsNotNone(later.published)
        self.assertEqual(later.published.rules_version, "2.0.0")
        self.assertNotEqual(later.published.rules_fingerprint, first.published.rules_fingerprint)
        # 旧快照中的版本与指纹保持原样。
        self.assertEqual(svc.publications("EV-16")[0].rules_version, "1.0.0")


if __name__ == "__main__":
    unittest.main()
