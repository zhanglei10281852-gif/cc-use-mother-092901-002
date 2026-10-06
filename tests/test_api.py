"""HTTP API 端到端测试：摄入、封存、快照、重演。"""

import json
import sys
import threading
import unittest
from datetime import timedelta
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from event_fusion.api import create_handler
from event_fusion.service import FusionService

from test_engine import t


def iso(value):
    return value.isoformat()


def obs_body(rid, source_type, source_id, observed_at, kind, location, confidence,
             event="EV-1", received_at=None, action="observe", target=None):
    source = {"source_type": source_type, "external_id": source_id,
              "observed_at": iso(observed_at)}
    body = {
        "report_id": rid,
        "action": action,
        "source": source,
        "received_at": iso(received_at or observed_at + timedelta(seconds=5)),
        "target_report_id": target,
    }
    if kind is not None:
        body["observation"] = {
            "event_key": event,
            "kind": kind,
            "location_code": location,
            "source": source,
            "confidence": confidence,
        }
    return body


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.service = FusionService()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), create_handler(self.service))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _call(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urlrequest.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlrequest.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_full_operator_workflow(self):
        status, body = self._call("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["rules_version"], "fusion-rules-v1")

        # 1) 三来源施工占道上报，车道互相矛盾。
        self.assertEqual(self._call("POST", "/reports", obs_body(
            "r1", "rsu", "rsu-7", t(0), "lane_closed", "lane-3", 0.95))[0], 200)
        self.assertEqual(self._call("POST", "/reports", obs_body(
            "v1", "vehicle", "car-9", t(5), "lane_closed", "lane-2", 0.8,
            received_at=t(8)))[0], 200)
        self.assertEqual(self._call("POST", "/reports", obs_body(
            "i1", "inspector", "patrol-2", t(20), "lane_closed", "lane-3", 0.9,
            received_at=t(25)))[0], 200)

        status, judgment = self._call("GET", f"/events/EV-1/judgment?as_of={quote(iso(t(30)))}")
        self.assertEqual(status, 200)
        self.assertEqual(judgment["location_code"], "lane-3")
        self.assertIn("lane-2", judgment["lane_conflicts"])
        self.assertGreaterEqual(len(judgment["evidence"]), 3)

        # 2) 重复报文：200 但标记 duplicated，不产生新发布。
        status, dup = self._call("POST", "/reports", obs_body(
            "r1", "rsu", "rsu-7", t(0), "lane_closed", "lane-3", 0.95))
        self.assertTrue(dup["duplicated"])
        self.assertEqual(dup["published"], [])

        # 3) 封存。
        status, sealed = self._call("POST", "/events/EV-1/seal", {"at": iso(t(40))})
        self.assertEqual(status, 200)
        self.assertTrue(sealed["sealed"])
        seal_snapshot_id = sealed["snapshot_id"]

        # 4) 封存后迟到的冲突车道观测：不产生快照。
        status, late = self._call("POST", "/reports", obs_body(
            "v2", "vehicle", "car-9", t(2), "lane_closed", "lane-2", 0.99,
            received_at=t(60)))
        self.assertEqual(status, 200)
        self.assertEqual(late["published"], [])

        status, snaps = self._call("GET", "/events/EV-1/snapshots")
        self.assertEqual(snaps["snapshots"][-1]["snapshot_id"], seal_snapshot_id)
        self.assertEqual(snaps["snapshots"][-1]["location_code"], "lane-3")

        # 5) 时点重演：t(3) 时平台尚无判断，事后证据已认定 active/lane-3。
        status, replay = self._call("POST", "/replay", {"as_of": iso(t(3))})
        self.assertEqual(status, 200)
        self.assertEqual(replay["historical"], {})
        self.assertEqual(replay["current"]["EV-1"]["status"], "active")
        self.assertEqual(replay["current"]["EV-1"]["location_code"], "lane-3")
        self.assertIn("EV-1", replay["changes"])
        self.assertTrue(replay["published_snapshots_untouched"])

        # 6) 哈希链校验与日志完整。
        self.assertEqual(self._call("GET", "/verify")[1]["chain_valid"], True)
        journal = self._call("GET", "/journal")[1]
        report_ids = [e["report_id"] for e in journal["entries"]]
        self.assertIn("v2", report_ids)  # 迟到报文仍完整保存
        self.assertTrue(any(e["duplicate_of"] is not None for e in journal["entries"]))

    def test_withdraw_via_api(self):
        self._call("POST", "/reports", obs_body(
            "v1", "vehicle", "car-9", t(0), "obstacle", "lane-1", 0.9))
        status, withdrawn = self._call("POST", "/reports", {
            "report_id": "v1w",
            "action": "withdraw",
            "source": {"source_type": "vehicle", "external_id": "car-9",
                       "observed_at": iso(t(30))},
            "received_at": iso(t(35)),
            "target_report_id": "v1",
        })
        self.assertEqual(status, 200)
        # 撤回触发一次 unknown 撤销发布，下游不会把旧障碍继续留在地图上。
        self.assertEqual(withdrawn["published"][0]["status"], "unknown")
        status, body = self._call("GET", f"/events/EV-1/judgment?as_of={quote(iso(t(40)))}")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "unknown")

    def test_bad_time_is_rejected(self):
        status, body = self._call("POST", "/publish", {"as_of": "21/10/2026 02:00"})
        self.assertEqual(status, 400)
        self.assertIn("ISO-8601", body["error"])

    def test_naive_time_is_rejected(self):
        status, body = self._call("POST", "/publish", {"as_of": "2026-10-21T02:00:00"})
        self.assertEqual(status, 400)
        self.assertIn("时区", body["error"])


if __name__ == "__main__":
    unittest.main()
