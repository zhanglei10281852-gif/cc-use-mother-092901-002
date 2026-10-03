"""车路云事件可信合成服务的 HTTP API（标准库实现，无外部依赖）。

路由：
* POST /v1/events/<key>/observations   上报观测
* POST /v1/events/<key>/revisions      来源修订旧报文
* POST /v1/events/<key>/withdrawals    撤回报文
* GET  /v1/events/<key>/judgment       当前判断与证据明细
* GET  /v1/events/<key>/publications   该事件的历史发布（不可变）
* GET  /v1/snapshots/<publication_id>  按编号取原始发布快照
* GET  /v1/events                      事件清单
* GET  /v1/replay?as_of=...&event_key=...  时点重演与今昔对比
* GET  /health
"""

import json
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .clock import SystemClock
from .contracts import EventObservation, ObservationKind, SourceRef
from .rules import RuleRegistry
from .service import FusionService
from .sources import SourceRegistry


def _parse_instant(value: str) -> datetime:
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None:
        raise ValueError("时刻必须携带时区信息，例如 2026-10-21T02:00:00+00:00")
    return instant


def _build_observation(event_key: str, body: dict) -> EventObservation:
    source = body.get("source") or {}
    missing = [
        name
        for name in ("kind", "location_code", "confidence")
        if body.get(name) is None
    ]
    missing += [
        f"source.{name}"
        for name in ("source_type", "external_id", "observed_at")
        if source.get(name) is None
    ]
    if missing:
        raise ValueError("缺少字段: " + ", ".join(missing))
    return EventObservation(
        event_key=event_key,
        kind=ObservationKind(body["kind"]),
        location_code=str(body["location_code"]),
        source=SourceRef(
            str(source["source_type"]),
            str(source["external_id"]),
            _parse_instant(str(source["observed_at"])),
        ),
        confidence=float(body["confidence"]),
    )


class _Handler(BaseHTTPRequestHandler):
    server_version = "EventFusion/1.0"

    # 由 server 注入
    service: FusionService = None  # type: ignore[assignment]
    lock: threading.Lock = None  # type: ignore[assignment]

    def log_message(self, fmt: str, *args) -> None:  # 安静运行
        return

    def _send_json(self, status: int, payload: dict | list) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _error(self, status: int, message: str) -> None:
        self._send_json(status, {"error": message})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [unquote(p) for p in parsed.path.split("/") if p]
        query = parse_qs(parsed.query)
        try:
            if parts == ["health"]:
                self._send_json(200, {"status": "ok"})
                return
            if parts == ["v1", "events"]:
                self._send_json(200, {"event_keys": self.service.event_keys()})
                return
            if len(parts) == 4 and parts[:2] == ["v1", "events"]:
                event_key = parts[2]
                if parts[3] == "judgment":
                    self._get_judgment(event_key)
                    return
                if parts[3] == "publications":
                    self._get_publications(event_key)
                    return
            if len(parts) == 3 and parts[:2] == ["v1", "snapshots"]:
                self._get_snapshot(parts[2])
                return
            if parts == ["v1", "replay"]:
                self._replay(query)
                return
            self._error(404, "未知路径")
        except (ValueError, KeyError) as exc:
            self._error(400, str(exc))

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        parts = [unquote(p) for p in parsed.path.split("/") if p]
        try:
            body = self._read_body()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._error(400, f"请求体不是合法 JSON: {exc}")
            return
        try:
            if len(parts) == 4 and parts[:2] == ["v1", "events"]:
                event_key = parts[2]
                with self.lock:
                    if parts[3] == "observations":
                        obs = _build_observation(event_key, body)
                        received_at = (
                            _parse_instant(str(body["received_at"]))
                            if body.get("received_at")
                            else None
                        )
                        response = self.service.ingest_observation(obs, received_at=received_at)
                        self._send_json(200, response.to_dict())
                        return
                    if parts[3] == "withdrawals":
                        response = self.service.withdraw(
                            event_key,
                            str(body["source_type"]),
                            str(body["external_id"]),
                            note=str(body.get("note", "")),
                            received_at=(
                                _parse_instant(str(body["received_at"]))
                                if body.get("received_at")
                                else None
                            ),
                        )
                        self._send_json(
                            200,
                            response.to_dict()
                            if response
                            else {"classification": "withdrawal_pending",
                                  "reason": "观测尚未到达，撤回已登记，将在其到达时生效"},
                        )
                        return
                    if parts[3] == "revisions":
                        source = body.get("source") or {}
                        response = self.service.revise_observation(
                            event_key=event_key,
                            source_type=str(source["source_type"]),
                            external_id=str(source["external_id"]),
                            new_kind=ObservationKind(body["new_kind"]),
                            new_location_code=str(body["new_location_code"]),
                            new_confidence=float(body["new_confidence"]),
                            new_observed_at=(
                                _parse_instant(source["new_observed_at"])
                                if source.get("new_observed_at")
                                else None
                            ),
                            received_at=(
                                _parse_instant(str(body["received_at"]))
                                if body.get("received_at")
                                else None
                            ),
                        )
                        self._send_json(200, response.to_dict())
                        return
            self._error(404, "未知路径")
        except (ValueError, KeyError) as exc:
            self._error(400, str(exc))

    # --------------------------------------------------------------- GET 实现

    def _get_judgment(self, event_key: str) -> None:
        judgment = self.service.current_judgment(event_key)
        if judgment is None:
            self._error(404, "该事件尚无判断")
            return
        pubs = self.service.publications(event_key)
        self._send_json(
            200,
            {
                "judgment": judgment.to_dict(),
                "evidence": [item.to_dict() for item in self.service.evidence(event_key)],
                "latest_publication_id": pubs[-1].publication_id if pubs else None,
            },
        )

    def _get_publications(self, event_key: str) -> None:
        snapshots = self.service.publications(event_key)
        self._send_json(200, {"event_key": event_key, "snapshots": [s.to_dict() for s in snapshots]})

    def _get_snapshot(self, publication_id: str) -> None:
        for snapshot in self.service.publications():
            if snapshot.publication_id == publication_id:
                self._send_json(200, snapshot.to_dict())
                return
        self._error(404, "发布快照不存在（历史发布不可变，编号不会复用）")

    def _replay(self, query: dict) -> None:
        if "as_of" not in query or "event_key" not in query:
            raise ValueError("必须提供 as_of 与 event_key 查询参数")
        as_of = _parse_instant(query["as_of"][0])
        event_key = query["event_key"][0]
        # 重演为只读操作，但与写入共用锁，避免看到半个状态。
        with self.lock:
            view = self.service.replay_at(as_of)
            self._send_json(200, view.compare(event_key))


def create_server(
    host: str = "127.0.0.1",
    port: int = 8080,
    log_path: str | None = None,
    rules: RuleRegistry | None = None,
) -> ThreadingHTTPServer:
    service = FusionService(
        clock=SystemClock(),
        log_path=log_path,
        rules=rules,
        sources=SourceRegistry.default(),
    )
    lock = threading.Lock()
    handler = type("_BoundHandler", (_Handler,), {"service": service, "lock": lock})
    server = ThreadingHTTPServer((host, port), handler)
    server.service = service  # type: ignore[attr-defined]
    return server


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="车路云事件可信合成服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--log", default=None, help="JSONL 事件日志路径，提供后可跨重启重建")
    args = parser.parse_args()

    server = create_server(args.host, args.port, args.log)
    print(f"车路云事件可信合成服务监听 http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
