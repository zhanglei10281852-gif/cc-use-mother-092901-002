"""车路云事件可信合成服务的 HTTP API（仅依赖标准库）。

端点：
  POST /reports                 摄入报文（observe/amend/withdraw），默认即时发布
  POST /publish                 {"as_of": "..."} 手动触发一次合成发布
  POST /events/{key}/seal       {"at": "..."} 封存当前片段
  GET  /events/{key}/judgment?as_of=...   查询某时点判断（不落账本）
  GET  /events/{key}/snapshots  事件的历史发布
  GET  /snapshots               全部快照
  GET  /snapshots/{id}          单条快照及其哈希链信息
  POST /replay                  {"as_of": "...", "rules_version"?: "..."} 重演对比
  GET  /journal                 只增日志
  GET  /verify                  校验快照哈希链
"""

import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .contracts import EventObservation, ObservationKind, ReportAction, SourceRef, SourceReport
from .rules import latest_rules
from .serialization import (
    ingest_outcome_to_dict,
    judgment_to_dict,
    replay_to_dict,
    seal_to_dict,
    snapshot_to_dict,
)
from .service import FusionService


def parse_dt(value: str, field_name: str = "时间") -> datetime:
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} 不是合法 ISO-8601 时间：{value!r}") from None
    if dt.tzinfo is None:
        raise ValueError(f"{field_name} 必须携带时区")
    return dt


def parse_report(body: dict) -> SourceReport:
    obs_body = body.get("observation")
    observation = None
    if obs_body is not None:
        src_body = obs_body["source"]
        obs_source = SourceRef(
            source_type=src_body["source_type"],
            external_id=src_body["external_id"],
            observed_at=parse_dt(src_body["observed_at"], "观测时刻"),
        )
        observation = EventObservation(
            event_key=obs_body["event_key"],
            kind=ObservationKind(obs_body["kind"]),
            location_code=obs_body["location_code"],
            source=obs_source,
            confidence=float(obs_body["confidence"]),
        )

    src_body = body.get("source") or (obs_body["source"] if obs_body else None)
    if src_body is None:
        raise ValueError("撤回报文必须提供 source")
    received_at = parse_dt(body["received_at"], "接收时刻")
    source = SourceRef(
        source_type=src_body["source_type"],
        external_id=src_body["external_id"],
        observed_at=parse_dt(src_body["observed_at"], "来源观测时刻"),
    )
    return SourceReport(
        report_id=body["report_id"],
        action=ReportAction(body["action"]),
        source=source,
        received_at=received_at,
        observation=observation,
        target_report_id=body.get("target_report_id"),
    )


def create_handler(service: FusionService):
    class Handler(BaseHTTPRequestHandler):
        server_version = "EventFusion/1.0"

        def log_message(self, fmt, *args):  # 静默访问日志，保留错误即可
            return

        def _send(self, status: int, payload: dict) -> None:
            data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            return json.loads(raw.decode("utf-8"))

        def _error(self, status: int, message: str) -> None:
            self._send(status, {"error": message})

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            query = parse_qs(parsed.query)
            try:
                if parts == ["health"]:
                    self._send(200, {"status": "ok", "rules_version": service.rules_version()})
                elif parts == ["journal"]:
                    self._send(200, {"entries": [
                        {
                            "seq": e.seq,
                            "report_id": e.report.report_id,
                            "action": e.report.action.value,
                            "source_type": e.report.source.source_type,
                            "source_id": e.report.source.external_id,
                            "received_at": e.report.received_at.isoformat(),
                            "duplicate_of": e.duplicate_of,
                            "target_report_id": e.report.target_report_id,
                        }
                        for e in service.journal.entries
                    ]})
                elif parts == ["snapshots"]:
                    event_key = query.get("event_key", [None])[0]
                    snaps = (
                        service.ledger.for_event(event_key)
                        if event_key
                        else service.ledger.all()
                    )
                    self._send(200, {"snapshots": [snapshot_to_dict(s) for s in snaps]})
                elif len(parts) == 2 and parts[0] == "snapshots":
                    snapshot = service.ledger.get(parts[1])
                    if snapshot is None:
                        self._error(404, "快照不存在")
                    else:
                        self._send(200, snapshot_to_dict(snapshot))
                elif len(parts) == 3 and parts[0] == "events" and parts[2] == "judgment":
                    as_of = parse_dt(query["as_of"][0], "as_of")
                    result = service.current_judgment(as_of)
                    judgment = result.judgments.get(parts[1])
                    if judgment is None:
                        self._error(404, "该时点尚无此事件的判断")
                    else:
                        self._send(200, judgment_to_dict(judgment))
                elif len(parts) == 3 and parts[0] == "events" and parts[2] == "snapshots":
                    self._send(
                        200,
                        {"snapshots": [
                            snapshot_to_dict(s) for s in service.ledger.for_event(parts[1])
                        ]},
                    )
                elif parts == ["verify"]:
                    self._send(200, {"chain_valid": service.ledger.verify_chain()})
                elif parts == ["seals"]:
                    self._send(200, {"seals": {
                        key: {episode: seal_to_dict(record)
                              for episode, record in by_episode.items()}
                        for key, by_episode in service.seals().items()
                    }})
                else:
                    self._error(404, "未知路径")
            except (ValueError, KeyError) as exc:
                self._error(400, str(exc))

        def do_POST(self) -> None:
            parts = [p for p in urlparse(self.path).path.split("/") if p]
            try:
                body = self._read_json()
                if parts == ["reports"]:
                    report = parse_report(body)
                    auto_publish = bool(body.get("auto_publish", True))
                    outcome = service.ingest(report, auto_publish=auto_publish)
                    self._send(200, ingest_outcome_to_dict(outcome))
                elif parts == ["publish"]:
                    as_of = parse_dt(body["as_of"], "as_of")
                    emitted = service.publish(as_of)
                    self._send(200, {"published": [snapshot_to_dict(s) for s in emitted]})
                elif len(parts) == 3 and parts[0] == "events" and parts[2] == "seal":
                    sealed_at = parse_dt(body["at"], "封存时刻")
                    snapshot = service.seal_event(parts[1], sealed_at)
                    self._send(200, snapshot_to_dict(snapshot))
                elif parts == ["replay"]:
                    as_of = parse_dt(body["as_of"], "as_of")
                    comparison = service.replay(as_of, rules_version=body.get("rules_version"))
                    self._send(200, replay_to_dict(comparison))
                else:
                    self._error(404, "未知路径")
            except (ValueError, KeyError, json.JSONDecodeError) as exc:
                self._error(400, str(exc))

    return Handler


def make_server(host: str = "127.0.0.1", port: int = 8080, service: FusionService | None = None):
    service = service or FusionService(latest_rules())
    return ThreadingHTTPServer((host, port), create_handler(service)), service


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="车路云事件可信合成服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    server, _ = make_server(args.host, args.port)
    print(f"车路云事件合成服务监听 http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
