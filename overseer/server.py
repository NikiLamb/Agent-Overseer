"""HTTP surface: ingest, query, live stream, dashboard.

Standard library only (http.server + sqlite3). One thread per connection is
fine here -- an overseer serves a handful of agents and one or two browser
tabs, and the per-connection cost buys us dead-simple blocking SSE.
"""

from __future__ import annotations

import json
import hmac
import mimetypes
import os
import queue
import re
import socket
import threading
import time
import traceback
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .bus import Bus
from .config import Config
from .notify import build_notifier
from .rules import RuleEngine
from .store import Store
from .sweeper import Sweeper

UI_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui")
MAX_BODY = 4 * 1024 * 1024          # 4 MiB per ingest batch
SSE_PING_INTERVAL = 15.0            # seconds between keep-alive comments

# Types the ingest endpoint treats as lifecycle signals rather than plain
# timeline entries.
LIFECYCLE = {"run.start", "run.end", "heartbeat"}


class BadRequest(ValueError):
    """Client sent something malformed -- a 400, not a 500."""


class App:
    """Wiring. Kept separate from the handler so tests can drive it directly."""

    def __init__(self, config: Config):
        self.config = config
        self.store = Store(config.db_path)
        self.bus = Bus()
        self.notifier = build_notifier(config)
        self.rules = RuleEngine(config, self.store, self.notifier)
        self.sweeper = Sweeper(self.store, self.bus, self.rules, config.sweep_interval)

    # ------------------------------------------------------------- ingest

    def ingest(self, events: list[dict]) -> dict:
        accepted, errors = 0, []
        for raw in events:
            try:
                self._ingest_one(raw)
                accepted += 1
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(str(exc))
        return {"accepted": accepted, "rejected": len(errors), "errors": errors[:10]}

    def _ingest_one(self, raw: dict) -> None:
        if not isinstance(raw, dict):
            raise TypeError("event must be an object")
        run_id = raw.get("run_id")
        if not run_id or not isinstance(run_id, str):
            raise ValueError("event is missing a string run_id")
        etype = raw.get("type") or "log"
        ts = float(raw.get("ts") or time.time())

        if etype == "run.start":
            run = self.store.start_run({
                "id": run_id,
                "agent": raw.get("agent") or "unnamed",
                "kind": raw.get("kind") or "agent",
                "started_at": ts,
                "heartbeat_timeout": raw.get("heartbeat_timeout"),
                "host": raw.get("host"),
                "pid": raw.get("pid"),
                "meta": raw.get("meta"),
            })
            self.bus.publish("run", run)
            self._record(raw, run_id, ts, run)
            return

        if etype == "heartbeat":
            self.store.touch(run_id, ts)
            self.store.revive(run_id)
            return

        if etype == "run.end":
            status = raw.get("status")
            if status not in ("ok", "failed", "cancelled"):
                exit_code = raw.get("exit_code")
                status = "ok" if not exit_code else "failed"
            run = self.store.end_run(
                run_id, status,
                exit_code=raw.get("exit_code"),
                error=raw.get("error"),
                ts=ts,
            )
            if run is None:                       # duplicate or unknown end
                run = self.store.get_run(run_id)
                if run is None:
                    raise ValueError(f"run.end for unknown run {run_id}")
            else:
                self._record(raw, run_id, ts, run)
                self.bus.publish("run", run)
                self.rules.on_run_end(run)
            return

        # Ordinary timeline event.
        run = self.store.get_run(run_id)
        if run is None:
            raise ValueError(f"event for unknown run {run_id}; send run.start first")
        if run["status"] == "stalled":
            self.store.revive(run_id)
        event = self._record(raw, run_id, ts, run)
        refreshed = self.store.get_run(run_id)
        self.bus.publish("run", refreshed)
        self.rules.on_event(event, refreshed)

    def _record(self, raw: dict, run_id: str, ts: float, run: dict) -> dict:
        event = self.store.record_event({
            "run_id": run_id,
            "ts": ts,
            "type": raw.get("type") or "log",
            "name": raw.get("name"),
            "level": raw.get("level") or "info",
            "duration_ms": raw.get("duration_ms"),
            "span_id": raw.get("span_id"),
            "parent_id": raw.get("parent_id"),
            "tokens_in": raw.get("tokens_in"),
            "tokens_out": raw.get("tokens_out"),
            "cost_usd": raw.get("cost_usd"),
            "payload": raw.get("payload"),
        })
        self.bus.publish("event", event)
        return event


class Handler(BaseHTTPRequestHandler):
    server_version = "AgentOverseer/0.1"
    protocol_version = "HTTP/1.1"
    app: App = None  # set by serve()

    # ---------------------------------------------------------- plumbing

    def log_message(self, fmt, *args):
        if os.environ.get("OVERSEER_ACCESS_LOG"):
            super().log_message(fmt, *args)

    def _json(self, status: int, payload) -> None:
        body = json.dumps(payload, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self, params: dict) -> bool:
        cfg = self.app.config
        if not cfg.auth_required:
            return True
        header = self.headers.get("Authorization", "")
        supplied = header[7:] if header.startswith("Bearer ") else ""
        if not supplied:
            # EventSource cannot set headers, so the stream endpoint also
            # accepts ?token=. Query strings leak into access logs, which is
            # why it is not the documented default for ingest.
            supplied = (params.get("token") or [""])[0]
        return hmac.compare_digest(supplied, cfg.token)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise ValueError("empty body")
        if length > MAX_BODY:
            raise ValueError(f"body exceeds {MAX_BODY} bytes")
        return json.loads(self.rfile.read(length).decode("utf-8"))

    # ------------------------------------------------------------ routing

    def send_response(self, *args, **kwargs):
        self._response_started = True
        super().send_response(*args, **kwargs)

    def _guard(self, route):
        """An unhandled handler exception would otherwise close the socket
        with no response, which reaches the client as an unexplained
        'connection reset' -- the least debuggable failure there is."""
        self._response_started = False
        try:
            route()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except BadRequest as exc:
            if not self._response_started:
                self._json(400, {"error": str(exc)})
        except Exception as exc:
            traceback.print_exc()
            if not self._response_started:
                try:
                    self._json(500, {"error": f"{type(exc).__name__}: {exc}"})
                except OSError:
                    pass

    def do_POST(self):
        self._guard(self._route_post)

    def do_GET(self):
        self._guard(self._route_get)

    def _route_post(self):
        path, params = self._split()
        if not self._authorized(params):
            return self._json(401, {"error": "unauthorized"})
        if path == "/v1/ingest":
            try:
                payload = self._body()
            except (ValueError, UnicodeDecodeError) as exc:
                return self._json(400, {"error": f"bad request body: {exc}"})
            events = payload.get("events") if isinstance(payload, dict) else payload
            if not isinstance(events, list):
                return self._json(400, {"error": "expected {\"events\": [...]} or a JSON array"})
            result = self.app.ingest(events)
            return self._json(202 if result["rejected"] == 0 else 207, result)
        return self._json(404, {"error": "not found"})

    def _route_get(self):
        path, params = self._split()

        if path == "/healthz":
            return self._json(200, {"ok": True, "subscribers": self.app.bus.subscriber_count})
        if not self._authorized(params):
            return self._json(401, {"error": "unauthorized"})

        if path == "/v1/stream":
            return self._stream(params)
        if path == "/v1/runs":
            runs = self.app.store.list_runs(
                status=(params.get("status") or [None])[0],
                agent=(params.get("agent") or [None])[0],
                limit=self._int_param(params, "limit", 100, maximum=500),
            )
            return self._json(200, {"runs": runs})
        if path == "/v1/stats":
            return self._json(200, self.app.store.stats())
        if path == "/v1/alerts":
            return self._json(200, {"alerts": self.app.store.list_alerts()})

        match = re.fullmatch(r"/v1/runs/([^/]+)", path)
        if match:
            run = self.app.store.get_run(urllib.parse.unquote(match.group(1)))
            if not run:
                return self._json(404, {"error": "no such run"})
            run["events"] = self.app.store.get_events(run["id"])
            return self._json(200, run)

        return self._static(path)

    def _int_param(self, params, name, default, maximum=None):
        raw = (params.get(name) or [default])[0]
        try:
            value = int(raw)
        except (TypeError, ValueError):
            raise BadRequest(f"{name} must be an integer, got {raw!r}") from None
        if value < 0:
            raise BadRequest(f"{name} must not be negative")
        return min(value, maximum) if maximum else value

    def _split(self):
        parsed = urllib.parse.urlparse(self.path)
        return parsed.path.rstrip("/") or "/", urllib.parse.parse_qs(parsed.query)

    # ------------------------------------------------------------ streaming

    def _stream(self, params):
        sub = self.app.bus.subscribe()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")   # defeat proxy buffering
        self.end_headers()
        try:
            # Replay anything the client missed before going live, so a
            # reconnect after a dropped connection leaves no hole.
            cursor = self._int_param(params, "cursor", 0)
            if cursor:
                for event in self.app.store.events_after(cursor):
                    self._sse("event", event, event_id=event["id"])
            self._sse("hello", {"cursor": self.app.store.max_event_id()})

            last_ping = time.time()
            while True:
                try:
                    kind, data = sub.get(timeout=1.0)
                    self._sse(kind, data, event_id=data.get("id") if kind == "event" else None)
                except queue.Empty:
                    if time.time() - last_ping >= SSE_PING_INTERVAL:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        last_ping = time.time()
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            pass                                       # client navigated away
        finally:
            self.app.bus.unsubscribe(sub)

    def _sse(self, kind: str, data, event_id=None) -> None:
        chunk = ""
        if event_id is not None:
            chunk += f"id: {event_id}\n"
        chunk += f"event: {kind}\ndata: {json.dumps(data, default=str)}\n\n"
        self.wfile.write(chunk.encode())
        self.wfile.flush()

    # --------------------------------------------------------------- static

    def _static(self, path: str):
        rel = "index.html" if path == "/" else path.lstrip("/")
        target = os.path.normpath(os.path.join(UI_DIR, rel))
        if not target.startswith(UI_DIR) or not os.path.isfile(target):
            return self._json(404, {"error": "not found"})
        ctype = mimetypes.guess_type(target)[0] or "application/octet-stream"
        with open(target, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_server(config: Config | None = None):
    """Construct the server without running it. Returns (httpd, app) so tests
    and embedders can drive both halves."""
    config = config or Config()
    config.validate()
    app = App(config)
    app.sweeper.start()

    handler = type("BoundHandler", (Handler,), {"app": app})
    try:
        httpd = ThreadingHTTPServer((config.host, config.port), handler)
    except OSError as exc:
        raise SystemExit(
            f"cannot bind {config.host}:{config.port}: {exc}\n"
            "Another overseer may already be running; "
            "stop it or set OVERSEER_PORT to a free port.") from exc
    httpd.daemon_threads = True
    return httpd, app


def serve(config: Config | None = None):
    config = config or Config()
    httpd, app = build_server(config)

    auth = "token required" if config.auth_required else "no auth (loopback only)"
    print(f"[overseer] dashboard  http://{config.host}:{config.port}  ({auth})")
    print(f"[overseer] database   {os.path.abspath(config.db_path)}")
    if config.webhook_url:
        print("[overseer] alerts     console + webhook")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[overseer] shutting down")
    finally:
        app.sweeper.stop()
        httpd.server_close()
    return httpd
