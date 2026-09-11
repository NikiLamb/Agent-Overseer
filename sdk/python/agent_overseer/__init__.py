"""Client SDK for Agent Overseer. Standard library only.

The governing rule of this module: *observability must never take down the
thing it observes*. Every network path is buffered, runs on a daemon thread,
and swallows its own errors. If the overseer server is down, misconfigured, or
slow, your agent keeps running and simply goes unreported.

    from agent_overseer import Overseer

    ov = Overseer(agent="research-bot")

    with ov.run(meta={"topic": "tariffs"}) as run:
        with run.step("search"):
            results = search(query)
        run.llm(model="claude-opus-5", tokens_in=1200, tokens_out=340)
        run.log("wrote report", path="/tmp/out.md")
"""

from __future__ import annotations

import atexit
import contextlib
import json
import os
import queue
import socket
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid

__all__ = ["Overseer", "Run"]

DEFAULT_URL = os.environ.get("OVERSEER_URL", "http://127.0.0.1:8787")
POLL_INTERVAL = 0.2       # worker wake-up cadence, seconds
COALESCE_WINDOW = 0.1     # how long to gather a burst into one request
FLUSH_BATCH = 50          # max events per request
HEARTBEAT_INTERVAL = 30.0
QUEUE_LIMIT = 10_000      # bound memory if the server is unreachable for long


class _Transport:
    """Batches events onto a daemon thread and POSTs them to the overseer.

    Events count as *pending* from the moment they are enqueued until a POST
    attempt for them has finished -- not merely until they leave the queue.
    That distinction matters: a short-lived job (the wrap CLI, a cron script)
    calls flush() and exits immediately, and anything still sitting in an
    unsent batch would be lost in that window.
    """

    def __init__(self, url: str, token: str, timeout: float = 5.0, debug: bool = False):
        self.url = url.rstrip("/") + "/v1/ingest"
        self.token = token
        self.timeout = timeout
        self.debug = debug
        self._q: queue.Queue = queue.Queue(maxsize=QUEUE_LIMIT)
        self._pending = 0
        self._cond = threading.Condition()
        self._warned = False
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="overseer-transport")
        self._thread.start()
        atexit.register(self.flush)

    def emit(self, event: dict) -> None:
        with self._cond:
            try:
                self._q.put_nowait(event)
                self._pending += 1
            except queue.Full:
                pass          # drop rather than block the agent

    def flush(self, timeout: float = 5.0) -> None:
        """Block until every emitted event has had a delivery attempt, or the
        timeout expires. Bounded on purpose -- a dead overseer must delay a
        finishing agent by seconds, never hang it."""
        deadline = time.time() + timeout
        with self._cond:
            while self._pending > 0:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self._cond.wait(remaining)

    def _loop(self) -> None:
        while True:
            try:
                batch = [self._q.get(timeout=POLL_INTERVAL)]
            except queue.Empty:
                continue
            # Coalesce a burst into one request, but never hold the first
            # event longer than COALESCE_WINDOW -- the dashboard is live.
            deadline = time.time() + COALESCE_WINDOW
            while len(batch) < FLUSH_BATCH:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                try:
                    batch.append(self._q.get(timeout=remaining))
                except queue.Empty:
                    break
            try:
                self._post(batch)
            finally:
                with self._cond:
                    self._pending -= len(batch)
                    self._cond.notify_all()

    def _post(self, batch: list[dict]) -> None:
        body = json.dumps({"events": batch}).encode()
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(self.url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp.read()
            self._warned = False
        except (urllib.error.URLError, OSError, socket.timeout) as exc:
            # Warn once per outage, then stay quiet. A noisy observability
            # client is one people disable.
            if not self._warned:
                self._warned = True
                print(f"[overseer] cannot reach {self.url}: {exc} "
                      f"(agent continues; events dropped)", file=sys.stderr)
            if self.debug:
                traceback.print_exc()


class Run:
    """A single agent execution. Obtain one via Overseer.run()."""

    def __init__(self, overseer: "Overseer", run_id: str, agent: str):
        self._ov = overseer
        self.id = run_id
        self.agent = agent
        self._span_stack: list[str] = []
        self._closed = False

    # -------------------------------------------------------------- emitters

    def _emit(self, type_: str, **fields) -> None:
        event = {"run_id": self.id, "type": type_, "ts": time.time()}
        if self._span_stack:
            event["parent_id"] = self._span_stack[-1]
        event.update({k: v for k, v in fields.items() if v is not None})
        self._ov._transport.emit(event)

    def log(self, message: str, *, level: str = "info", **payload) -> None:
        self._emit("log", name="log", level=level,
                   payload={"message": message, **payload})

    def error(self, message: str, **payload) -> None:
        self.log(message, level="error", **payload)

    def llm(self, *, model: str, tokens_in: int = 0, tokens_out: int = 0,
            cost_usd: float | None = None, duration_ms: float | None = None,
            **payload) -> None:
        """Record one model call. Pass cost_usd if you track spend; the server
        stores whatever you give it rather than guessing at price tables."""
        self._emit("llm", name=model, tokens_in=tokens_in, tokens_out=tokens_out,
                   cost_usd=cost_usd, duration_ms=duration_ms, payload=payload)

    def tool(self, name: str, *, duration_ms: float | None = None,
             ok: bool = True, **payload) -> None:
        self._emit("tool", name=name, duration_ms=duration_ms,
                   level="info" if ok else "error", payload=payload)

    @contextlib.contextmanager
    def step(self, name: str, **payload):
        """Times a block and nests anything emitted inside it under this span."""
        span_id = uuid.uuid4().hex[:12]
        started = time.perf_counter()
        self._span_stack.append(span_id)
        try:
            yield span_id
        except Exception as exc:
            self._span_stack.pop()
            self._emit("step", name=name, span_id=span_id, level="error",
                       duration_ms=(time.perf_counter() - started) * 1000,
                       payload={**payload, "error": f"{type(exc).__name__}: {exc}"})
            raise
        else:
            self._span_stack.pop()
            self._emit("step", name=name, span_id=span_id,
                       duration_ms=(time.perf_counter() - started) * 1000,
                       payload=payload)

    # ------------------------------------------------------------- lifecycle

    def finish(self, status: str = "ok", *, error: str | None = None,
               exit_code: int | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        self._emit("run.end", status=status, error=error, exit_code=exit_code)
        self._ov._forget(self)
        self._ov._transport.flush()


class Overseer:
    def __init__(self, *, agent: str = "unnamed", url: str = DEFAULT_URL,
                 token: str = "", kind: str = "agent",
                 heartbeat_timeout: float = 120.0, debug: bool = False):
        self.agent = agent
        self.kind = kind
        self.heartbeat_timeout = heartbeat_timeout
        self._transport = _Transport(url, token or os.environ.get("OVERSEER_TOKEN", ""),
                                     debug=debug)
        self._active: dict[str, Run] = {}
        self._lock = threading.Lock()
        self._hb = threading.Thread(target=self._heartbeat_loop, daemon=True,
                                    name="overseer-heartbeat")
        self._hb.start()

    def _heartbeat_loop(self) -> None:
        """Proves liveness between events. Without it, an agent legitimately
        working on one long operation looks identical to a hung one."""
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self._lock:
                run_ids = list(self._active)
            for run_id in run_ids:
                self._transport.emit({"run_id": run_id, "type": "heartbeat",
                                      "ts": time.time()})

    def _forget(self, run: Run) -> None:
        with self._lock:
            self._active.pop(run.id, None)

    def start(self, *, agent: str | None = None, run_id: str | None = None,
              meta: dict | None = None) -> Run:
        """Start a run without a context manager. You must call finish()."""
        run_id = run_id or uuid.uuid4().hex
        agent = agent or self.agent
        run = Run(self, run_id, agent)
        with self._lock:
            self._active[run_id] = run
        self._transport.emit({
            "run_id": run_id, "type": "run.start", "ts": time.time(),
            "agent": agent, "kind": self.kind,
            "heartbeat_timeout": self.heartbeat_timeout,
            "host": socket.gethostname(), "pid": os.getpid(),
            "meta": meta or {},
        })
        return run

    @contextlib.contextmanager
    def run(self, *, agent: str | None = None, run_id: str | None = None,
            meta: dict | None = None):
        """Context manager form. An escaping exception marks the run failed and
        records the traceback, so a crash is never silently a 'still running'."""
        run = self.start(agent=agent, run_id=run_id, meta=meta)
        try:
            yield run
        except BaseException as exc:
            run.error(f"{type(exc).__name__}: {exc}",
                      traceback=traceback.format_exc()[-4000:])
            run.finish("failed", error=f"{type(exc).__name__}: {exc}")
            raise
        else:
            run.finish("ok")
