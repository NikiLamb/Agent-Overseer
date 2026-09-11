"""Alert rules.

Each rule is a pure-ish predicate over a single event or run transition. They
run synchronously on the ingest path because they are cheap (no I/O -- the
Notifier owns delivery), which keeps "agent failed" to "alert queued" in the
same millisecond as the write.

Rules are deduplicated per (run_id, rule) so a budget overrun alerts once,
not once per subsequent LLM call.
"""

from __future__ import annotations

import threading
import time


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


class RuleEngine:
    def __init__(self, config, store, notifier):
        self.config = config
        self.store = store
        self.notifier = notifier
        self._fired: set[tuple[str, str]] = set()
        self._lock = threading.Lock()

    def _fire(self, run_id, rule, message, level="warn", once=True) -> None:
        key = (run_id or "", rule)
        if once:
            with self._lock:
                if key in self._fired:
                    return
                self._fired.add(key)
        alert = self.store.add_alert(run_id, rule, message, level=level)
        self.notifier.dispatch(alert)

    def forget(self, run_id: str) -> None:
        with self._lock:
            self._fired = {k for k in self._fired if k[0] != run_id}

    # ------------------------------------------------------------ triggers

    def on_event(self, event: dict, run: dict | None) -> None:
        if event.get("level") == "error":
            name = event.get("name") or event.get("type")
            detail = (event.get("payload") or {}).get("message", "")
            agent = run["agent"] if run else "?"
            self._fire(
                event["run_id"], f"error:{name}",
                f"{agent} logged an error in {name}: {detail}"[:400],
                level="error",
            )
        if run:
            self._check_budget(run)

    def on_run_end(self, run: dict) -> None:
        agent, status = run["agent"], run["status"]
        duration = _fmt_duration((run.get("ended_at") or time.time()) - run["started_at"])
        if status == "failed":
            reason = run.get("error") or f"exit code {run.get('exit_code')}"
            self._fire(run["id"], "run_failed",
                       f"{agent} failed after {duration}: {reason}"[:400],
                       level="error")
        else:
            # A run that finished cleanly cannot stall or overrun later.
            self.forget(run["id"])

    def on_stall(self, run: dict, silent_for: float) -> None:
        self._fire(
            run["id"], "run_stalled",
            f"{run['agent']} has sent nothing for {_fmt_duration(silent_for)} "
            f"(heartbeat timeout {int(run['heartbeat_timeout'])}s) -- assuming it is stuck",
            level="error",
        )

    def _check_budget(self, run: dict) -> None:
        budget = self.config.cost_budget_usd
        if budget > 0 and run.get("cost_usd", 0) > budget:
            self._fire(run["id"], "cost_budget",
                       f"{run['agent']} has spent ${run['cost_usd']:.4f}, "
                       f"over the ${budget:.2f} budget")

    def check_long_running(self, run: dict, now: float) -> None:
        limit = self.config.max_run_seconds
        if limit > 0 and (now - run["started_at"]) > limit:
            self._fire(run["id"], "long_running",
                       f"{run['agent']} has been running for "
                       f"{_fmt_duration(now - run['started_at'])}, over the "
                       f"{_fmt_duration(limit)} limit")
