"""Background stall detection.

The single most useful thing this app does. An agent that crashes hard, blocks
on a network call forever, or gets OOM-killed never sends a 'run.end' -- so
without a sweeper the dashboard shows it as happily "running" indefinitely,
which is worse than showing nothing at all.
"""

from __future__ import annotations

import threading
import time


class Sweeper:
    def __init__(self, store, bus, rules, interval=10.0):
        self.store = store
        self.bus = bus
        self.rules = rules
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="overseer-sweeper")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.sweep()
            except Exception as exc:  # never let one bad pass kill the thread
                print(f"[overseer] sweep failed: {exc}", flush=True)

    def sweep(self) -> int:
        now = time.time()
        stalled = 0
        for run in self.store.find_stalled(now):
            updated = self.store.mark_stalled(run["id"])
            if not updated:
                continue
            stalled += 1
            silent_for = now - run["last_seen_at"]
            event = self.store.record_event({
                "run_id": run["id"],
                "ts": now,
                "type": "stall",
                "name": "stalled",
                "level": "error",
                "payload": {"silent_for_seconds": round(silent_for, 1)},
            })
            self.bus.publish("event", event)
            self.bus.publish("run", updated)
            self.rules.on_stall(updated, silent_for)

        # Long-running check applies to anything still alive.
        for run in self.store.list_runs(status="live", limit=500):
            self.rules.check_long_running(run, now)
        return stalled
