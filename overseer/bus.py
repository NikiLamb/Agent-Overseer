"""In-process fan-out so SSE clients see events the moment they land.

Deliberately not a queue broker. The dashboard is a soft real-time view, not a
system of record -- SQLite is the system of record. If a subscriber is too slow
we drop its oldest messages rather than applying backpressure to ingest, and
the client repairs the gap by replaying from its last event id.
"""

from __future__ import annotations

import queue
import threading

MAX_PENDING = 512


class Bus:
    def __init__(self):
        self._subscribers: set[queue.Queue] = set()
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=MAX_PENDING)
        with self._lock:
            self._subscribers.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            self._subscribers.discard(q)

    def publish(self, kind: str, data: dict) -> None:
        msg = (kind, data)
        with self._lock:
            targets = list(self._subscribers)
        for q in targets:
            try:
                q.put_nowait(msg)
            except queue.Full:
                # Drop the oldest so a stalled browser tab cannot wedge ingest.
                try:
                    q.get_nowait()
                    q.put_nowait(msg)
                except (queue.Empty, queue.Full):
                    pass

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)
