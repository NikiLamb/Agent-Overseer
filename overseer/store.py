"""SQLite-backed storage for runs, events and alerts.

Design notes:
  * One connection per thread. SQLite connection objects are not safe to share
    across threads, and the server is threaded (one thread per request, plus
    the sweeper), so we keep them in thread-local storage.
  * WAL journalling so that readers (the dashboard, the SSE backfill) never
    block the ingest path, which is the one path that must not be slow.
  * Runs carry rolling aggregates (tokens, cost) updated on write. Recomputing
    them from the event table on every dashboard poll would be the obvious
    first thing to get slow.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  id                TEXT PRIMARY KEY,
  agent             TEXT NOT NULL,
  kind              TEXT NOT NULL DEFAULT 'agent',
  status            TEXT NOT NULL DEFAULT 'running',
  started_at        REAL NOT NULL,
  ended_at          REAL,
  last_seen_at      REAL NOT NULL,
  heartbeat_timeout REAL NOT NULL DEFAULT 120,
  exit_code         INTEGER,
  error             TEXT,
  host              TEXT,
  pid               INTEGER,
  tokens_in         INTEGER NOT NULL DEFAULT 0,
  tokens_out        INTEGER NOT NULL DEFAULT 0,
  cost_usd          REAL    NOT NULL DEFAULT 0,
  meta              TEXT    NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS runs_live    ON runs(status, last_seen_at);
CREATE INDEX IF NOT EXISTS runs_recent  ON runs(started_at DESC);
CREATE INDEX IF NOT EXISTS runs_agent   ON runs(agent, started_at DESC);

CREATE TABLE IF NOT EXISTS events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id      TEXT NOT NULL,
  ts          REAL NOT NULL,
  type        TEXT NOT NULL,
  name        TEXT,
  level       TEXT NOT NULL DEFAULT 'info',
  duration_ms REAL,
  span_id     TEXT,
  parent_id   TEXT,
  tokens_in   INTEGER,
  tokens_out  INTEGER,
  cost_usd    REAL,
  payload     TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS events_run ON events(run_id, id);
CREATE INDEX IF NOT EXISTS events_ts  ON events(ts DESC);

CREATE TABLE IF NOT EXISTS alerts (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id     TEXT,
  rule       TEXT NOT NULL,
  level      TEXT NOT NULL DEFAULT 'warn',
  message    TEXT NOT NULL,
  created_at REAL NOT NULL,
  delivery   TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS alerts_recent ON alerts(created_at DESC);
"""

# Event types that represent real agent activity. A heartbeat proves the
# process is alive but says nothing about progress, so it refreshes
# last_seen_at without cluttering the timeline.
TIMELINE_TYPES = {"run.start", "run.end", "llm", "tool", "log", "step", "stall"}


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    for key in ("meta", "payload", "delivery"):
        if key in d and isinstance(d[key], str):
            try:
                d[key] = json.loads(d[key])
            except json.JSONDecodeError:
                d[key] = {}
    return d


class Store:
    def __init__(self, path: str):
        self.path = path
        self._local = threading.local()
        # Serialises writers ourselves rather than relying on busy_timeout
        # alone; it keeps ingest latency predictable under concurrent agents.
        self._write_lock = threading.Lock()
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            # CREATE TABLE IF NOT EXISTS is idempotent and runs once per
            # thread, so this costs nothing and means a connection opened
            # after the database file was moved or deleted still works.
            conn.executescript(SCHEMA)
            self._local.conn = conn
        return conn

    # ---------------------------------------------------------------- runs

    def start_run(self, run: dict) -> dict:
        now = run.get("started_at") or time.time()
        with self._write_lock, self._conn() as conn:
            conn.execute(
                """INSERT INTO runs (id, agent, kind, status, started_at, last_seen_at,
                                     heartbeat_timeout, host, pid, meta)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET
                     agent=excluded.agent, kind=excluded.kind,
                     last_seen_at=excluded.last_seen_at, meta=excluded.meta""",
                (
                    run["id"],
                    run.get("agent", "unnamed"),
                    run.get("kind", "agent"),
                    "running",
                    now,
                    now,
                    float(run.get("heartbeat_timeout") or 120),
                    run.get("host"),
                    run.get("pid"),
                    json.dumps(run.get("meta") or {}),
                ),
            )
        return self.get_run(run["id"])

    def end_run(self, run_id: str, status: str, *, exit_code=None,
                error=None, ts=None) -> dict | None:
        ts = ts or time.time()
        with self._write_lock, self._conn() as conn:
            cur = conn.execute(
                """UPDATE runs SET status=?, ended_at=?, last_seen_at=?,
                                   exit_code=?, error=?
                   WHERE id=? AND status='running'""",
                (status, ts, ts, exit_code, error, run_id),
            )
            if cur.rowcount == 0:
                return None
        return self.get_run(run_id)

    def touch(self, run_id: str, ts: float) -> None:
        with self._write_lock, self._conn() as conn:
            conn.execute(
                "UPDATE runs SET last_seen_at=MAX(last_seen_at, ?) WHERE id=?",
                (ts, run_id),
            )

    def get_run(self, run_id: str) -> dict | None:
        row = self._conn().execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return _row_to_dict(row) if row else None

    def list_runs(self, *, status=None, agent=None, limit=100, offset=0) -> list[dict]:
        sql = "SELECT * FROM runs"
        where, args = [], []
        if status:
            if status == "live":
                where.append("status IN ('running','stalled')")
            else:
                where.append("status=?")
                args.append(status)
        if agent:
            where.append("agent=?")
            args.append(agent)
        if where:
            sql += " WHERE " + " AND ".join(where)
        # Live runs first, then most recently started.
        sql += """ ORDER BY (status='running') DESC, (status='stalled') DESC,
                            started_at DESC LIMIT ? OFFSET ?"""
        args += [int(limit), int(offset)]
        return [_row_to_dict(r) for r in self._conn().execute(sql, args)]

    # -------------------------------------------------------------- events

    def record_event(self, ev: dict) -> dict:
        ts = ev.get("ts") or time.time()
        tokens_in = ev.get("tokens_in")
        tokens_out = ev.get("tokens_out")
        cost = ev.get("cost_usd")
        with self._write_lock, self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO events (run_id, ts, type, name, level, duration_ms,
                                       span_id, parent_id, tokens_in, tokens_out,
                                       cost_usd, payload)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    ev["run_id"], ts, ev.get("type", "log"), ev.get("name"),
                    ev.get("level", "info"), ev.get("duration_ms"),
                    ev.get("span_id"), ev.get("parent_id"),
                    tokens_in, tokens_out, cost,
                    json.dumps(ev.get("payload") or {}),
                ),
            )
            event_id = cur.lastrowid
            # Fold usage into the run's rolling totals in the same transaction,
            # so a crashed ingest can never leave totals half-applied.
            conn.execute(
                """UPDATE runs SET last_seen_at=MAX(last_seen_at, ?),
                                   tokens_in = tokens_in + ?,
                                   tokens_out = tokens_out + ?,
                                   cost_usd  = cost_usd  + ?
                   WHERE id=?""",
                (ts, tokens_in or 0, tokens_out or 0, cost or 0.0, ev["run_id"]),
            )
        row = self._conn().execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        return _row_to_dict(row)

    def get_events(self, run_id: str, *, after_id=0, limit=1000) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM events WHERE run_id=? AND id>? ORDER BY id LIMIT ?",
            (run_id, int(after_id), int(limit)),
        )
        return [_row_to_dict(r) for r in rows]

    def events_after(self, after_id: int, limit=200) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM events WHERE id>? ORDER BY id LIMIT ?",
            (int(after_id), int(limit)),
        )
        return [_row_to_dict(r) for r in rows]

    def max_event_id(self) -> int:
        row = self._conn().execute("SELECT COALESCE(MAX(id),0) AS m FROM events").fetchone()
        return int(row["m"])

    # -------------------------------------------------------------- stalls

    def find_stalled(self, now: float) -> list[dict]:
        rows = self._conn().execute(
            """SELECT * FROM runs
               WHERE status='running' AND (? - last_seen_at) > heartbeat_timeout""",
            (now,),
        )
        return [_row_to_dict(r) for r in rows]

    # -------------------------------------------------------------- alerts

    def add_alert(self, run_id, rule, message, level="warn", delivery=None) -> dict:
        with self._write_lock, self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO alerts (run_id, rule, level, message, created_at, delivery)
                   VALUES (?,?,?,?,?,?)""",
                (run_id, rule, level, message, time.time(),
                 json.dumps(delivery or [])),
            )
            alert_id = cur.lastrowid
        row = self._conn().execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
        return _row_to_dict(row)

    def list_alerts(self, limit=50) -> list[dict]:
        rows = self._conn().execute(
            "SELECT * FROM alerts ORDER BY id DESC LIMIT ?", (int(limit),)
        )
        return [_row_to_dict(r) for r in rows]

    def mark_stalled(self, run_id: str) -> dict | None:
        """A stalled run is still nominally alive -- we just stopped hearing
        from it. ended_at stays NULL so that a run which recovers and resumes
        reporting is not mistaken for a finished one."""
        with self._write_lock, self._conn() as conn:
            cur = conn.execute(
                "UPDATE runs SET status='stalled' WHERE id=? AND status='running'",
                (run_id,),
            )
            if cur.rowcount == 0:
                return None
        return self.get_run(run_id)

    def revive(self, run_id: str) -> None:
        """Called when a stalled run reports in again."""
        with self._write_lock, self._conn() as conn:
            conn.execute(
                "UPDATE runs SET status='running' WHERE id=? AND status='stalled'",
                (run_id,),
            )

    # --------------------------------------------------------------- stats

    def stats(self, window_hours=24) -> dict:
        conn = self._conn()
        since = time.time() - window_hours * 3600
        by_status = {
            r["status"]: r["n"]
            for r in conn.execute(
                "SELECT status, COUNT(*) AS n FROM runs WHERE started_at>=? GROUP BY status",
                (since,),
            )
        }
        totals = conn.execute(
            """SELECT COALESCE(SUM(tokens_in),0)  AS tin,
                      COALESCE(SUM(tokens_out),0) AS tout,
                      COALESCE(SUM(cost_usd),0)   AS cost
               FROM runs WHERE started_at>=?""",
            (since,),
        ).fetchone()
        live = conn.execute(
            "SELECT COUNT(*) AS n FROM runs WHERE status IN ('running','stalled')"
        ).fetchone()
        return {
            "window_hours": window_hours,
            "by_status": by_status,
            "live": live["n"],
            "tokens_in": totals["tin"],
            "tokens_out": totals["tout"],
            "cost_usd": round(totals["cost"], 6),
        }
