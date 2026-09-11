# Agent Overseer

Watch what your background agents and jobs are actually doing — live, with
alerts when one fails, stalls, or burns past a budget.

No dependencies. The server is Python standard library plus SQLite; so is the
client SDK. That is deliberate: the SDK runs *inside* your agents' processes,
and an observability tool that drags version constraints into the environment
of the thing it observes is one you eventually rip out.

```
your agent ──push──▶ overseer server ──▶ SQLite (system of record)
 (SDK/wrap)           │
                      ├──▶ SSE ──▶ live dashboard
                      └──▶ rules ──▶ console / webhook alerts
```

## Quick start

```sh
python3 -m overseer                 # dashboard on http://127.0.0.1:8787
```

In another terminal:

```sh
python3 examples/demo_agent.py
```

Open the dashboard and watch the run appear, step by step.

## Instrumenting an agent you wrote

```python
from agent_overseer import Overseer

ov = Overseer(agent="research-bot")

with ov.run(meta={"topic": "coastal erosion"}) as run:
    with run.step("search"):                 # times the block, nests what's inside
        results = search(query)
        run.tool("web_search", results=len(results))

    run.llm(model="claude-opus-5", tokens_in=1200, tokens_out=340, cost_usd=0.02)
    run.log("wrote report", path="/tmp/out.md")
```

An exception escaping the `with` marks the run `failed` and records the
traceback — a crashed agent never sits on the dashboard looking healthy.

## Tracking a background job or script

No code changes needed. Wrap it:

```sh
PYTHONPATH=sdk/python python3 -m agent_overseer.wrap --agent nightly-sync -- ./sync.sh
```

stdout becomes log events, stderr becomes error events, the exit code decides
`ok` vs `failed`. Output still flows to the real stdout/stderr, so existing
logging and cron mail are unaffected. In a crontab:

```
0 3 * * * cd /srv/jobs && PYTHONPATH=/opt/overseer/sdk/python \
          python3 -m agent_overseer.wrap --agent nightly-sync -- ./sync.sh
```

## Configuration

All via environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `OVERSEER_HOST` | `127.0.0.1` | Bind address |
| `OVERSEER_PORT` | `8787` | Port |
| `OVERSEER_DB` | `overseer.db` | SQLite file |
| `OVERSEER_TOKEN` | *(unset)* | Shared bearer token; **required** to bind anything but loopback |
| `OVERSEER_WEBHOOK_URL` | *(unset)* | Slack/Discord-compatible incoming webhook for alerts |
| `OVERSEER_COST_BUDGET_USD` | `0` (off) | Alert when one run exceeds this spend |
| `OVERSEER_MAX_RUN_SECONDS` | `0` (off) | Alert when one run runs longer than this |
| `OVERSEER_SWEEP_INTERVAL` | `10` | Seconds between stall checks |

The server refuses to start if you bind a non-loopback address without a token,
rather than quietly exposing your agent history.

## Alerts

Fired once per run per rule, delivered on a worker thread so a slow webhook
never delays ingest:

- `run_failed` — run ended non-`ok`
- `run_stalled` — no event or heartbeat within the run's `heartbeat_timeout`
- `error:<name>` — an error-level event
- `cost_budget` — spend over `OVERSEER_COST_BUDGET_USD`
- `long_running` — wall-clock over `OVERSEER_MAX_RUN_SECONDS`

### Why stall detection matters

An agent that crashes hard, blocks forever on a socket, or gets OOM-killed
never sends `run.end`. Without a sweeper it shows as "running" indefinitely,
which is worse than showing nothing — you'd trust a dashboard that is lying.
The SDK heartbeats every 30s so a genuinely slow step isn't mistaken for a hung
one, and a stalled run flips back to `running` the moment it reports in again.

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/ingest` | `{"events": [...]}` — the only write path |
| `GET` | `/v1/runs` | `?status=live\|ok\|failed\|stalled&agent=&limit=` |
| `GET` | `/v1/runs/{id}` | One run plus its full event timeline |
| `GET` | `/v1/stream` | SSE; `?cursor=<last_event_id>` replays the gap after a reconnect |
| `GET` | `/v1/stats` | 24h rollup |
| `GET` | `/v1/alerts` | Recent alerts |
| `GET` | `/healthz` | Unauthenticated liveness |

Ingest is one uniform event stream. `run.start`, `run.end` and `heartbeat` are
lifecycle signals; every other `type` (`llm`, `tool`, `log`, `step`) is a
timeline entry. Any language that can POST JSON can report in:

```sh
curl -X POST localhost:8787/v1/ingest -H 'Content-Type: application/json' -d '{
  "events": [{"run_id": "abc", "type": "run.start", "agent": "my-bot"}]
}'
```

Bad events in a batch are rejected individually (HTTP 207) — one malformed
event never discards the good ones alongside it.

## Design notes

**Failure isolation.** Every SDK network path is buffered on a daemon thread
and swallows its own errors. If the overseer is down, your agent logs one
warning and keeps working. `flush()` is bounded, so a dead overseer delays a
finishing agent by seconds, never hangs it.

**SQLite is the system of record; SSE is a convenience.** The stream may drop
messages under load (slow subscribers get their oldest dropped rather than
applying backpressure to ingest). Clients repair gaps by replaying from
`?cursor=`.

**Cost is supplied, not inferred.** The server stores whatever `cost_usd` you
pass and never guesses from a built-in price table, which would silently go
stale. Tokens are always recorded regardless.

## Tests

```sh
python3 -m unittest discover -s tests -v
```

26 tests, no test dependencies. They cover the ingest state machine, stall
detection and revival, alert deduplication, auth, path traversal, and two
regressions worth keeping:

- events surviving a process that exits immediately after `run.finish()`
- an internal handler error returning a readable 500 rather than dropping the
  socket

## Limits

Single-node and single-process by design: one SQLite file, in-process fan-out,
one shared token with no per-agent identity or RBAC. That comfortably covers
tens of agents and millions of events on one box. Beyond that the seams to cut
are the `Store` class (→ Postgres) and `Bus` (→ Redis pub/sub); the HTTP
contract would not need to change.

There is no retention policy yet — the events table grows without bound.
