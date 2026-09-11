"""End-to-end tests. No test framework dependencies -- unittest only."""

import json
import os
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from overseer.config import Config          # noqa: E402
from overseer.server import App, build_server, serve   # noqa: E402


def make_app(**overrides):
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    cfg = Config(db_path=tmp.name, token="", webhook_url="", **overrides)
    return App(cfg), tmp.name


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.app, self.db = make_app()

    def tearDown(self):
        os.unlink(self.db)

    def test_run_lifecycle_and_totals(self):
        self.app.ingest([
            {"run_id": "r1", "type": "run.start", "agent": "bot"},
            {"run_id": "r1", "type": "llm", "name": "claude-opus-5",
             "tokens_in": 100, "tokens_out": 20, "cost_usd": 0.5},
            {"run_id": "r1", "type": "llm", "name": "claude-opus-5",
             "tokens_in": 50, "tokens_out": 5, "cost_usd": 0.25},
            {"run_id": "r1", "type": "run.end", "status": "ok"},
        ])
        run = self.app.store.get_run("r1")
        self.assertEqual(run["status"], "ok")
        self.assertEqual(run["tokens_in"], 150)
        self.assertEqual(run["tokens_out"], 25)
        self.assertAlmostEqual(run["cost_usd"], 0.75)
        self.assertIsNotNone(run["ended_at"])

    def test_event_for_unknown_run_is_rejected_not_crashing(self):
        result = self.app.ingest([{"run_id": "ghost", "type": "log"}])
        self.assertEqual(result["accepted"], 0)
        self.assertEqual(result["rejected"], 1)
        self.assertIn("unknown run", result["errors"][0])

    def test_malformed_events_do_not_block_good_ones(self):
        result = self.app.ingest([
            {"type": "run.start"},                       # no run_id
            {"run_id": "r2", "type": "run.start", "agent": "bot"},
            "not an object",
        ])
        self.assertEqual(result["accepted"], 1)
        self.assertEqual(result["rejected"], 2)
        self.assertIsNotNone(self.app.store.get_run("r2"))

    def test_failed_run_raises_alert(self):
        self.app.ingest([
            {"run_id": "r3", "type": "run.start", "agent": "bot"},
            {"run_id": "r3", "type": "run.end", "status": "failed",
             "error": "ValueError: boom"},
        ])
        alerts = self.app.store.list_alerts()
        self.assertTrue(any(a["rule"] == "run_failed" for a in alerts))
        self.assertIn("boom", alerts[0]["message"])

    def test_duplicate_run_end_is_idempotent(self):
        self.app.ingest([{"run_id": "r4", "type": "run.start", "agent": "bot"}])
        self.app.ingest([{"run_id": "r4", "type": "run.end", "status": "ok"}])
        self.app.ingest([{"run_id": "r4", "type": "run.end", "status": "failed"}])
        self.assertEqual(self.app.store.get_run("r4")["status"], "ok")
        self.assertEqual(len(self.app.store.list_alerts()), 0)


class StallTests(unittest.TestCase):
    def setUp(self):
        self.app, self.db = make_app()

    def tearDown(self):
        os.unlink(self.db)

    def test_silent_run_is_marked_stalled_and_alerts(self):
        past = time.time() - 600
        self.app.ingest([{"run_id": "s1", "type": "run.start", "agent": "hung-bot",
                          "ts": past, "heartbeat_timeout": 60}])
        self.assertEqual(self.app.sweeper.sweep(), 1)
        self.assertEqual(self.app.store.get_run("s1")["status"], "stalled")
        self.assertTrue(any(a["rule"] == "run_stalled"
                            for a in self.app.store.list_alerts()))

    def test_stall_alerts_only_once(self):
        past = time.time() - 600
        self.app.ingest([{"run_id": "s2", "type": "run.start", "agent": "hung",
                          "ts": past, "heartbeat_timeout": 60}])
        self.app.sweeper.sweep()
        self.app.sweeper.sweep()
        stalls = [a for a in self.app.store.list_alerts() if a["rule"] == "run_stalled"]
        self.assertEqual(len(stalls), 1)

    def test_heartbeat_prevents_stall(self):
        past = time.time() - 600
        self.app.ingest([{"run_id": "s3", "type": "run.start", "agent": "slow",
                          "ts": past, "heartbeat_timeout": 60}])
        self.app.ingest([{"run_id": "s3", "type": "heartbeat"}])
        self.assertEqual(self.app.sweeper.sweep(), 0)
        self.assertEqual(self.app.store.get_run("s3")["status"], "running")

    def test_stalled_run_revives_when_it_reports_again(self):
        past = time.time() - 600
        self.app.ingest([{"run_id": "s4", "type": "run.start", "agent": "lazy",
                          "ts": past, "heartbeat_timeout": 60}])
        self.app.sweeper.sweep()
        self.assertEqual(self.app.store.get_run("s4")["status"], "stalled")
        self.app.ingest([{"run_id": "s4", "type": "log", "payload": {"message": "back"}}])
        self.assertEqual(self.app.store.get_run("s4")["status"], "running")


class BudgetTests(unittest.TestCase):
    def test_cost_budget_alerts_once(self):
        app, db = make_app(cost_budget_usd=1.0)
        try:
            app.ingest([{"run_id": "b1", "type": "run.start", "agent": "spendy"}])
            for _ in range(4):
                app.ingest([{"run_id": "b1", "type": "llm", "name": "m",
                             "tokens_in": 10, "tokens_out": 1, "cost_usd": 0.6}])
            budget = [a for a in app.store.list_alerts() if a["rule"] == "cost_budget"]
            self.assertEqual(len(budget), 1)
        finally:
            os.unlink(db)


class HttpTests(unittest.TestCase):
    """Exercises the real socket path, including auth."""

    @classmethod
    def setUpClass(cls):
        import threading
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        cls.db = tmp.name
        cls.cfg = Config(db_path=cls.db, host="127.0.0.1", port=8799,
                         token="secret", webhook_url="")
        cls.httpd, cls.app = build_server(cls.cfg)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        for _ in range(50):                       # wait for bind
            try:
                urllib.request.urlopen("http://127.0.0.1:8799/healthz", timeout=0.5).read()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        os.unlink(cls.db)

    def _req(self, path, data=None, token="secret"):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(
            "http://127.0.0.1:8799" + path,
            data=json.dumps(data).encode() if data is not None else None,
            headers=headers, method="POST" if data is not None else "GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())

    def test_healthz_is_unauthenticated(self):
        status, body = self._req("/healthz", token=None)
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])

    def test_ingest_requires_token(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self._req("/v1/ingest", {"events": []}, token=None)
        self.assertEqual(ctx.exception.code, 401)

    def test_wrong_token_rejected(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self._req("/v1/runs", token="guess")
        self.assertEqual(ctx.exception.code, 401)

    def test_full_roundtrip(self):
        status, body = self._req("/v1/ingest", {"events": [
            {"run_id": "http1", "type": "run.start", "agent": "netbot"},
            {"run_id": "http1", "type": "tool", "name": "curl", "duration_ms": 12},
            {"run_id": "http1", "type": "run.end", "status": "ok"},
        ]})
        self.assertEqual(status, 202)
        self.assertEqual(body["accepted"], 3)

        status, body = self._req("/v1/runs/http1")
        self.assertEqual(body["agent"], "netbot")
        self.assertEqual(body["status"], "ok")
        self.assertEqual(len(body["events"]), 3)

    def test_dashboard_is_served(self):
        req = urllib.request.Request("http://127.0.0.1:8799/",
                                     headers={"Authorization": "Bearer secret"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn(b"Agent Overseer", resp.read())

    def test_internal_error_answers_500_instead_of_dropping_the_socket(self):
        """Regression: an exception inside a handler used to escape and close
        the connection with no response, which clients see as an unexplained
        reset rather than an error they can log."""
        original = self.app.store.list_runs

        def explode(**kwargs):
            raise RuntimeError("simulated store failure")

        self.app.store.list_runs = explode
        try:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                self._req("/v1/runs")
            self.assertEqual(ctx.exception.code, 500)
            body = json.loads(ctx.exception.read())
            self.assertIn("simulated store failure", body["error"])
        finally:
            self.app.store.list_runs = original

        # ...and the server is still healthy afterwards.
        self.assertEqual(self._req("/healthz", token=None)[0], 200)

    def test_malformed_query_param_is_a_400(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self._req("/v1/runs?limit=abc")
        self.assertEqual(ctx.exception.code, 400)
        self.assertIn("must be an integer", json.loads(ctx.exception.read())["error"])

    def test_path_traversal_is_blocked(self):
        req = urllib.request.Request("http://127.0.0.1:8799/../../etc/passwd",
                                     headers={"Authorization": "Bearer secret"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                self.assertNotIn(b"root:", resp.read())
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 404)


class ConfigTests(unittest.TestCase):
    def test_refuses_public_bind_without_token(self):
        with self.assertRaises(SystemExit):
            Config(host="0.0.0.0", token="").validate()

    def test_loopback_without_token_is_allowed(self):
        Config(host="127.0.0.1", token="").validate()   # must not raise


if __name__ == "__main__":
    unittest.main(verbosity=2)
