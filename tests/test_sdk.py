"""SDK tests, focused on the two properties that matter most:
   1. events survive a process that exits immediately after finishing a run;
   2. an unreachable overseer never breaks the agent.
"""

import json
import os
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "sdk" / "python"))

from overseer.config import Config    # noqa: E402
from overseer.server import serve     # noqa: E402

PORT = 8801
BASE = f"http://127.0.0.1:{PORT}"


class LiveServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        cls.db = tmp.name
        cfg = Config(db_path=cls.db, host="127.0.0.1", port=PORT, token="", webhook_url="")
        threading.Thread(target=serve, args=(cfg,), daemon=True).start()
        for _ in range(50):
            try:
                urllib.request.urlopen(BASE + "/healthz", timeout=0.5).read()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.db)

    def get(self, path):
        with urllib.request.urlopen(BASE + path, timeout=5) as r:
            return json.loads(r.read())

    def run_script(self, body, extra_env=None):
        env = {**os.environ,
               "PYTHONPATH": str(ROOT / "sdk" / "python"),
               "OVERSEER_URL": BASE, **(extra_env or {})}
        return subprocess.run([sys.executable, "-c", textwrap.dedent(body)],
                              capture_output=True, text=True, timeout=60, env=env)

    # -------------------------------------------------------------- tests

    def test_events_survive_immediate_process_exit(self):
        """Regression: flush() used to return as soon as the internal queue
        drained, while the worker still held an unsent batch. A process that
        exited right after run.finish() lost every event."""
        proc = self.run_script('''
            from agent_overseer import Overseer
            ov = Overseer(agent="quickbot")
            with ov.run(run_id="fast1") as run:
                run.tool("noop", duration_ms=1)
                run.llm(model="m", tokens_in=7, tokens_out=3, cost_usd=0.01)
        ''')
        self.assertEqual(proc.returncode, 0, proc.stderr)
        run = self.get("/v1/runs/fast1")
        self.assertEqual(run["status"], "ok")
        self.assertEqual(run["tokens_in"], 7)
        self.assertEqual([e["type"] for e in run["events"]],
                         ["run.start", "tool", "llm", "run.end"])

    def test_crash_is_recorded_as_failed_with_traceback(self):
        proc = self.run_script('''
            from agent_overseer import Overseer
            ov = Overseer(agent="crashbot")
            with ov.run(run_id="crash1") as run:
                run.log("about to divide")
                1 / 0
        ''')
        self.assertNotEqual(proc.returncode, 0)
        run = self.get("/v1/runs/crash1")
        self.assertEqual(run["status"], "failed")
        self.assertIn("ZeroDivisionError", run["error"])
        tracebacks = [e for e in run["events"]
                      if "traceback" in (e.get("payload") or {})]
        self.assertTrue(tracebacks, "expected the traceback to be captured")

    def test_nested_steps_record_parent(self):
        self.run_script('''
            from agent_overseer import Overseer
            ov = Overseer(agent="nestbot")
            with ov.run(run_id="nest1") as run:
                with run.step("outer"):
                    run.tool("inner_tool")
        ''')
        events = self.get("/v1/runs/nest1")["events"]
        tool = next(e for e in events if e["type"] == "tool")
        step = next(e for e in events if e["type"] == "step")
        self.assertEqual(tool["parent_id"], step["span_id"])
        self.assertIsNotNone(step["duration_ms"])

    def test_agent_survives_unreachable_overseer(self):
        """The whole point: observability must not take the agent down."""
        proc = self.run_script('''
            from agent_overseer import Overseer
            ov = Overseer(agent="orphan", url="http://127.0.0.1:9")
            with ov.run() as run:
                run.log("still working")
            print("AGENT_COMPLETED")
        ''')
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("AGENT_COMPLETED", proc.stdout)
        self.assertIn("cannot reach", proc.stderr)   # warned once, did not raise

    def test_unreachable_overseer_does_not_hang_the_agent(self):
        started = time.time()
        proc = self.run_script('''
            from agent_overseer import Overseer
            ov = Overseer(agent="orphan2", url="http://127.0.0.1:9")
            with ov.run() as run:
                run.log("x")
        ''')
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertLess(time.time() - started, 30, "flush did not stay bounded")

    def test_wrap_records_failing_job(self):
        script = Path(tempfile.mkdtemp()) / "job.sh"
        script.write_text("#!/bin/sh\necho hello\necho oops >&2\nexit 3\n")
        script.chmod(0o755)
        proc = subprocess.run(
            [sys.executable, "-m", "agent_overseer.wrap", "--agent", "wrapped-job",
             "--", str(script)],
            capture_output=True, text=True, timeout=60,
            env={**os.environ, "PYTHONPATH": str(ROOT / "sdk" / "python"),
                 "OVERSEER_URL": BASE})
        self.assertEqual(proc.returncode, 3)
        self.assertIn("hello", proc.stdout)          # output still passes through
        run = next(r for r in self.get("/v1/runs")["runs"] if r["agent"] == "wrapped-job")
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["exit_code"], 3)
        self.assertEqual(run["kind"], "job")
        events = self.get("/v1/runs/" + run["id"])["events"]
        self.assertTrue(any(e["level"] == "error" and "oops" in str(e["payload"])
                            for e in events), "stderr should land as error events")


if __name__ == "__main__":
    unittest.main(verbosity=2)
