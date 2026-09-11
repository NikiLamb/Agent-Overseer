"""Wrap any command as a tracked run -- for background jobs and scripts that
know nothing about the SDK.

    python -m agent_overseer.wrap --agent nightly-sync -- ./sync.sh --full

stdout becomes info-level log events, stderr becomes error-level ones, and the
exit code decides whether the run lands as ok or failed. Output still goes to
the real stdout/stderr, so existing logging and cron mail are unaffected.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading

from . import Overseer

MAX_LINE = 4000


def _pump(stream, sink, run, level: str) -> None:
    for raw in iter(stream.readline, ""):
        line = raw.rstrip("\n")
        sink.write(raw)
        sink.flush()
        if line.strip():
            run.log(line[:MAX_LINE], level=level)
    stream.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="agent_overseer.wrap",
        description="Run a command and report it to an Agent Overseer server.")
    parser.add_argument("--agent", default=None,
                        help="name to report (default: the command's basename)")
    parser.add_argument("--url", default=os.environ.get("OVERSEER_URL",
                                                        "http://127.0.0.1:8787"))
    parser.add_argument("--token", default=os.environ.get("OVERSEER_TOKEN", ""))
    parser.add_argument("--heartbeat-timeout", type=float, default=300.0,
                        help="seconds of silence before the run is called stalled")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("no command given; use: wrap --agent NAME -- ./script.sh")

    agent = args.agent or os.path.basename(command[0])
    ov = Overseer(agent=agent, url=args.url, token=args.token, kind="job",
                  heartbeat_timeout=args.heartbeat_timeout)
    run = ov.start(meta={"command": " ".join(command), "cwd": os.getcwd()})

    try:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, bufsize=1)
    except (OSError, ValueError) as exc:
        run.error(f"could not launch: {exc}")
        run.finish("failed", error=str(exc))
        print(f"wrap: {exc}", file=sys.stderr)
        return 127

    pumps = [
        threading.Thread(target=_pump, args=(proc.stdout, sys.stdout, run, "info")),
        threading.Thread(target=_pump, args=(proc.stderr, sys.stderr, run, "error")),
    ]
    for t in pumps:
        t.start()
    try:
        code = proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        code = proc.wait()
        run.finish("cancelled", exit_code=code)
        return code
    for t in pumps:
        t.join()

    run.finish("ok" if code == 0 else "failed", exit_code=code,
               error=None if code == 0 else f"exited {code}")
    return code


if __name__ == "__main__":
    sys.exit(main())
