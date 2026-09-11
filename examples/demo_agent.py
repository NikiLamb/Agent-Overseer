"""A fake agent that exercises every part of the pipeline.

    python3 -m overseer            # terminal 1
    python3 examples/demo_agent.py # terminal 2
"""

import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from agent_overseer import Overseer  # noqa: E402

ov = Overseer(agent="research-bot", heartbeat_timeout=60)


def main():
    with ov.run(meta={"topic": "coastal erosion", "depth": 2}) as run:
        run.log("planning research")

        for i, query in enumerate(["tidal models", "sediment transport", "policy"], 1):
            with run.step(f"research:{query}"):
                run.tool("web_search", duration_ms=random.uniform(120, 400),
                         query=query, results=random.randint(3, 9))
                time.sleep(0.4)
                run.llm(model="claude-opus-5",
                        tokens_in=random.randint(800, 2400),
                        tokens_out=random.randint(200, 900),
                        cost_usd=round(random.uniform(0.01, 0.05), 4),
                        duration_ms=random.uniform(900, 2600))
                if i == 2:
                    run.log("source returned a 503, retrying", level="warn")

        with run.step("write_report"):
            time.sleep(0.3)
            run.tool("write_file", path="/tmp/report.md", bytes=8214)

        run.log("done")


if __name__ == "__main__":
    main()   # run.finish() flushes synchronously; no sleep needed
