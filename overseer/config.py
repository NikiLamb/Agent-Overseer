"""Configuration, read once from the environment at startup."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

LOOPBACK = {"127.0.0.1", "::1", "localhost"}


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass
class Config:
    host: str = field(default_factory=lambda: os.environ.get("OVERSEER_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(os.environ.get("OVERSEER_PORT", "8787")))
    db_path: str = field(default_factory=lambda: os.environ.get("OVERSEER_DB", "overseer.db"))
    token: str = field(default_factory=lambda: os.environ.get("OVERSEER_TOKEN", ""))
    webhook_url: str = field(default_factory=lambda: os.environ.get("OVERSEER_WEBHOOK_URL", ""))

    # Alert thresholds. Zero disables the rule.
    cost_budget_usd: float = field(default_factory=lambda: _float("OVERSEER_COST_BUDGET_USD", 0.0))
    max_run_seconds: float = field(default_factory=lambda: _float("OVERSEER_MAX_RUN_SECONDS", 0.0))
    sweep_interval: float = field(default_factory=lambda: _float("OVERSEER_SWEEP_INTERVAL", 10.0))

    def validate(self) -> None:
        """Fail loudly rather than silently exposing an unauthenticated
        control surface on a routable interface."""
        if not self.token and self.host not in LOOPBACK:
            raise SystemExit(
                f"refusing to bind {self.host} without OVERSEER_TOKEN set.\n"
                "Either set OVERSEER_TOKEN=<secret>, or bind to 127.0.0.1 for local use."
            )

    @property
    def auth_required(self) -> bool:
        return bool(self.token)
