"""Alert delivery.

Channels are intentionally dumb: a channel takes an alert dict and tries to
deliver it once. Delivery happens on a worker thread so that a slow or dead
webhook endpoint can never add latency to -- or fail -- an ingest request.
"""

from __future__ import annotations

import json
import queue
import sys
import threading
import urllib.error
import urllib.request

USER_AGENT = "agent-overseer/0.1"


def _fmt(alert: dict) -> str:
    icon = {"error": "[!]", "warn": "[~]", "info": "[i]"}.get(alert["level"], "[-]")
    return f"{icon} {alert['rule']}: {alert['message']}"


class ConsoleChannel:
    name = "console"

    def send(self, alert: dict) -> None:
        print(_fmt(alert), file=sys.stderr, flush=True)


class WebhookChannel:
    """Posts {"text": ...} -- the shape Slack and Discord incoming webhooks
    both accept, so one channel covers the common cases."""

    name = "webhook"

    def __init__(self, url: str, timeout: float = 10.0):
        self.url = url
        self.timeout = timeout

    def send(self, alert: dict) -> None:
        body = json.dumps({
            "text": _fmt(alert),
            "rule": alert["rule"],
            "level": alert["level"],
            "run_id": alert.get("run_id"),
        }).encode()
        req = urllib.request.Request(
            self.url,
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            resp.read()


class Notifier:
    def __init__(self, channels=None):
        self.channels = list(channels or [])
        self._q: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._worker, daemon=True,
                                        name="overseer-notify")
        self._thread.start()

    def dispatch(self, alert: dict) -> None:
        self._q.put(alert)

    def _worker(self) -> None:
        while True:
            alert = self._q.get()
            for channel in self.channels:
                try:
                    channel.send(alert)
                except (urllib.error.URLError, OSError, ValueError) as exc:
                    print(f"[overseer] {channel.name} delivery failed: {exc}",
                          file=sys.stderr, flush=True)


def build_notifier(config) -> Notifier:
    channels: list = [ConsoleChannel()]
    if config.webhook_url:
        channels.append(WebhookChannel(config.webhook_url))
    return Notifier(channels)
