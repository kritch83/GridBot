"""Blynk Cloud client -- push realized-PnL values to virtual pins.

Uses stdlib urllib (no extra dependency, matches the existing Pushover code
that uses http.client). Blocking HTTP calls are run via asyncio.to_thread so
the event loop keeps ticking. Failures are logged, never raised -- a Blynk
hiccup must never break trading.
"""
from __future__ import annotations

import asyncio
import logging
import urllib.parse
import urllib.request
from typing import Iterable

from .config import BLYNK_BASE_URL, BLYNK_ENABLED, BLYNK_HEARTBEAT_SEC, BLYNK_TOKEN, BLYNK_TOTAL_PNL_PIN


def _http_get(url: str, timeout: float = 5.0) -> None:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Blynk HTTP {resp.status}: {resp.read()[:200]!r}")


class BlynkClient:
    def __init__(self, token: str = BLYNK_TOKEN, base_url: str = BLYNK_BASE_URL) -> None:
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.enabled = BLYNK_ENABLED and bool(token)

    async def push(self, pin: str, value: float) -> None:
        """Fire-and-forget push to one virtual pin. Logs on failure, never raises."""
        if not self.enabled:
            return
        qs = urllib.parse.urlencode({"token": self.token, pin: value})
        url = f"{self.base_url}/external/api/update?{qs}"
        try:
            await asyncio.to_thread(_http_get, url)
        except Exception as e:
            logging.warning("Blynk push %s=%s failed: %s", pin, value, e)

    async def push_pnl(self, states: dict, coin_cfgs: Iterable[dict]) -> None:
        """Push total realized PnL + each coin's realized PnL in parallel."""
        if not self.enabled:
            return
        total = sum(s.realized_pnl_usd for s in states.values())
        tasks = [self.push(BLYNK_TOTAL_PNL_PIN, round(total, 2))]
        for cfg in coin_cfgs:
            pin = cfg.get("blynk_pin")
            sym = cfg["symbol"]
            if pin and sym in states:
                tasks.append(self.push(pin, round(states[sym].realized_pnl_usd, 2)))
        await asyncio.gather(*tasks, return_exceptions=True)


async def blynk_heartbeat(blynk: BlynkClient, states: dict, coin_cfgs: list) -> None:
    """Push current PnL values every BLYNK_HEARTBEAT_SEC, even with no trades."""
    if not blynk.enabled:
        return
    while True:
        try:
            await asyncio.sleep(BLYNK_HEARTBEAT_SEC)
            await blynk.push_pnl(states, coin_cfgs)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.warning("Blynk heartbeat error: %s", e)
