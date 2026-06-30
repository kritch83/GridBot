"""Shared paper-trading USD wallet across all coins.

One pool of USD that every coin's paper buys/sells draw from. Mirrors how a
real Kraken USD balance works -- a big position in one coin reduces what's
available for others.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from .config import WALLET_FILE


class PaperWallet:
    def __init__(self, usd: float) -> None:
        self._usd: float = float(usd)
        self._lock = asyncio.Lock()

    @property
    def usd(self) -> float:
        return self._usd

    async def spend(self, amount: float) -> None:
        async with self._lock:
            self._usd -= amount
            self._save_locked()

    async def credit(self, amount: float) -> None:
        async with self._lock:
            self._usd += amount
            self._save_locked()

    def _save_locked(self) -> None:
        try:
            path = Path(WALLET_FILE)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"usd": self._usd}, indent=2))
        except Exception as e:
            logging.warning("Could not write %s: %s", WALLET_FILE, e)

    def save(self) -> None:
        self._save_locked()

    @classmethod
    def load_or_init(cls, starting_usd: float, bootstrap_usd: Optional[float] = None) -> "PaperWallet":
        """Load wallet.json if it exists. Otherwise initialize from `bootstrap_usd`
        (preferred -- used to preserve the legacy state.json paper_wallet_usd value
        during migration) or fall back to `starting_usd`."""
        path = Path(WALLET_FILE)
        if path.exists():
            try:
                data = json.loads(path.read_text())
                return cls(float(data.get("usd", starting_usd)))
            except Exception as e:
                logging.warning("Could not parse %s: %s -- starting fresh", path, e)

        initial = bootstrap_usd if (bootstrap_usd is not None and bootstrap_usd > 0) else starting_usd
        w = cls(initial)
        w.save()
        return w
