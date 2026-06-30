"""Per-coin trading state dataclasses + load/save helpers.

State is keyed by symbol -- one file per coin, e.g. state_TRX_USD.json. On
first run after the multi-coin refactor, legacy state.json is renamed to
state_<FIRST_COIN>_<QUOTE>.json so existing positions are preserved.

The shared paper-USD pool lives in wallet.json (see wallet.py). The legacy
State.paper_wallet_usd field is kept for backwards-compat deserialization
but is zeroed on load -- USD is owned by PaperWallet going forward.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from .config import COINS, STATE_DIR


@dataclass
class TrailState:
    armed: bool = False
    extreme: Optional[float] = None
    armed_at_price: Optional[float] = None
    # Manually-armed sell-trail (option #3). Trails as a pure trailing stop:
    # the take-profit floor is bypassed so it can arm/fire below avg*(1+tp_pct).
    manual: bool = False


@dataclass
class Position:
    level: int
    qty_coin: float
    entry_price: float
    fee_usd: float
    ts: str


@dataclass
class PendingSellOrder:
    """Outstanding limit sell order waiting for fill (or cancel).

    order_id is None in paper mode -- the simulator checks bid >= limit_price
    on each tick to decide fill. In live mode it's the exchange-assigned id
    we'll fetch_order / cancel_order against.
    """
    order_id: Optional[str] = None
    limit_price: float = 0.0
    qty_coin: float = 0.0
    placed_ts: float = 0.0


@dataclass
class PendingBuyOrder:
    """Outstanding limit buy order waiting for fill (or cancel).

    Mirrors PendingSellOrder. level is the grid level we'll record on fill.
    placed_at_price is the reference price (typically bid) at placement time --
    used by the cancel rule when price moves up away from us.
    """
    order_id: Optional[str] = None
    limit_price: float = 0.0
    qty_coin: float = 0.0
    level: int = 0
    placed_at_price: float = 0.0
    placed_ts: float = 0.0


@dataclass
class State:
    mode: str = "paper"
    positions: list = field(default_factory=list)
    last_buy_price: Optional[float] = None
    total_qty_coin: float = 0.0
    total_cost_usd: float = 0.0
    avg_entry_price: Optional[float] = None
    realized_pnl_usd: float = 0.0
    paper_wallet_usd: float = 0.0     # legacy field; shared wallet owns USD now
    paper_wallet_coin: float = 0.0
    cycle_count: int = 0
    last_action_ts: float = 0.0
    initial_entry_high: Optional[float] = None
    paused: bool = False
    breakeven_exit_armed: bool = False
    pause_after_sell: bool = False
    trailing_buy: TrailState = field(default_factory=TrailState)
    trailing_sell: TrailState = field(default_factory=TrailState)
    pending_sell_order: Optional[PendingSellOrder] = None
    pending_buy_order: Optional[PendingBuyOrder] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "State":
        d = dict(d)
        # Migrate legacy *_btc keys (state files written before the coin-agnostic rename)
        if "total_qty_btc" in d:
            d.setdefault("total_qty_coin", d.pop("total_qty_btc"))
        if "paper_wallet_btc" in d:
            d.setdefault("paper_wallet_coin", d.pop("paper_wallet_btc"))

        positions = []
        for p in d.get("positions", []):
            p = dict(p)
            if "qty_btc" in p:
                p.setdefault("qty_coin", p.pop("qty_btc"))
            positions.append(Position(**p))

        tb = TrailState(**d["trailing_buy"]) if "trailing_buy" in d else TrailState()
        ts = TrailState(**d["trailing_sell"]) if "trailing_sell" in d else TrailState()
        pso_raw = d.get("pending_sell_order")
        pso = PendingSellOrder(**pso_raw) if pso_raw else None
        pbo_raw = d.get("pending_buy_order")
        pbo = PendingBuyOrder(**pbo_raw) if pbo_raw else None
        skip = {"positions", "trailing_buy", "trailing_sell",
                "pending_sell_order", "pending_buy_order"}
        kwargs = {k: v for k, v in d.items() if k not in skip}
        return cls(
            positions=positions, trailing_buy=tb, trailing_sell=ts,
            pending_sell_order=pso, pending_buy_order=pbo, **kwargs,
        )


@dataclass
class PendingAction:
    kind: str
    level: int = -1
    reason: str = ""
    force_market: bool = False   # if True for kind="sell_all", bypass cfg['order_type']


def state_path(symbol: str) -> Path:
    """state file path for a coin: TRX/USD -> state_TRX_USD.json."""
    safe = symbol.replace("/", "_")
    return Path(STATE_DIR) / f"state_{safe}.json"


def _migrate_legacy_if_needed(symbol: str, path: Path) -> None:
    """If this is the first configured coin and a legacy state.json exists,
    rename it to the new per-coin path. Runs once."""
    if path.exists():
        return
    if not COINS or symbol != COINS[0]["symbol"]:
        return
    legacy = Path(STATE_DIR) / "state.json"
    if legacy.exists():
        legacy.rename(path)
        logging.info("Migrated legacy state.json -> %s", path)


def load_state(symbol: str, mode: str = "paper") -> State:
    path = state_path(symbol)
    _migrate_legacy_if_needed(symbol, path)
    if not path.exists():
        return State(mode=mode)
    try:
        return State.from_dict(json.loads(path.read_text()))
    except Exception as e:
        logging.error("Could not parse %s: %s -- starting fresh", path, e)
        return State(mode=mode)


def save_state(symbol: str, state: State) -> None:
    path = state_path(symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_dict(), indent=2, default=str))
