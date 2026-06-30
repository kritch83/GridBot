"""Per-coin trading task.

Each enabled coin gets one `run_coin` task in the asyncio event loop. The
task subscribes to its own ticker feed, runs the trail state machines on
every tick, executes buys/sells against either the paper wallet or live
exchange, persists state, and drains a per-coin manual-command queue.

Trail handlers and order execution live here (rather than conductor.py) so
they can take a `coin_cfg` dict instead of importing single-coin globals.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import time
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

import ccxt

from .blynk import BlynkClient
from .config import ACTION_COOLDOWN_SEC, COINS, LIVE_BUY_FILL_TIMEOUT_SEC, MAKER_FEE_PCT, TAKER_FEE_PCT
from .state import PendingAction, PendingBuyOrder, PendingSellOrder, Position, State, TrailState, save_state
from .wallet import PaperWallet


class CoinAdapter(logging.LoggerAdapter):
    """Prefix every log line with [SYMBOL]."""
    def process(self, msg, kwargs):
        return f"[{self.extra['symbol']}] {msg}", kwargs


# --- Trail state machines (coin-aware) --------------------------------------

def handle_initial_entry_trail(state: State, price: float, cfg: dict, log: logging.LoggerAdapter) -> Optional[PendingAction]:
    """Trail for the very first entry of a fresh cycle.

    Tracks a running high from when the bot starts watching. Arms a trail when
    price drops drop_pct below that high, then fires on a trail_buy_pct rebound
    off the local low -- same shape as the regular grid trail. Prevents buying
    blindly at the top on the first tick.
    """
    if state.positions:
        return None

    drop_pct       = cfg["drop_pct"]
    trail_buy_pct  = cfg["trail_buy_pct"]
    pp             = cfg["price_prec"]

    if state.initial_entry_high is None or price > state.initial_entry_high:
        state.initial_entry_high = price

    if not state.trailing_buy.armed:
        if price <= state.initial_entry_high * (1 - drop_pct):
            if trail_buy_pct == 0:
                return PendingAction(
                    "buy", 0,
                    f"initial entry: -{drop_pct:.1%} from observed high ${state.initial_entry_high:.{pp}f}",
                )
            state.trailing_buy = TrailState(armed=True, extreme=price, armed_at_price=price)
            d = (state.initial_entry_high - price) / state.initial_entry_high * 100
            log.info(
                f"[{state.mode.upper()}] INITIAL-ENTRY TRAIL ARMED price=${price:.{pp}f} "
                f"(high ${state.initial_entry_high:.{pp}f}, -{d:.1f}%)"
            )
        return None

    if price < state.trailing_buy.extreme:
        state.trailing_buy.extreme = price
        log.info(f"[{state.mode.upper()}] INITIAL-ENTRY TRAIL low=${price:.{pp}f}")
        return None

    fire_threshold = state.trailing_buy.extreme * (1 + trail_buy_pct)
    if price >= fire_threshold:
        action = PendingAction(
            "buy", 0,
            f"initial entry: trail-back +{trail_buy_pct:.1%} off low ${state.trailing_buy.extreme:.{pp}f}",
        )
        state.trailing_buy = TrailState()
        return action

    if state.trailing_buy.armed_at_price is not None and price >= state.trailing_buy.armed_at_price:
        log.info(
            f"[{state.mode.upper()}] INITIAL-ENTRY TRAIL DISARMED "
            f"price=${price:.{pp}f} back above arm ${state.trailing_buy.armed_at_price:.{pp}f}"
        )
        state.trailing_buy = TrailState()

    return None


def handle_buy_trail(state: State, price: float, cfg: dict, log: logging.LoggerAdapter) -> Optional[PendingAction]:
    drop_pct        = cfg["drop_pct"]
    trail_buy_pct   = cfg["trail_buy_pct"]
    max_grid_levels = cfg["max_grid_levels"]
    pp              = cfg["price_prec"]

    if state.last_buy_price is None or len(state.positions) >= max_grid_levels:
        if state.trailing_buy.armed:
            state.trailing_buy = TrailState()
        return None

    trigger_price = state.last_buy_price * (1 - drop_pct)
    next_level = len(state.positions)

    if not state.trailing_buy.armed:
        if price <= trigger_price:
            if trail_buy_pct == 0:
                return PendingAction(
                    "buy", next_level,
                    f"-{drop_pct:.1%} from last buy ${state.last_buy_price:.{pp}f}",
                )
            state.trailing_buy = TrailState(armed=True, extreme=price, armed_at_price=price)
            log.info(
                f"[{state.mode.upper()}] BUY-TRAIL ARMED level={next_level} "
                f"price=${price:.{pp}f} trigger=${trigger_price:.{pp}f}"
            )
        return None

    # Already armed -- track the low and watch for the rebound.
    if price < state.trailing_buy.extreme:
        state.trailing_buy.extreme = price
        log.info(f"[{state.mode.upper()}] BUY-TRAIL low=${price:.{pp}f}")
        return None

    fire_threshold = state.trailing_buy.extreme * (1 + trail_buy_pct)
    if price >= fire_threshold:
        action = PendingAction(
            "buy", next_level,
            f"trail-back +{trail_buy_pct:.1%} off low ${state.trailing_buy.extreme:.{pp}f}",
        )
        state.trailing_buy = TrailState()
        return action

    if state.trailing_buy.armed_at_price is not None and price >= state.trailing_buy.armed_at_price:
        log.info(
            f"[{state.mode.upper()}] BUY-TRAIL DISARMED price=${price:.{pp}f} "
            f"back above arm ${state.trailing_buy.armed_at_price:.{pp}f}"
        )
        state.trailing_buy = TrailState()

    return None


def sell_threshold(state: State, cfg: dict) -> tuple[float, str, str]:
    """Compute the sell-trail arm threshold and a human label for it.

    Returns (threshold, source_label, tag). `tag` is the trail-log prefix
    ("SELL-TRAIL" or "BREAKEVEN-EXIT SELL-TRAIL").

    When `state.breakeven_exit_armed`, the threshold is the avg entry
    grossed up by the sell-side fee rate so net proceeds >= total cost:
    p = avg / (1 - fee_rate). Otherwise it's avg * (1 + take_profit_pct).
    """
    avg = state.avg_entry_price
    if state.breakeven_exit_armed:
        is_limit = cfg.get("order_type", "market") == "limit"
        sell_fee_rate = MAKER_FEE_PCT if is_limit else TAKER_FEE_PCT
        fee_label = "maker fee" if is_limit else "taker fee"
        threshold = avg / (1 - sell_fee_rate)
        return threshold, f"breakeven (avg + {fee_label} {sell_fee_rate:.2%})", "BREAKEVEN-EXIT SELL-TRAIL"
    take_profit_pct = cfg["take_profit_pct"]
    return avg * (1 + take_profit_pct), f"+{take_profit_pct:.1%} from avg", "SELL-TRAIL"


def handle_sell_trail(state: State, price: float, cfg: dict, log: logging.LoggerAdapter) -> Optional[PendingAction]:
    trail_sell_pct  = cfg["trail_sell_pct"]
    pp              = cfg["price_prec"]

    if not state.positions or state.avg_entry_price is None:
        return None

    tp_threshold, source_label, tag = sell_threshold(state, cfg)

    if not state.trailing_sell.armed:
        if price >= tp_threshold:
            if trail_sell_pct == 0:
                return PendingAction(
                    "sell_all",
                    reason=f"{source_label} hit ${tp_threshold:.{pp}f} (avg ${state.avg_entry_price:.{pp}f})",
                )
            state.trailing_sell = TrailState(armed=True, extreme=price, armed_at_price=price)
            log.info(
                f"[{state.mode.upper()}] {tag} ARMED price=${price:.{pp}f} "
                f"threshold=${tp_threshold:.{pp}f} avg=${state.avg_entry_price:.{pp}f}"
            )
        return None

    if price > state.trailing_sell.extreme:
        state.trailing_sell.extreme = price
        log.info(f"[{state.mode.upper()}] {tag} high=${price:.{pp}f}")
        return None

    # A manually-armed trail (option #3) is a trailing stop clamped to avg entry:
    # it bypasses the take-profit floor (so it can arm/trail below avg*(1+tp_pct)),
    # but it never sells below avg entry -- a pull-back that would realize a loss
    # is held (keep trailing for a profitable exit) instead of firing or disarming.
    # An auto trail uses the take-profit threshold as its floor and disarms (to
    # re-arm cleanly later) whenever price slips back below it.
    manual = state.trailing_sell.manual
    floor = state.avg_entry_price if manual else tp_threshold

    fire_threshold = state.trailing_sell.extreme * (1 - trail_sell_pct)
    if price <= fire_threshold:
        if price >= floor:
            action = PendingAction(
                "sell_all",
                reason=(
                    f"trail-back -{trail_sell_pct:.1%} off high "
                    f"${state.trailing_sell.extreme:.{pp}f}" + (" (manual stop)" if manual else "")
                ),
            )
            state.trailing_sell = TrailState()
            return action
        if manual:
            # Pull-back would lock in a loss -- stay armed and keep trailing for a
            # profitable exit rather than selling below avg entry.
            return None
        log.info(
            f"[{state.mode.upper()}] {tag} DISARMED at ${price:.{pp}f} "
            f"(below ${floor:.{pp}f} -- {source_label})"
        )
        state.trailing_sell = TrailState()
        return None

    if not manual and price < tp_threshold:
        log.info(
            f"[{state.mode.upper()}] {tag} DISARMED price=${price:.{pp}f} "
            f"below ${tp_threshold:.{pp}f} ({source_label})"
        )
        state.trailing_sell = TrailState()

    return None


# --- Fill helpers -----------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def paper_buy(wallet: PaperWallet, usd: float, state: State, ticker: dict) -> dict:
    fill_price = ticker.get("ask") or ticker.get("last")
    if fill_price is None:
        raise RuntimeError("paper_buy: no ask/last on ticker")
    fee_usd = usd * TAKER_FEE_PCT
    qty_coin = (usd - fee_usd) / fill_price
    await wallet.spend(usd)
    state.paper_wallet_coin += qty_coin
    return {"qty": qty_coin, "price": fill_price, "fee_usd": fee_usd, "ts": _now_iso()}


async def paper_sell_all(wallet: PaperWallet, qty_coin: float, state: State, ticker: dict) -> dict:
    fill_price = ticker.get("bid") or ticker.get("last")
    if fill_price is None:
        raise RuntimeError("paper_sell_all: no bid/last on ticker")
    gross = qty_coin * fill_price
    fee_usd = gross * TAKER_FEE_PCT
    net = gross - fee_usd
    await wallet.credit(net)
    state.paper_wallet_coin -= qty_coin
    return {"qty": qty_coin, "price": fill_price, "fee_usd": fee_usd, "ts": _now_iso(), "proceeds_net": net}


_TERMINAL_ORDER_STATUSES = frozenset({"closed", "canceled", "cancelled", "rejected", "expired"})


async def live_buy(exchange, symbol: str, usd: float, ticker: dict) -> dict:
    """Place a market buy sized in USD; wait for the fill to settle.

    Kraken's REST AddOrder returns synchronously with the txid but no fill
    details -- `filled`/`average`/`status` are None until QueryOrders catches
    up. We poll fetch_order(id) until the order reaches a terminal status
    (closed/canceled/etc) or LIVE_BUY_FILL_TIMEOUT_SEC elapses. On timeout we
    cancel and return qty=0 so we don't leave a silent fill on the exchange.
    """
    if exchange.has.get("createMarketBuyOrderWithCostWs"):
        order = await exchange.create_market_buy_order_with_cost_ws(symbol, usd)
    else:
        order = await exchange.create_market_buy_order_with_cost(symbol, usd)

    order_id = order.get("id")
    status = (order.get("status") or "").lower()

    if not order_id:
        logging.error(
            "live_buy %s placed order has no id -- cannot poll for fill. raw: %r",
            symbol, order,
        )
    elif status not in _TERMINAL_ORDER_STATUSES:
        # Poll fetch_order until terminal or timeout
        deadline = time.monotonic() + LIVE_BUY_FILL_TIMEOUT_SEC
        backoff = 0.25
        while time.monotonic() < deadline:
            await asyncio.sleep(backoff)
            try:
                order = await exchange.fetch_order(order_id, symbol)
            except ccxt.OrderNotFound:
                logging.warning(
                    "live_buy %s order %s vanished from exchange mid-poll",
                    symbol, order_id,
                )
                break
            except ccxt.NetworkError as e:
                logging.warning(
                    "live_buy %s fetch_order network error: %s -- retrying",
                    symbol, e,
                )
                backoff = min(backoff * 2, 2.0)
                continue
            status = (order.get("status") or "").lower()
            if status in _TERMINAL_ORDER_STATUSES:
                break
            backoff = min(backoff * 1.5, 1.0)
        else:
            # Timeout: cancel the still-open order so it can't surprise-fill
            logging.error(
                "live_buy %s timed out after %.1fs waiting for fill -- cancelling order %s",
                symbol, LIVE_BUY_FILL_TIMEOUT_SEC, order_id,
            )
            await live_cancel_order(exchange, symbol, order_id)
            return {"qty": 0.0, "price": float(ticker.get("ask") or ticker.get("last") or 0.0),
                    "fee_usd": 0.0, "ts": _now_iso()}

    qty = float(order.get("filled") or order.get("amount") or 0.0)
    if qty <= 0:
        logging.warning(
            "live_buy %s returned zero qty (ws=%s, status=%s) -- raw order: %r",
            symbol, exchange.has.get("createMarketBuyOrderWithCostWs"), status, order,
        )
    price = float(order.get("average") or order.get("price") or ticker.get("ask") or ticker.get("last"))
    fee_usd = float((order.get("fee") or {}).get("cost") or 0.0)
    return {"qty": qty, "price": price, "fee_usd": fee_usd, "ts": _now_iso()}


async def live_sell_all(exchange, symbol: str, qty_coin: float, ticker: dict) -> dict:
    qty_str = exchange.amount_to_precision(symbol, qty_coin)
    if exchange.has.get("createMarketSellOrderWs"):
        order = await exchange.create_market_sell_order_ws(symbol, float(qty_str))
    else:
        order = await exchange.create_market_sell_order(symbol, float(qty_str))
    price = float(order.get("average") or order.get("price") or ticker.get("bid") or ticker.get("last"))
    fee_usd = float((order.get("fee") or {}).get("cost") or 0.0)
    proceeds_net = float(order.get("cost") or qty_coin * price) - fee_usd
    return {"qty": qty_coin, "price": price, "fee_usd": fee_usd, "ts": _now_iso(), "proceeds_net": proceeds_net}


async def live_limit_sell(exchange, symbol: str, qty_coin: float, limit_price: float) -> dict:
    """Place a post-only limit sell. Returns the resting order id + sanitized qty/price."""
    qty_str = exchange.amount_to_precision(symbol, qty_coin)
    price_str = exchange.price_to_precision(symbol, limit_price)
    order = await exchange.create_limit_sell_order(
        symbol, float(qty_str), float(price_str),
        params={"postOnly": True},
    )
    return {
        "order_id":    order["id"],
        "limit_price": float(price_str),
        "qty":         float(qty_str),
    }


async def live_limit_buy(exchange, symbol: str, qty_coin: float, limit_price: float) -> dict:
    """Place a post-only limit buy. Returns the resting order id + sanitized qty/price."""
    qty_str = exchange.amount_to_precision(symbol, qty_coin)
    price_str = exchange.price_to_precision(symbol, limit_price)
    order = await exchange.create_limit_buy_order(
        symbol, float(qty_str), float(price_str),
        params={"postOnly": True},
    )
    return {
        "order_id":    order["id"],
        "limit_price": float(price_str),
        "qty":         float(qty_str),
    }


async def live_cancel_order(exchange, symbol: str, order_id: str) -> None:
    """Best-effort cancel -- swallow OrderNotFound (already filled or gone)."""
    try:
        await exchange.cancel_order(order_id, symbol)
    except ccxt.OrderNotFound:
        pass


async def live_sellable_qty(exchange, symbol: str, tracked_qty: float,
                            log: logging.LoggerAdapter) -> float:
    """Return the qty we can actually sell right now: min(tracked, free balance),
    truncated DOWN to lot precision.

    Kraken deducts trading fees from the received base coin, so our tracked
    position (sum of gross buy fills) can exceed the actual free balance by a
    hair. Requesting the full tracked qty then fails with EOrder:Insufficient
    funds on both the limit-sell and the market fallback, stranding the position.
    Clamping to the real free balance (rounded down) lets it exit cleanly; the
    leftover dust is just the fee Kraken already took.
    """
    base = symbol.split("/")[0]
    try:
        bal = await exchange.fetch_balance()
        free = (bal.get(base) or {}).get("free")
    except Exception as e:
        log.warning("Balance check failed (%s) -- selling tracked qty %.8f", e, tracked_qty)
        return tracked_qty
    if free is None:
        return tracked_qty
    target = min(tracked_qty, float(free))
    # Truncate DOWN to the market's amount precision so we never over-request.
    try:
        safe = float(exchange.decimal_to_precision(
            target, ccxt.TRUNCATE,
            exchange.markets[symbol]["precision"]["amount"],
            exchange.precisionMode, exchange.paddingMode,
        ))
    except Exception:
        safe = target
    if safe < tracked_qty:
        log.info(
            "Sell qty clamped to free balance: tracked=%.8f free=%.8f -> selling %.8f %s",
            tracked_qty, float(free), safe, base,
        )
    return safe


# --- Execute (paper-vs-live swap site) --------------------------------------

def _finalize_buy(state: State, fill: dict, level: int, cfg: dict, reason: str,
                  tag: str, log: logging.LoggerAdapter) -> None:
    """Post-fill state cleanup shared by market buys and limit-buy fills.

    `fill` must carry: qty, price, fee_usd, ts.
    `tag` is a short log label ("BUY" for market, "LIMIT-BUY FILL" for limit).
    """
    pp = cfg["price_prec"]

    if fill["qty"] <= 0:
        # Exchange reported a zero-qty fill -- skip recording the position to
        # avoid corrupting totals (a 0/0 in avg_entry_price below) and getting
        # stuck on a phantom position next run. Clear pending/trail so the
        # coin can re-arm on the next tick.
        log.warning(
            f"[{state.mode.upper()}] {tag} skipped -- exchange reported zero qty "
            f"filled (price=${fill['price']:.{pp}f}, reason={reason})"
        )
        state.pending_buy_order = None
        state.trailing_buy = TrailState()
        return

    cost_usd = fill["qty"] * fill["price"] + fill["fee_usd"]
    state.positions.append(Position(
        level=level, qty_coin=fill["qty"], entry_price=fill["price"],
        fee_usd=fill["fee_usd"], ts=fill["ts"],
    ))
    state.total_qty_coin += fill["qty"]
    state.total_cost_usd += cost_usd
    state.avg_entry_price = state.total_cost_usd / state.total_qty_coin
    state.last_buy_price = fill["price"]
    state.last_action_ts = time.time()
    state.pending_buy_order = None
    state.trailing_buy = TrailState()
    log.info(
        f"[{state.mode.upper()}] {tag} level={level} "
        f"price=${fill['price']:.{pp}f} qty={fill['qty']:.8f} "
        f"fee=${fill['fee_usd']:.{pp}f} cost=${cost_usd:.{pp}f} "
        f"avg_entry=${state.avg_entry_price:.{pp}f} ({reason})"
    )


async def execute_buy(exchange, state: State, wallet: PaperWallet, cfg: dict,
                      ticker: dict, level: int, reason: str,
                      log: logging.LoggerAdapter,
                      force_market: bool = False) -> None:
    """Execute a buy. Branches on cfg['buy_order_type'] unless force_market=True.

    force_market=True is used by the manual `b` key so "buy now" always means
    market, regardless of the coin's configured buy_order_type.

    For the limit path, this places a post-only limit buy, records it in
    state.pending_buy_order, and returns -- the position is added when/if the
    order fills (handled by check_pending_buy).
    """
    pp         = cfg["price_prec"]
    symbol     = cfg["symbol"]
    usd        = cfg["usd_per_buy"]
    order_type = "market" if force_market else cfg.get("buy_order_type", "market")

    # Force-market path may need to cancel an outstanding limit buy first.
    if force_market and state.pending_buy_order is not None:
        pending = state.pending_buy_order
        if state.mode == "live" and pending.order_id:
            try:
                await live_cancel_order(exchange, symbol, pending.order_id)
                log.info(f"[LIVE] cancelled pending LIMIT-BUY id={pending.order_id}")
            except Exception as e:
                log.warning("Could not cancel pending LIMIT-BUY %s: %s", pending.order_id, e)
        else:
            log.info("[PAPER] dropped pending LIMIT-BUY to force market buy")
        state.pending_buy_order = None

    if order_type == "limit":
        offset = cfg.get("limit_buy_offset_pct", 0.001)
        # Reference price: prefer bid (typical maker placement for buys) then last then ask.
        ref = ticker.get("bid") or ticker.get("last") or ticker.get("ask")
        if ref is None:
            log.warning("LIMIT-BUY skipped -- no reference price on ticker; falling back to market")
        else:
            limit_price = ref * (1 - offset)
            # qty sized to spend ~usd_per_buy worth at the limit price (ignoring fee impact)
            qty_coin = usd / limit_price
            if state.mode == "live":
                try:
                    placed = await live_limit_buy(exchange, symbol, qty_coin, limit_price)
                    state.pending_buy_order = PendingBuyOrder(
                        order_id=placed["order_id"],
                        limit_price=placed["limit_price"],
                        qty_coin=placed["qty"],
                        level=level,
                        placed_at_price=ref,
                        placed_ts=time.time(),
                    )
                except ccxt.ExchangeError as e:
                    log.error("LIMIT-BUY rejected (%s) -- falling back to market", e)
                    state.pending_buy_order = None
                else:
                    state.last_action_ts = time.time()
                    state.trailing_buy = TrailState()
                    log.info(
                        f"[LIVE] LIMIT-BUY placed @ ${limit_price:.{pp}f} "
                        f"qty={qty_coin:.8f} ref=${ref:.{pp}f} -{offset:.3%} "
                        f"level={level} id={placed['order_id']} ({reason})"
                    )
                    return
            else:
                state.pending_buy_order = PendingBuyOrder(
                    order_id=None,
                    limit_price=limit_price,
                    qty_coin=qty_coin,
                    level=level,
                    placed_at_price=ref,
                    placed_ts=time.time(),
                )
                state.last_action_ts = time.time()
                state.trailing_buy = TrailState()
                log.info(
                    f"[PAPER] LIMIT-BUY placed @ ${limit_price:.{pp}f} "
                    f"qty={qty_coin:.8f} ref=${ref:.{pp}f} -{offset:.3%} "
                    f"level={level} ({reason})"
                )
                return
        # Falls through to market path on missing-ref or live exchange rejection

    if state.mode == "live":
        fill = await live_buy(exchange, symbol, usd, ticker)
    else:
        fill = await paper_buy(wallet, usd, state, ticker)

    _finalize_buy(state, fill, level, cfg, reason, "BUY", log)


async def check_pending_buy(exchange, state: State, wallet: PaperWallet, cfg: dict,
                             ticker: dict, log: logging.LoggerAdapter) -> bool:
    """Poll/simulate fill for an outstanding limit buy. Returns True if the order
    filled this tick (so the caller knows to save state and consider follow-on
    actions), False otherwise (still pending, cancelled, or no pending order).

    Cancel rule: cancel if current_price > placed_at_price * (1 + cancel_buffer),
    where cancel_buffer = max(trail_buy_pct, limit_buy_offset_pct). The max()
    guards against coins with trail_buy_pct=0 -- otherwise any tick above the
    placement price would trigger an immediate cancel.
    """
    pending = state.pending_buy_order
    if pending is None:
        return False

    pp             = cfg["price_prec"]
    symbol         = cfg["symbol"]
    trail_buy_pct  = cfg["trail_buy_pct"]
    offset         = cfg.get("limit_buy_offset_pct", 0.001)
    cancel_buffer  = max(trail_buy_pct, offset)
    price          = ticker.get("last")
    cancel_price   = pending.placed_at_price * (1 + cancel_buffer)

    if state.mode == "live":
        try:
            order = await exchange.fetch_order(pending.order_id, symbol)
        except ccxt.OrderNotFound:
            log.warning("LIMIT-BUY %s not found at exchange -- clearing pending", pending.order_id)
            state.pending_buy_order = None
            return False
        status = (order.get("status") or "").lower()
        if status == "closed":
            fill_price = float(order.get("average") or order.get("price") or pending.limit_price)
            filled_qty = float(order.get("filled") or pending.qty_coin)
            fee_usd    = float((order.get("fee") or {}).get("cost") or 0.0)
            fill = {"qty": filled_qty, "price": fill_price, "fee_usd": fee_usd, "ts": _now_iso()}
            _finalize_buy(state, fill, pending.level, cfg, "limit-buy filled", "LIMIT-BUY FILL", log)
            return True
        if status in ("canceled", "cancelled", "rejected", "expired"):
            log.info("LIMIT-BUY %s ended with status=%s -- clearing pending", pending.order_id, status)
            state.pending_buy_order = None
            return False
        # Still open -- check cancel rule
        if price is not None and price > cancel_price:
            log.info(
                f"[LIVE] LIMIT-BUY cancelling @ ${price:.{pp}f} "
                f"(limit ${pending.limit_price:.{pp}f}, threshold ${cancel_price:.{pp}f})"
            )
            await live_cancel_order(exchange, symbol, pending.order_id)
            state.pending_buy_order = None
        return False

    # Paper mode -- check fill on ask crossing
    ask = ticker.get("ask") or price
    if ask is not None and ask <= pending.limit_price:
        gross   = pending.qty_coin * pending.limit_price
        fee_usd = gross * MAKER_FEE_PCT
        await wallet.spend(gross)  # debit the limit price worth of USD
        state.paper_wallet_coin += pending.qty_coin
        fill = {"qty": pending.qty_coin, "price": pending.limit_price,
                "fee_usd": fee_usd, "ts": _now_iso()}
        _finalize_buy(state, fill, pending.level, cfg, "limit-buy filled (paper)", "LIMIT-BUY FILL", log)
        return True

    if price is not None and price > cancel_price:
        log.info(
            f"[PAPER] LIMIT-BUY cancelling @ ${price:.{pp}f} "
            f"(limit ${pending.limit_price:.{pp}f}, threshold ${cancel_price:.{pp}f})"
        )
        state.pending_buy_order = None
    return False


# A live sell is clamped to the exchange free balance (see live_sellable_qty),
# so the qty actually sold can be below the tracked position. Selling at least
# this fraction of the tracked qty is a normal full close -- the tiny remainder
# is just fee dust Kraken shaved off the base coin. Below it, the fill is a
# *partial* close: the tracked position has desynced from the real balance, so
# we book PnL on the sold portion only, keep the rest on the books, and pause
# the coin. Charging the whole stack's cost against a half-size fill is what
# fabricated the ~$175 phantom "loss" on SYN cycle 6.
FULL_CLOSE_MIN_FILL_FRAC = 0.98


async def _finalize_sell(state: State, fill: dict, cfg: dict, reason: str, tag: str,
                          log: logging.LoggerAdapter, push_notify,
                          pause_on_partial: bool = True) -> float:
    """Post-fill state cleanup shared by market sells and limit-sell fills.

    `fill` must carry: qty, price, fee_usd, proceeds_net (or we compute it).
    `tag` is a short log label ("SELL-ALL" for market, "LIMIT-SELL FILL" for limit).
    Returns realized PnL.

    Realized PnL is always booked against the cost basis of the coins *actually*
    sold. On a full close that is the whole position; on a partial fill it is the
    proportional (average-cost) share and the unsold remainder is kept on the
    books. `pause_on_partial` (default True) pauses the coin on a partial -- the
    caller sets it False when the remainder is known-good (e.g. the unfilled part
    of a cancelled order, which is genuinely back in free balance) rather than a
    balance/tracking desync that needs manual reconciliation.
    """
    pp     = cfg["price_prec"]
    symbol = cfg["symbol"]
    base   = symbol.split("/")[0]

    qty          = fill["qty"]
    proceeds_net = fill.get("proceeds_net", qty * fill["price"] - fill["fee_usd"])
    tracked      = state.total_qty_coin
    n_levels     = len(state.positions)
    avg_entry    = state.avg_entry_price or 0.0

    partial = tracked > 0 and (qty / tracked) < FULL_CLOSE_MIN_FILL_FRAC

    if partial:
        # Cost basis of only the coins that sold (average-cost allocation).
        frac_sold     = qty / tracked
        cost_basis    = state.total_cost_usd * frac_sold
        remaining_qty = tracked - qty
    else:
        cost_basis = state.total_cost_usd

    realized = proceeds_net - cost_basis
    pct      = (realized / cost_basis * 100) if cost_basis > 0 else 0.0
    state.realized_pnl_usd += realized

    if partial:
        # Scale every level down proportionally so per-level data stays
        # consistent with the new totals and avg_entry is preserved.
        keep = 1.0 - frac_sold
        for p in state.positions:
            p.qty_coin *= keep
            p.fee_usd  *= keep
        state.total_qty_coin  = remaining_qty
        state.total_cost_usd -= cost_basis
        state.avg_entry_price = (
            state.total_cost_usd / remaining_qty if remaining_qty > 0 else None
        )
        state.last_action_ts  = time.time()
        state.trailing_sell   = TrailState()
        state.pending_sell_order = None
        if pause_on_partial:
            state.paused = True   # desync -- stop trading this coin until reconciled
    else:
        state.positions = []
        state.total_qty_coin = 0.0
        state.total_cost_usd = 0.0
        state.avg_entry_price = None
        state.last_buy_price = None
        state.cycle_count += 1
        state.last_action_ts = time.time()
        state.initial_entry_high = None
        state.breakeven_exit_armed = False
        state.trailing_buy = TrailState()
        state.trailing_sell = TrailState()
        state.pending_sell_order = None

    qty_field = f"qty={qty:.8f}/{tracked:.8f}" if partial else f"qty={qty:.8f}"
    log.info(
        f"[{state.mode.upper()}] {tag}{' PARTIAL' if partial else ''} "
        f"price=${fill['price']:.{pp}f} {qty_field} "
        f"fee=${fill['fee_usd']:.{pp}f} net=${proceeds_net:.{pp}f} "
        f"avg_entry=${avg_entry:.{pp}f} levels={n_levels} "
        f"realized=${realized:.{pp}f} ({pct:+.5f}%) "
        f"cycle={state.cycle_count} ({reason})"
    )

    if partial:
        if pause_on_partial:
            log.warning(
                f"[{state.mode.upper()}] PARTIAL SELL -- only {qty:.8f}/{tracked:.8f} {base} "
                f"({frac_sold:.1%}) sold; free balance was below the tracked position. "
                f"Booked PnL on the sold portion only; kept {remaining_qty:.8f} {base} "
                f"(cost ${state.total_cost_usd:.{pp}f}) on the books and PAUSED the coin. "
                f"Reconcile tracked qty vs the exchange, then resume or clear-stats "
                f"(select coin -> menu 4 -> Y)."
            )
        else:
            log.warning(
                f"[{state.mode.upper()}] PARTIAL SELL -- {qty:.8f}/{tracked:.8f} {base} "
                f"({frac_sold:.1%}) filled before the order ended; booked PnL on the sold "
                f"portion and kept {remaining_qty:.8f} {base} "
                f"(cost ${state.total_cost_usd:.{pp}f}) on the books -- continuing."
            )
        mode_tag   = "[PAPER] " if state.mode == "paper" else ""
        tail       = "coin PAUSED" if pause_on_partial else f"{remaining_qty:.4f} {base} left"
        await push_notify(
            f"{mode_tag}{base} PARTIAL sell {qty:.4f}/{tracked:.4f} for {pct:+.5f}% "
            f"(${realized:.{pp}f}) -- {tail}"
        )
        return realized

    # One-shot "pause after sell": if armed, pause the coin now that the cycle
    # has closed so it won't open a new buy cycle until manually resumed.
    paused_now = False
    if state.pause_after_sell:
        state.paused = True
        state.pause_after_sell = False
        paused_now = True
        log.info(
            f"[{state.mode.upper()}] PAUSE-AFTER-SELL fired -- coin paused; "
            f"no new buy cycle until resumed (select coin -> menu 4 -> Y)"
        )

    mode_tag = "[PAPER] " if state.mode == "paper" else ""
    pause_note = " -- now PAUSED" if paused_now else ""
    await push_notify(f"{mode_tag}{base} sold for {pct:+.5f}% (${realized:.{pp}f}){pause_note}")
    return realized


async def execute_sell_all(exchange, state: State, wallet: PaperWallet, cfg: dict,
                            ticker: dict, reason: str,
                            log: logging.LoggerAdapter, push_notify,
                            force_market: bool = False) -> float:
    """Execute a sell. Branches on cfg['order_type'] unless force_market=True.

    force_market=True is used by the manual `f` key so "sell now" always means
    market, regardless of the coin's configured order_type.

    For the market path (or force_market), this returns realized PnL synchronously.
    For the limit path, this places a post-only limit sell, records it in
    state.pending_sell_order, and returns 0.0 -- realized PnL is booked later by
    check_pending_sell() when/if the order fills.
    """
    qty = state.total_qty_coin
    if qty <= 0:
        return 0.0

    pp         = cfg["price_prec"]
    symbol     = cfg["symbol"]
    order_type = "market" if force_market else cfg.get("order_type", "market")

    # Force-market path may need to cancel an outstanding limit sell first.
    if force_market and state.pending_sell_order is not None:
        pending = state.pending_sell_order
        if state.mode == "live" and pending.order_id:
            try:
                await live_cancel_order(exchange, symbol, pending.order_id)
                log.info(f"[LIVE] cancelled pending LIMIT-SELL id={pending.order_id}")
            except Exception as e:
                log.warning("Could not cancel pending LIMIT-SELL %s: %s", pending.order_id, e)
        else:
            log.info("[PAPER] dropped pending LIMIT-SELL to force market sell")
        state.pending_sell_order = None

    # In live mode, clamp to the actual free balance (rounded down) so a
    # fee-shaved or precision-overcounted position can still exit instead of
    # bouncing off EOrder:Insufficient funds on both the limit and market paths.
    if state.mode == "live":
        qty = await live_sellable_qty(exchange, symbol, qty, log)
        if qty <= 0:
            log.error("SELL skipped -- no free %s balance on exchange to sell", symbol.split("/")[0])
            return 0.0

    if order_type == "limit":
        offset = cfg.get("limit_sell_offset_pct", 0.001)
        # Reference price: prefer ask (typical maker placement) then last then bid.
        ref = ticker.get("ask") or ticker.get("last") or ticker.get("bid")
        if ref is None:
            log.warning("LIMIT-SELL skipped -- no reference price on ticker; falling back to market")
        else:
            limit_price = ref * (1 + offset)
            if state.mode == "live":
                try:
                    placed = await live_limit_sell(exchange, symbol, qty, limit_price)
                    state.pending_sell_order = PendingSellOrder(
                        order_id=placed["order_id"],
                        limit_price=placed["limit_price"],
                        qty_coin=placed["qty"],
                        placed_ts=time.time(),
                    )
                except ccxt.ExchangeError as e:
                    log.error("LIMIT-SELL rejected (%s) -- falling back to market", e)
                    state.pending_sell_order = None
                else:
                    state.last_action_ts = time.time()
                    state.trailing_sell = TrailState()
                    log.info(
                        f"[LIVE] LIMIT-SELL placed @ ${limit_price:.{pp}f} "
                        f"qty={qty:.8f} ref=${ref:.{pp}f} +{offset:.3%} "
                        f"id={placed['order_id']} ({reason})"
                    )
                    return 0.0
            else:
                state.pending_sell_order = PendingSellOrder(
                    order_id=None,
                    limit_price=limit_price,
                    qty_coin=qty,
                    placed_ts=time.time(),
                )
                state.last_action_ts = time.time()
                state.trailing_sell = TrailState()
                log.info(
                    f"[PAPER] LIMIT-SELL placed @ ${limit_price:.{pp}f} "
                    f"qty={qty:.8f} ref=${ref:.{pp}f} +{offset:.3%} ({reason})"
                )
                return 0.0
        # Falls through to market path on missing-ref or live exchange rejection

    if state.mode == "live":
        fill = await live_sell_all(exchange, symbol, qty, ticker)
    else:
        fill = await paper_sell_all(wallet, qty, state, ticker)

    return await _finalize_sell(state, fill, cfg, reason, "SELL-ALL", log, push_notify)


async def execute_clear_stats(exchange, state: State, cfg: dict,
                              log: logging.LoggerAdapter) -> None:
    """Full reset of a coin's state back to fresh.

    Cancels any resting live order first (so it isn't orphaned on the exchange),
    then wipes positions, totals, realized PnL, cycle count, trails, flags, and
    pending orders. The coin is left PAUSED so it doesn't immediately open a new
    cycle right after the wipe -- resume it from the menu when ready.

    NOTE: in live mode any coins actually held on the exchange are NOT sold --
    the bot simply stops tracking them. The caller is expected to confirm.
    """
    symbol = cfg["symbol"]

    if state.mode == "live":
        for po in (state.pending_buy_order, state.pending_sell_order):
            oid = getattr(po, "order_id", None) if po is not None else None
            if oid:
                try:
                    await live_cancel_order(exchange, symbol, oid)
                    log.info(f"[LIVE] clear-stats cancelled resting order {oid}")
                except Exception as e:
                    log.warning("clear-stats: could not cancel %s: %s", oid, e)

    old_pnl, old_cycle, old_pos = state.realized_pnl_usd, state.cycle_count, len(state.positions)

    state.positions = []
    state.total_qty_coin = 0.0
    state.total_cost_usd = 0.0
    state.avg_entry_price = None
    state.last_buy_price = None
    state.realized_pnl_usd = 0.0
    state.cycle_count = 0
    state.last_action_ts = time.time()
    state.initial_entry_high = None
    state.breakeven_exit_armed = False
    state.pause_after_sell = False
    state.trailing_buy = TrailState()
    state.trailing_sell = TrailState()
    state.pending_buy_order = None
    state.pending_sell_order = None
    state.paper_wallet_coin = 0.0
    state.paused = True   # safety: don't auto-trade right after a wipe

    log.warning(
        f"[{state.mode.upper()}] STATS CLEARED -- full reset "
        f"(was realized=${old_pnl:.2f}, cycle={old_cycle}, {old_pos} position(s)); "
        f"coin is now PAUSED -- resume via: select coin -> menu 4 (toggle pause) -> Y"
    )


def _sell_fill_from_order(order: dict, pending: PendingSellOrder, filled_qty: float) -> dict:
    """Build a _finalize_sell `fill` dict from an exchange order's executed portion.

    `cost` is the gross quote received for the filled base amount; net proceeds
    subtract the fee. Falls back to filled_qty * price when a field is missing.
    """
    fill_price   = float(order.get("average") or order.get("price") or pending.limit_price)
    fee_usd      = float((order.get("fee") or {}).get("cost") or 0.0)
    proceeds_net = float(order.get("cost") or filled_qty * fill_price) - fee_usd
    return {"qty": filled_qty, "price": fill_price, "fee_usd": fee_usd,
            "ts": _now_iso(), "proceeds_net": proceeds_net}


async def check_pending_sell(exchange, state: State, wallet: PaperWallet, cfg: dict,
                              ticker: dict, log: logging.LoggerAdapter, push_notify) -> Optional[float]:
    """Poll/simulate fill for an outstanding limit sell. Returns realized PnL on
    fill, or None if still pending / cancelled with nothing filled / no pending order.

    Cancel rule: if current price has dropped more than cfg['trail_sell_pct']
    below the resting limit, cancel and clear pending_sell_order so the
    sell-trail can re-arm on the next rally. Any portion that executed before the
    order ended (closed, cancelled, or our own cancel) is always booked -- a
    dropped partial fill is what desynced SYN in cycle 6.
    """
    pending = state.pending_sell_order
    if pending is None:
        return None

    pp             = cfg["price_prec"]
    symbol         = cfg["symbol"]
    trail_sell_pct = cfg["trail_sell_pct"]
    price          = ticker.get("last")
    cancel_price   = pending.limit_price * (1 - trail_sell_pct)

    if state.mode == "live":
        try:
            order = await exchange.fetch_order(pending.order_id, symbol)
        except ccxt.OrderNotFound:
            log.warning("LIMIT-SELL %s not found at exchange -- clearing pending", pending.order_id)
            state.pending_sell_order = None
            return None
        status     = (order.get("status") or "").lower()
        filled_qty = float(order.get("filled") or 0.0)
        # A clamped order (placed for materially less than the tracked position)
        # means free balance was short -> a real desync, pause on partial. A
        # full-size order that only partly filled leaves the rest in free
        # balance, so keep trading.
        desync = pending.qty_coin < state.total_qty_coin * FULL_CLOSE_MIN_FILL_FRAC

        if status == "closed":
            if filled_qty <= 0:
                filled_qty = float(pending.qty_coin)
            fill = _sell_fill_from_order(order, pending, filled_qty)
            return await _finalize_sell(state, fill, cfg, "limit-sell filled",
                                        "LIMIT-SELL FILL", log, push_notify,
                                        pause_on_partial=desync)

        if status in _TERMINAL_ORDER_STATUSES:   # canceled/expired/rejected (closed handled above)
            state.pending_sell_order = None
            if filled_qty > 1e-12:
                log.info("LIMIT-SELL %s ended status=%s with %.8f filled -- booking partial",
                         pending.order_id, status, filled_qty)
                fill = _sell_fill_from_order(order, pending, filled_qty)
                return await _finalize_sell(state, fill, cfg, f"limit-sell {status} (partial fill)",
                                            "LIMIT-SELL FILL", log, push_notify,
                                            pause_on_partial=desync)
            log.info("LIMIT-SELL %s ended with status=%s -- clearing pending", pending.order_id, status)
            return None

        # Still open -- check cancel rule
        if price is not None and price < cancel_price:
            log.info(
                f"[LIVE] LIMIT-SELL cancelling @ ${price:.{pp}f} "
                f"(limit ${pending.limit_price:.{pp}f}, threshold ${cancel_price:.{pp}f})"
            )
            try:
                await live_cancel_order(exchange, symbol, pending.order_id)
            except Exception as e:
                log.warning("LIMIT-SELL cancel failed for %s: %s", pending.order_id, e)
            state.pending_sell_order = None
            # Re-fetch: part of the order may have executed before the cancel
            # landed. Booking it keeps tracked qty in sync (the SYN cycle-6 bug).
            try:
                final = await exchange.fetch_order(pending.order_id, symbol)
                filled_qty = float(final.get("filled") or 0.0)
            except Exception as e:
                log.warning("post-cancel fetch_order failed for %s: %s -- using last-known filled %.8f",
                            pending.order_id, e, filled_qty)
                final = order
            if filled_qty > 1e-12:
                log.info("LIMIT-SELL %s cancelled with %.8f filled -- booking partial",
                         pending.order_id, filled_qty)
                fill = _sell_fill_from_order(final, pending, filled_qty)
                return await _finalize_sell(state, fill, cfg, "limit-sell cancelled (partial fill)",
                                            "LIMIT-SELL FILL", log, push_notify,
                                            pause_on_partial=desync)
        return None

    # Paper mode -- check fill on bid crossing
    bid = ticker.get("bid") or price
    if bid is not None and bid >= pending.limit_price:
        gross   = pending.qty_coin * pending.limit_price
        fee_usd = gross * MAKER_FEE_PCT
        net     = gross - fee_usd
        await wallet.credit(net)
        state.paper_wallet_coin -= pending.qty_coin
        fill = {"qty": pending.qty_coin, "price": pending.limit_price,
                "fee_usd": fee_usd, "ts": _now_iso(), "proceeds_net": net}
        return await _finalize_sell(state, fill, cfg, "limit-sell filled (paper)", "LIMIT-SELL FILL", log, push_notify)

    if price is not None and price < cancel_price:
        log.info(
            f"[PAPER] LIMIT-SELL cancelling @ ${price:.{pp}f} "
            f"(limit ${pending.limit_price:.{pp}f}, threshold ${cancel_price:.{pp}f})"
        )
        state.pending_sell_order = None
    return None


# --- Price stream -----------------------------------------------------------

async def price_stream(exchange, symbol: str, simulate_file: Optional[str]) -> AsyncIterator[dict]:
    if simulate_file:
        prices: list[float] = []
        with open(simulate_file) as f:
            for row in csv.reader(f):
                if not row:
                    continue
                try:
                    prices.append(float(row[0]))
                except ValueError:
                    continue
        logging.info("[%s] Simulating %d prices from %s", symbol, len(prices), simulate_file)
        for p in prices:
            yield {"last": p, "bid": p * 0.9999, "ask": p * 1.0001}
            await asyncio.sleep(0.01)
        return

    backoff = 1.0
    while True:
        try:
            ticker = await exchange.watch_ticker(symbol)
            backoff = 1.0
            yield ticker
        except ccxt.NetworkError as e:
            logging.warning("[%s] WebSocket disconnect: %s -- reconnecting in %.1fs", symbol, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
        except ccxt.ExchangeError as e:
            logging.error("[%s] Exchange error in stream: %s -- retrying in %.1fs", symbol, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


# --- Manual command application ---------------------------------------------

def apply_manual(cmd: PendingAction, state: State, cfg: dict, current_price: float, log: logging.LoggerAdapter) -> Optional[PendingAction]:
    """Apply a manual command queued by the keypress listener.

    Pause and arm-sell-trail mutate state immediately and return None. Force-buy
    and force-sell return a PendingAction to fire on the same tick.
    """
    pp = cfg["price_prec"]

    if cmd.kind == "pause_toggle":
        state.paused = not state.paused
        log.info("MANUAL: %s", "PAUSED" if state.paused else "RESUMED")
        return None

    if cmd.kind == "arm_sell_trail":
        if not state.positions:
            log.info("MANUAL: arm-sell-trail ignored -- no open positions")
            return None
        state.trailing_sell = TrailState(armed=True, extreme=current_price, armed_at_price=current_price, manual=True)
        avg = state.avg_entry_price
        floor_note = f"won't sell below avg ${avg:.{pp}f}" if avg is not None else "clamped to avg entry"
        log.info(f"MANUAL: sell-trail ARMED at ${current_price:.{pp}f} (trailing stop -{cfg['trail_sell_pct']:.1%}, {floor_note})")
        return None

    if cmd.kind == "arm_breakeven_exit":
        state.breakeven_exit_armed = not state.breakeven_exit_armed
        if state.breakeven_exit_armed and state.avg_entry_price is not None:
            be_threshold, source_label, _ = sell_threshold(state, cfg)
            log.info(
                f"MANUAL: breakeven-exit ARMED -- will sell when price >= "
                f"${be_threshold:.{pp}f} ({source_label})"
            )
        elif state.breakeven_exit_armed:
            log.info("MANUAL: breakeven-exit ARMED (no open positions yet)")
        else:
            log.info("MANUAL: breakeven-exit DISARMED")
        return None

    if cmd.kind == "arm_pause_after_sell":
        state.pause_after_sell = not state.pause_after_sell
        if state.pause_after_sell:
            target = "current position sells" if state.positions else "next sell"
            log.info(f"MANUAL: pause-after-sell ARMED -- coin will pause once the {target}")
        else:
            log.info("MANUAL: pause-after-sell DISARMED")
        return None

    if cmd.kind == "clear_targets":
        # Re-baseline the initial-entry reference to the current price so the
        # first buy of a fresh cycle arms at current * (1 - drop_pct) again,
        # instead of trailing a stale "observed high" that drifted while the
        # coin was paused / a limit buy was resting. Only meaningful pre-entry;
        # with a position open there is no "first low" to reset, so we just
        # disarm any half-formed buy trail and leave the grid alone.
        if state.positions:
            state.trailing_buy = TrailState()
            log.info(
                f"MANUAL: clear-targets -- position open; disarmed pending buy-trail. "
                f"Grid buys still measured from last buy ${state.last_buy_price or 0.0:.{pp}f}."
            )
            return None
        old = state.initial_entry_high
        state.initial_entry_high = current_price
        state.trailing_buy = TrailState()
        arm = current_price * (1 - cfg["drop_pct"])
        was = f" (was ${old:.{pp}f})" if old is not None else ""
        log.info(
            f"MANUAL: clear-targets -- first-buy reference reset to "
            f"${current_price:.{pp}f}{was}; will arm at ${arm:.{pp}f} "
            f"(-{cfg['drop_pct']:.1%} from now)"
        )
        return None

    if cmd.kind == "clear_stats":
        # Needs exchange access (to cancel resting orders), so it's executed in
        # the run_coin dispatch loop -- just pass it through here.
        return cmd

    if cmd.kind == "buy":
        if len(state.positions) >= cfg["max_grid_levels"]:
            log.info("MANUAL: force-buy refused -- grid full (%d levels)", cfg["max_grid_levels"])
            return None
        cmd.level = len(state.positions)
        # Manual `b` is always immediate market buy regardless of cfg['buy_order_type'].
        # If a limit buy is currently pending, execute_buy will cancel it first.
        cmd.force_market = True
        if state.pending_buy_order is not None:
            log.info("MANUAL: force-buy queued (will cancel pending LIMIT-BUY first)")
        else:
            log.info("MANUAL: force-buy queued")
        return cmd

    if cmd.kind == "sell_all":
        if not state.positions:
            log.info("MANUAL: force-sell-all ignored -- no open positions")
            return None
        # Manual `f` is always immediate market sell regardless of cfg['order_type'].
        # If a limit sell is currently pending, execute_sell_all will cancel it first.
        cmd.force_market = True
        if state.pending_sell_order is not None:
            log.info("MANUAL: force-sell-all queued (will cancel pending LIMIT-SELL first)")
        else:
            log.info("MANUAL: force-sell-all queued")
        return cmd

    return None


# --- Per-coin task ----------------------------------------------------------

async def run_coin(
    exchange,
    cfg: dict,
    state: State,
    wallet: PaperWallet,
    cmd_queue: asyncio.Queue,
    last_price: dict,
    states: dict,
    blynk: BlynkClient,
    push_notify,
    dry_run: bool,
    simulate_file: Optional[str],
    bypass_cooldown: bool,
) -> None:
    symbol = cfg["symbol"]
    pp     = cfg["price_prec"]
    log    = CoinAdapter(logging.getLogger(), {"symbol": symbol})

    if state.positions:
        avg_entry = state.avg_entry_price or 0.0
        last_buy  = state.last_buy_price or 0.0
        log.info(
            f"Resumed {len(state.positions)} positions, "
            f"avg_entry=${avg_entry:.{pp}f}, last_buy=${last_buy:.{pp}f}, "
            f"cycle={state.cycle_count}"
        )
    if state.paused:
        log.warning("PAUSED (persisted from previous session). Resume via: select coin -> menu 4 (toggle pause) -> Y.")

    try:
        async for ticker in price_stream(exchange, symbol, simulate_file):
            try:
                price = ticker.get("last")
                if price is None:
                    continue
                last_price[symbol] = price

                action: Optional[PendingAction] = None

                # Drain manual commands for this coin (apply pause/arm-trail; queue buy/sell)
                while not cmd_queue.empty():
                    queued = cmd_queue.get_nowait()
                    qa = apply_manual(queued, state, cfg, price, log)
                    if qa is not None:
                        action = qa

                # Service any outstanding limit orders BEFORE the trail handlers,
                # so a fill in this tick books PnL/positions and the trail logic
                # sees the post-fill state.
                if state.pending_sell_order is not None:
                    realized = await check_pending_sell(
                        exchange, state, wallet, cfg, ticker, log, push_notify
                    )
                    if realized is not None:
                        asyncio.create_task(blynk.push_pnl(states, COINS))
                if state.pending_buy_order is not None:
                    await check_pending_buy(exchange, state, wallet, cfg, ticker, log)

                if action is None and not state.paused:
                    cooldown_ok = (
                        bypass_cooldown
                        or (time.time() - state.last_action_ts) >= ACTION_COOLDOWN_SEC
                    )
                    if cooldown_ok:
                        if not state.positions:
                            # Skip initial-entry trail while a limit buy is resting.
                            if state.pending_buy_order is None:
                                action = handle_initial_entry_trail(state, price, cfg, log)
                        else:
                            # Skip sell-trail while a limit sell is resting --
                            # don't want two sell signals racing.
                            if state.pending_sell_order is None:
                                action = handle_sell_trail(state, price, cfg, log)
                            # Same for buy-trail while a limit buy is resting.
                            if action is None and state.pending_buy_order is None:
                                action = handle_buy_trail(state, price, cfg, log)

                if action is not None:
                    if dry_run and state.mode == "live":
                        log.warning(
                            f"[DRY-RUN] would {action.kind} reason={action.reason} "
                            f"price=${price:.{pp}f}"
                        )
                    elif action.kind == "buy":
                        await execute_buy(
                            exchange, state, wallet, cfg, ticker, action.level,
                            action.reason, log, force_market=action.force_market,
                        )
                    elif action.kind == "sell_all":
                        await execute_sell_all(
                            exchange, state, wallet, cfg, ticker, action.reason,
                            log, push_notify, force_market=action.force_market,
                        )
                        # Realized PnL changed -- push to Blynk immediately (fire-and-forget)
                        asyncio.create_task(blynk.push_pnl(states, COINS))
                    elif action.kind == "clear_stats":
                        await execute_clear_stats(exchange, state, cfg, log)
                        # Realized PnL was zeroed -- refresh Blynk
                        asyncio.create_task(blynk.push_pnl(states, COINS))

                save_state(symbol, state)
            except ccxt.NetworkError as e:
                log.warning("Network error: %s", e)
            except ccxt.ExchangeError as e:
                log.error("Exchange error: %s", e)
    except asyncio.CancelledError:
        log.info("Coin task cancelled")
        raise
    finally:
        save_state(symbol, state)
