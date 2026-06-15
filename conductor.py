"""Crypto grid/DCA bot (CCXT Pro WebSocket on Kraken) -- multi-coin.

All coins and their tunables live in config.py under COINS. Each coin runs
as its own asyncio task with its own ticker stream, state file, and trail
state machines. Paper-mode USD is a shared pool (wallet.json) across all
coins.

Run paper by default:
    python conductor.py
Run live (real orders):
    python conductor.py --live
Add --dry-run alongside --live to exercise the live code path without
sending real orders. --simulate CSV replays prices for the first
enabled coin only.
"""
from __future__ import annotations

import argparse
import asyncio
import http.client
import logging
import sys
import time
import urllib.parse
from typing import Optional

import ccxt
import ccxt.pro as ccxtpro

from helpers.blynk import BlynkClient, blynk_heartbeat
from helpers.config import (
    COINS,
    EXCHANGE_ID,
    KRAKEN_API_KEY,
    KRAKEN_API_SECRET,
    LOG_FILE,
    PAPER_STARTING_USD,
    PUSHOVER_ENABLED,
    PUSHOVER_SOUND,
    PUSHOVER_TOKEN,
    PUSHOVER_USER,
)
from helpers.coin_runner import run_coin
from helpers.state import PendingAction, State, load_state, save_state
from helpers.wallet import PaperWallet

# Runtime mode -- set in main() from the --live flag. "paper" until proven otherwise.
MODE = "paper"


# --- Notifications ----------------------------------------------------------

def _pushover_send(message: str) -> None:
    """Blocking Pushover POST. Failures are logged, never raised."""
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        logging.warning("Pushover creds missing in .env (PUSHOVER_TOKEN/PUSHOVER_USER) -- skipping push")
        return
    try:
        conn = http.client.HTTPSConnection("api.pushover.net:443", timeout=5)
        conn.request(
            "POST", "/1/messages.json",
            urllib.parse.urlencode({
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER,
                "message": message,
                "sound": PUSHOVER_SOUND,
            }),
            {"Content-type": "application/x-www-form-urlencoded"},
        )
        resp = conn.getresponse()
        if resp.status != 200:
            logging.warning("Pushover returned %d: %s", resp.status, resp.read()[:200])
        conn.close()
    except Exception as e:
        logging.warning("Pushover send failed: %s", e)


async def push_notify(message: str) -> None:
    """Async wrapper -- runs the blocking HTTP call in a thread so the event loop keeps ticking."""
    if not PUSHOVER_ENABLED:
        return
    await asyncio.to_thread(_pushover_send, message)


# --- Kraken balance refresher (live mode only) ------------------------------

KRAKEN_BALANCE_REFRESH_SEC = 30.0


async def kraken_balance_refresher(exchange, holder: dict) -> None:
    """Periodically poll exchange.fetch_balance() and store free USD into holder.

    holder is a shared dict {"usd": Optional[float], "ts": float} read by the
    keypress-driven status screens. The loop never raises -- errors are logged
    and the holder is left at its last-good value (or None if never fetched).
    """
    while True:
        try:
            bal = await exchange.fetch_balance()
            usd_block = bal.get("USD") or bal.get("ZUSD") or {}
            free = usd_block.get("free")
            if free is None:
                # Fall back to the top-level 'free' dict ccxt sometimes uses
                free = (bal.get("free") or {}).get("USD")
            if free is not None:
                holder["usd"] = float(free)
                holder["ts"] = time.time()
        except ccxt.NetworkError as e:
            logging.warning("Kraken balance fetch network error: %s", e)
        except Exception as e:
            logging.warning("Kraken balance fetch failed: %s", e)
        await asyncio.sleep(KRAKEN_BALANCE_REFRESH_SEC)


def _kraken_usd_line(balance_holder: Optional[dict], label_width: int) -> Optional[str]:
    """Render the 'Kraken USD' status line, or None if it shouldn't show."""
    if not balance_holder or balance_holder.get("usd") is None:
        return None
    usd = balance_holder["usd"]
    age = max(0, int(time.time() - balance_holder.get("ts", 0.0)))
    return f"{'Kraken USD:':<{label_width}}${usd:,.2f}  ({age}s ago)"


# --- Big ASCII banner (for the action-menu coin name) -----------------------
# 5-row block font. Dependency-free so it works on any terminal. Covers A-Z,
# 0-9 and '/' -- everything that appears in a Kraken pair symbol.
_BANNER_FONT = {
    "A": [" ## ", "#  #", "####", "#  #", "#  #"],
    "B": ["### ", "#  #", "### ", "#  #", "### "],
    "C": [" ###", "#   ", "#   ", "#   ", " ###"],
    "D": ["### ", "#  #", "#  #", "#  #", "### "],
    "E": ["####", "#   ", "### ", "#   ", "####"],
    "F": ["####", "#   ", "### ", "#   ", "#   "],
    "G": [" ###", "#   ", "# ##", "#  #", " ###"],
    "H": ["#  #", "#  #", "####", "#  #", "#  #"],
    "I": ["###", " # ", " # ", " # ", "###"],
    "J": ["####", "  # ", "  # ", "# # ", " ## "],
    "K": ["#  #", "# # ", "##  ", "# # ", "#  #"],
    "L": ["#   ", "#   ", "#   ", "#   ", "####"],
    "M": ["#   #", "## ##", "# # #", "#   #", "#   #"],
    "N": ["#  #", "## #", "# ##", "#  #", "#  #"],
    "O": [" ## ", "#  #", "#  #", "#  #", " ## "],
    "P": ["### ", "#  #", "### ", "#   ", "#   "],
    "Q": [" ## ", "#  #", "# ##", "#  #", " ###"],
    "R": ["### ", "#  #", "### ", "# # ", "#  #"],
    "S": [" ###", "#   ", " ## ", "   #", "### "],
    "T": ["#####", "  #  ", "  #  ", "  #  ", "  #  "],
    "U": ["#  #", "#  #", "#  #", "#  #", " ## "],
    "V": ["#   #", "#   #", "#   #", " # # ", "  #  "],
    "W": ["#   #", "#   #", "# # #", "## ##", "#   #"],
    "X": ["#  #", " ## ", " ## ", " ## ", "#  #"],
    "Y": ["#   #", " # # ", "  #  ", "  #  ", "  #  "],
    "Z": ["####", "  # ", " #  ", "#   ", "####"],
    "0": [" ## ", "#  #", "#  #", "#  #", " ## "],
    "1": [" # ", "## ", " # ", " # ", "###"],
    "2": ["### ", "   #", " ## ", "#   ", "####"],
    "3": ["### ", "   #", " ## ", "   #", "### "],
    "4": ["#  #", "#  #", "####", "   #", "   #"],
    "5": ["####", "#   ", "### ", "   #", "### "],
    "6": [" ###", "#   ", "### ", "#  #", " ## "],
    "7": ["####", "   #", "  # ", " #  ", " #  "],
    "8": [" ## ", "#  #", " ## ", "#  #", " ## "],
    "9": [" ## ", "#  #", " ###", "   #", "### "],
    "/": ["   #", "  # ", " #  ", "#   ", "#   "],
    "-": ["    ", "    ", "####", "    ", "    "],
    " ": ["  ", "  ", "  ", "  ", "  "],
}


def big_banner(text: str, gap: int = 2) -> str:
    """Render `text` as a 5-line ASCII block banner. Unknown chars render blank."""
    rows = ["", "", "", "", ""]
    spacer = " " * gap
    for ch in text.upper():
        glyph = _BANNER_FONT.get(ch, _BANNER_FONT[" "])
        w = max(len(r) for r in glyph)
        for i in range(5):
            rows[i] += glyph[i].ljust(w) + spacer
    return "\n".join(r.rstrip() for r in rows)


# --- Retro "80s synthwave" banner -------------------------------------------
# Sunset gradient (top->bottom): yellow -> orange -> hot pink -> magenta-purple.
# Neon-cyan double-line border. 256-color ANSI; set BANNER_RETRO=False to fall
# back to the plain monochrome banner on terminals without color.
_RETRO_ROW_COLORS = [226, 214, 208, 199, 165]
_RETRO_BORDER = 51   # neon cyan
BANNER_RETRO = True


def _ansi(code: int, s: str) -> str:
    return f"\x1b[1;38;5;{code}m{s}\x1b[0m"


def retro_banner(text: str) -> str:
    """80s synthwave banner: neon-cyan box + sunset-gradient solid-block letters."""
    body = big_banner(text, gap=1).replace("#", "█").split("\n")
    width = max((len(r) for r in body), default=0)
    bar = _ansi(_RETRO_BORDER, "║")
    top = _ansi(_RETRO_BORDER, "╔" + "═" * (width + 2) + "╗")
    bot = _ansi(_RETRO_BORDER, "╚" + "═" * (width + 2) + "╝")
    out = [top]
    for i, r in enumerate(body):
        color = _RETRO_ROW_COLORS[min(i, len(_RETRO_ROW_COLORS) - 1)]
        out.append(f"{bar} {_ansi(color, r.ljust(width))} {bar}")
    out.append(bot)
    return "\n".join(out)


def menu_banner(text: str) -> str:
    """Banner used by the action menu -- retro by default, plain as a fallback."""
    return retro_banner(text) if BANNER_RETRO else "\n" + big_banner(text) + "\n"


# --- Stats display ----------------------------------------------------------

def format_stats(state: State, cfg: dict, last_price: Optional[float], wallet: PaperWallet,
                 balance_holder: Optional[dict] = None) -> str:
    pp              = cfg["price_prec"]
    base            = cfg["symbol"].split("/")[0]
    max_grid_levels = cfg["max_grid_levels"]
    drop_pct        = cfg["drop_pct"]
    trail_buy_pct   = cfg["trail_buy_pct"]
    trail_sell_pct  = cfg["trail_sell_pct"]
    take_profit_pct = cfg["take_profit_pct"]

    L = 17  # label column width -- aligns all "Label:" values into one column

    bar = "=" * 60
    out = [bar, f"STATS  {cfg['symbol']}", bar]

    mode_line = state.mode.upper()
    if state.paused:
        mode_line += "  [PAUSED]"
    if state.breakeven_exit_armed:
        mode_line += "  [BE-EXIT]"
    if state.pause_after_sell:
        mode_line += "  [PAUSE-AFTER-SELL]"
    out.append(f"{'Mode:':<{L}}{mode_line}")
    out.append(f"{'Cycle:':<{L}}{state.cycle_count}")
    out.append(f"{'Realized PnL:':<{L}}${state.realized_pnl_usd:+,.{pp}f}")
    if last_price is not None:
        out.append(f"{'Current price:':<{L}}${last_price:,.{pp}f}")
    out.append(
        f"{'Paper wallet:':<{L}}${wallet.usd:,.{pp}f} USD (shared) "
        f"+ {state.paper_wallet_coin:.8f} {base}"
    )
    kraken_line = _kraken_usd_line(balance_holder, L)
    if kraken_line:
        out.append(kraken_line)
    out.append("")

    if state.positions:
        if last_price is not None and state.avg_entry_price:
            unreal = (last_price - state.avg_entry_price) * state.total_qty_coin
            unreal_pct = (last_price / state.avg_entry_price - 1) * 100
            unreal_val = f"${unreal:+,.{pp}f} ({unreal_pct:+.5f}%)"
        else:
            unreal_val = "(no price yet)"
        out.append(f"{'Open positions:':<{L}}{len(state.positions)} / {max_grid_levels}")
        out.append(f"{'  Avg entry:':<{L}}${state.avg_entry_price:,.{pp}f}")
        out.append(
            f"{'  Total cost:':<{L}}${state.total_cost_usd:,.{pp}f}  "
            f"({state.total_qty_coin:.8f} {base})"
        )
        out.append(f"{'  Last buy:':<{L}}${state.last_buy_price:,.{pp}f}")
        out.append(f"{'  Unrealized:':<{L}}{unreal_val}")
    else:
        out.append(f"{'Open positions:':<{L}}0  (waiting for initial entry)")
        if state.initial_entry_high is not None:
            out.append(f"{'  Running high:':<{L}}${state.initial_entry_high:,.{pp}f}")

    def delta_str(target: float) -> str:
        if last_price is None or last_price == 0:
            return ""
        d = (target / last_price - 1) * 100
        return f"  ({d:+.5f}% from now)"

    out.append("")
    out.append("Next BUY:")
    if state.pending_buy_order is not None:
        pbo = state.pending_buy_order
        offset_pct = cfg.get("limit_buy_offset_pct", 0.001)
        cancel_buffer = max(trail_buy_pct, offset_pct)
        cancel = pbo.placed_at_price * (1 + cancel_buffer)
        out.append(f"{'  Pending LIMIT BUY:':<{L}}@ ${pbo.limit_price:,.{pp}f}  qty={pbo.qty_coin:.8f} {cfg['symbol'].split('/')[0]}  level={pbo.level}")
        out.append(f"{'  Fills when:':<{L}}ask <= ${pbo.limit_price:,.{pp}f}{delta_str(pbo.limit_price)}")
        out.append(f"{'  Cancels when:':<{L}}price > ${cancel:,.{pp}f}{delta_str(cancel)}")
    elif state.trailing_buy.armed and state.trailing_buy.extreme is not None:
        fire = state.trailing_buy.extreme * (1 + trail_buy_pct)
        out.append(f"{'  Trail:':<{L}}ARMED  (low so far ${state.trailing_buy.extreme:,.{pp}f})")
        out.append(f"{'  Fires at:':<{L}}${fire:,.{pp}f}{delta_str(fire)}")
    elif len(state.positions) >= max_grid_levels:
        out.append(f"{'  Status:':<{L}}grid full ({max_grid_levels} levels) -- no more buys until sell-all")
    elif state.positions and state.last_buy_price is not None:
        arm = state.last_buy_price * (1 - drop_pct)
        out.append(f"{'  Will arm at:':<{L}}${arm:,.{pp}f}{delta_str(arm)}")
        out.append(f"{'  Source:':<{L}}-{drop_pct:.1%} from last buy ${state.last_buy_price:,.{pp}f}")
    elif not state.positions and state.initial_entry_high is not None:
        arm = state.initial_entry_high * (1 - drop_pct)
        out.append(f"{'  Will arm at:':<{L}}${arm:,.{pp}f}{delta_str(arm)}")
        out.append(f"{'  Source:':<{L}}-{drop_pct:.1%} from observed high ${state.initial_entry_high:,.{pp}f}")
    else:
        out.append(f"{'  Status:':<{L}}waiting for first price tick...")

    out.append("")
    out.append("Next SELL:")
    if state.pending_sell_order is not None:
        pso = state.pending_sell_order
        cancel = pso.limit_price * (1 - trail_sell_pct)
        out.append(f"{'  Pending LIMIT SELL:':<{L}}@ ${pso.limit_price:,.{pp}f}  qty={pso.qty_coin:.8f} {cfg['symbol'].split('/')[0]}")
        out.append(f"{'  Fills when:':<{L}}bid >= ${pso.limit_price:,.{pp}f}{delta_str(pso.limit_price)}")
        out.append(f"{'  Cancels when:':<{L}}price < ${cancel:,.{pp}f}{delta_str(cancel)}")
    elif state.trailing_sell.armed and state.trailing_sell.extreme is not None:
        fire = state.trailing_sell.extreme * (1 - trail_sell_pct)
        out.append(f"{'  Trail:':<{L}}ARMED  (high so far ${state.trailing_sell.extreme:,.{pp}f})")
        out.append(f"{'  Fires at:':<{L}}${fire:,.{pp}f}{delta_str(fire)}")
    elif state.positions and state.avg_entry_price is not None:
        from helpers.coin_runner import sell_threshold
        arm, source_label, _ = sell_threshold(state, cfg)
        out.append(f"{'  Will arm at:':<{L}}${arm:,.{pp}f}{delta_str(arm)}")
        out.append(f"{'  Source:':<{L}}{source_label} (avg ${state.avg_entry_price:,.{pp}f})")
    else:
        out.append(f"{'  Status:':<{L}}N/A (no open positions)")

    out.append(bar)
    return "\n".join(out)


def format_config(cfg: dict) -> str:
    L = 21  # label column width

    bar = "=" * 60
    out = [bar, f"CONFIG  {cfg['symbol']}", bar]

    out.append(f"{'Symbol:':<{L}}{cfg['symbol']}")
    out.append(f"{'Enabled:':<{L}}{cfg.get('enabled', True)}")
    out.append(f"{'Price precision:':<{L}}{cfg['price_prec']} decimals")
    out.append("")
    out.append("Buy settings:")
    out.append(f"{'  USD per buy:':<{L}}${cfg['usd_per_buy']:,.2f}")
    out.append(f"{'  Max grid levels:':<{L}}{cfg['max_grid_levels']}")
    out.append(f"{'  Drop pct:':<{L}}{cfg['drop_pct']:.3%}  (spacing between grid buys)")
    out.append(f"{'  Trail buy pct:':<{L}}{cfg['trail_buy_pct']:.3%}  (rebound off low to fire buy)")
    buy_order_type = cfg.get("buy_order_type", "market")
    out.append(f"{'  Buy order type:':<{L}}{buy_order_type}  (menu 7 'force BUY' is always market)")
    if buy_order_type == "limit":
        buy_offset = cfg.get("limit_buy_offset_pct", 0.001)
        out.append(f"{'  Buy limit offset:':<{L}}{buy_offset:.3%}  (limit_price = bid * (1-offset))")
    out.append("")
    out.append("Sell settings:")
    out.append(f"{'  Take profit pct:':<{L}}{cfg['take_profit_pct']:.3%}  (above avg entry to arm sell-trail)")
    out.append(f"{'  Trail sell pct:':<{L}}{cfg['trail_sell_pct']:.3%}  (pullback off high to fire sell)")
    order_type = cfg.get("order_type", "market")
    out.append(f"{'  Sell order type:':<{L}}{order_type}  (menu 8 'sell ALL' is always market)")
    if order_type == "limit":
        offset = cfg.get("limit_sell_offset_pct", 0.001)
        out.append(f"{'  Sell limit offset:':<{L}}{offset:.3%}  (limit_price = last * (1+offset))")
    out.append("")
    out.append(f"{'Blynk pin:':<{L}}{cfg.get('blynk_pin', '-')}")
    out.append(f"{'Max grid spend:':<{L}}${cfg['max_grid_levels'] * cfg['usd_per_buy']:,.2f}")

    out.append(bar)
    return "\n".join(out)


def format_all_stats(states: dict, cfgs: list, last_price: dict, wallet: PaperWallet,
                     balance_holder: Optional[dict] = None) -> str:
    bar = "=" * 60
    out = [bar, "ALL COINS", bar]

    L = 23  # label width for the summary section
    total_realized = sum(s.realized_pnl_usd for s in states.values())
    total_unreal = 0.0
    for c in cfgs:
        sym = c["symbol"]
        st = states.get(sym)
        if st and st.avg_entry_price and last_price.get(sym):
            total_unreal += (last_price[sym] - st.avg_entry_price) * st.total_qty_coin
    out.append(f"{'Paper wallet (shared):':<{L}}${wallet.usd:,.2f} USD")
    kraken_line = _kraken_usd_line(balance_holder, L)
    if kraken_line:
        out.append(kraken_line)
    out.append(f"{'Total realized PnL:':<{L}}${total_realized:+,.2f}")
    out.append(f"{'Total unrealized:':<{L}}${total_unreal:+,.2f}")
    out.append("")

    rows = []
    for i, c in enumerate(cfgs, start=1):
        sym = c["symbol"]
        st = states.get(sym)
        if not st:
            continue
        pp = c["price_prec"]
        base = sym.split("/")[0]
        lp = last_price.get(sym)
        if st.positions and st.avg_entry_price and lp:
            u = (lp - st.avg_entry_price) * st.total_qty_coin
            unreal = f"${u:+,.2f}"
        elif not st.positions:
            unreal = "-"
        else:
            unreal = "?"
        status_tags = []
        if st.paused:
            status_tags.append("[PAUSED]")
        if st.breakeven_exit_armed:
            status_tags.append("[BE-EXIT]")
        if st.pause_after_sell:
            status_tags.append("[PAUSE-AFTER-SELL]")
        rows.append({
            "n":       f"{i})",
            "sym":     sym,
            "pos":     f"{len(st.positions)}/{c['max_grid_levels']}",
            "real":    f"${st.realized_pnl_usd:+,.{pp}f}",
            "cycle":   str(st.cycle_count),
            "avg":     f"${st.avg_entry_price:,.{pp}f}" if st.avg_entry_price else "-",
            "qty":     f"{st.total_qty_coin:.8f} {base}" if st.positions else "-",
            "price":   f"${lp:,.{pp}f}" if lp else "?",
            "unreal":  unreal,
            "status":  " ".join(status_tags),
        })

    if rows:
        w_n      = max(len(r["n"])      for r in rows)
        w_sym    = max(len("Symbol"),     *(len(r["sym"])    for r in rows))
        w_pos    = max(len("Pos"),        *(len(r["pos"])    for r in rows))
        w_real   = max(len("Realized"),   *(len(r["real"])   for r in rows))
        w_cycle  = max(len("Cycle"),      *(len(r["cycle"])  for r in rows))
        w_avg    = max(len("Avg entry"),  *(len(r["avg"])    for r in rows))
        w_qty    = max(len("Holdings"),   *(len(r["qty"])    for r in rows))
        w_price  = max(len("Price"),      *(len(r["price"])  for r in rows))
        w_unreal = max(len("Unrealized"), *(len(r["unreal"]) for r in rows))

        out.append(
            f"  {'':<{w_n}} {'Symbol':<{w_sym}}  "
            f"{'Cycle':>{w_cycle}}  {'Pos':>{w_pos}}  "
            f"{'Holdings':<{w_qty}}  {'Price':>{w_price}}  "
            f"{'Avg entry':>{w_avg}}  {'Unrealized':>{w_unreal}}  "
            f"{'Realized':>{w_real}}  Status"
        )
        for r in rows:
            out.append(
                f"  {r['n']:<{w_n}} {r['sym']:<{w_sym}}  "
                f"{r['cycle']:>{w_cycle}}  {r['pos']:>{w_pos}}  "
                f"{r['qty']:<{w_qty}}  {r['price']:>{w_price}}  "
                f"{r['avg']:>{w_avg}}  {r['unreal']:>{w_unreal}}  "
                f"{r['real']:>{w_real}}  {r['status']}"
            )

    out.append(bar)
    return "\n".join(out)


# --- Keypress (TTY single-char read) ---------------------------------------

def _enable_keypress():
    if not sys.stdin.isatty():
        return None, None
    try:
        import termios
        import tty
    except ImportError:
        return None, None
    fd = sys.stdin.fileno()
    try:
        old = termios.tcgetattr(fd)
        tty.setcbreak(fd)
        return fd, old
    except Exception:
        return None, None


def _disable_keypress(fd, old) -> None:
    if fd is None or old is None:
        return
    try:
        import termios
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    except Exception:
        pass


# --- Main loop --------------------------------------------------------------

async def run(simulate_file: Optional[str], dry_run: bool) -> None:
    exchange_cls = getattr(ccxtpro, EXCHANGE_ID)
    creds: dict = {"enableRateLimit": True}
    if MODE == "live":
        creds["apiKey"] = KRAKEN_API_KEY
        creds["secret"] = KRAKEN_API_SECRET
        if not creds["apiKey"] or not creds["secret"]:
            raise RuntimeError("MODE=live but KRAKEN_API_KEY/SECRET missing in .env")
    exchange = exchange_cls(creds)

    if not simulate_file:
        await exchange.load_markets()

    enabled = [c for c in COINS if c.get("enabled", True)]
    if not enabled:
        raise RuntimeError("No enabled coins in COINS -- nothing to do")
    if simulate_file and len(enabled) > 1:
        logging.warning(
            "--simulate runs only the first enabled coin (%s); skipping %d others",
            enabled[0]["symbol"], len(enabled) - 1,
        )
        enabled = enabled[:1]

    # Load per-coin state. Migration of legacy state.json happens inside load_state.
    states: dict[str, State] = {}
    for c in enabled:
        s = load_state(c["symbol"], mode=MODE)
        s.mode = MODE
        states[c["symbol"]] = s

    # Shared paper wallet: prefer wallet.json; else bootstrap from the first
    # coin's legacy paper_wallet_usd (preserved through state.json migration).
    bootstrap_usd = states[enabled[0]["symbol"]].paper_wallet_usd
    wallet = PaperWallet.load_or_init(PAPER_STARTING_USD, bootstrap_usd=bootstrap_usd)
    # The legacy per-state paper_wallet_usd field is no longer authoritative --
    # zero it so subsequent saves reflect the new shared-wallet model.
    for st in states.values():
        st.paper_wallet_usd = 0.0

    if MODE == "paper":
        max_spend = sum(c["max_grid_levels"] * c["usd_per_buy"] for c in enabled)
        if max_spend > wallet.usd + sum(s.total_cost_usd for s in states.values()):
            logging.warning(
                "Max combined grid spend across coins (${:.2f}) exceeds available paper USD".format(max_spend)
            )

    if dry_run:
        logging.warning("DRY RUN: live order calls will be skipped")

    blynk = BlynkClient()

    # Manual command plumbing
    cmd_queues: dict[str, asyncio.Queue] = {c["symbol"]: asyncio.Queue() for c in enabled}
    last_price: dict[str, Optional[float]] = {c["symbol"]: None for c in enabled}

    # Startup banner -- vertical coin list for easy digit selection
    rows = []
    for i, c in enumerate(enabled, start=1):
        sym = c["symbol"]
        st = states[sym]
        pp = c["price_prec"]
        rows.append({
            "n":      f"{i})",
            "sym":    sym,
            "pos":    f"{len(st.positions)}/{c['max_grid_levels']}",
            "cycle":  str(st.cycle_count),
            "avg":    f"${st.avg_entry_price:.{pp}f}" if st.avg_entry_price else "-",
            "status": "[PAUSED]" if st.paused else "",
        })

    w_n     = max(len(r["n"])     for r in rows)
    w_sym   = max(len("Symbol"),    *(len(r["sym"])   for r in rows))
    w_pos   = max(len("Pos"),       *(len(r["pos"])   for r in rows))
    w_cycle = max(len("Cycle"),     *(len(r["cycle"]) for r in rows))
    w_avg   = max(len("Avg entry"), *(len(r["avg"])   for r in rows))

    logging.info("Coins:")
    logging.info(
        f"  {'':<{w_n}} {'Symbol':<{w_sym}}  "
        f"{'Cycle':>{w_cycle}}  {'Pos':>{w_pos}}  "
        f"{'Avg entry':>{w_avg}}  Status"
    )
    for r in rows:
        logging.info(
            f"  {r['n']:<{w_n}} {r['sym']:<{w_sym}}  "
            f"{r['cycle']:>{w_cycle}}  {r['pos']:>{w_pos}}  "
            f"{r['avg']:>{w_avg}}  {r['status']}"
        )
    logging.info("Keys: <digit> selects a coin (opens its action menu).  'a' = all-coin stats.  '?' = help.")

    # Keypress listener state
    selected_idx: list = [None]    # coin index whose action menu is open (None = idle)
    confirm_cmd:  list = [None]    # (idx, kind, label) awaiting Y/N confirmation
    balance_holder: dict = {"usd": None, "ts": 0.0}

    # Action menu: (label, kind, needs_confirm). 'stats'/'config' are handled
    # locally (printed); the rest are enqueued as PendingActions for the coin task.
    # (label, kind, needs_confirm). Everything asks Y to confirm except live stats.
    ACTIONS = [
        ("live stats",                 "stats",                False),
        ("config",                     "config",               True),
        ("arm sell-trail",             "arm_sell_trail",       True),
        ("toggle pause",               "pause_toggle",         True),
        ("toggle breakeven-exit",      "arm_breakeven_exit",   True),
        ("toggle pause-after-sell",    "arm_pause_after_sell", True),
        ("force BUY now (market)",     "buy",                  True),
        ("sell ALL now (market)",      "sell_all",             True),
        ("clear stats (FULL reset)",   "clear_stats",          True),
    ]

    REASONS = {
        "pause_toggle":         "manual pause toggle",
        "arm_sell_trail":       "manual arm-sell-trail",
        "arm_breakeven_exit":   "manual breakeven-exit toggle",
        "arm_pause_after_sell": "manual pause-after-sell toggle",
        "buy":                  "manual force-buy",
        "sell_all":             "manual force-sell-all",
        "clear_stats":          "manual clear-stats",
        "clear_targets":        "manual clear-targets",
    }

    HELP_TEXT = (
        "Controls:\n"
        "  <digit>   select a coin (1-{n}) -> opens its action menu\n"
        "  a         all-coin summary\n"
        "  ?         this help\n"
        "Action menu (after selecting a coin), type the number:\n"
        "  1 stats      2 config             3 arm sell-trail    4 pause\n"
        "  5 breakeven  6 pause-after-sell   7 BUY now           8 sell ALL now\n"
        "  9 clear stats (full reset)        t clear targets (reset first-buy)\n"
        "  Every action asks 'Y' to confirm except 1 (stats).  0/ENTER cancels.\n"
        "Ctrl-C to quit."
    ).format(n=len(enabled))

    def _action_menu(idx: int) -> str:
        sym = enabled[idx]["symbol"]
        lines = [menu_banner(sym), "  choose an action (type number; 0/ENTER cancels):"]
        for i, (label, _k, _needs) in enumerate(ACTIONS, start=1):
            lines.append(f"   {i}) {label}")
        lines.append("   t) clear targets (reset first-buy to -drop_pct from now)")
        lines.append("  all actions ask 'Y' to confirm except 1) live stats")
        return "\n".join(lines)

    def _enqueue(idx: int, kind: str) -> None:
        sym = enabled[idx]["symbol"]
        reason = REASONS.get(kind)
        if reason is None:
            return
        cmd_queues[sym].put_nowait(PendingAction(kind, 0, reason))
        print(f"  queued: {sym} -> {kind}")

    def _on_keypress() -> None:
        try:
            ch = sys.stdin.read(1)
        except Exception:
            return
        if not ch:
            return

        # 1) Awaiting Y/N confirmation of an action
        if confirm_cmd[0] is not None:
            idx, kind, label = confirm_cmd[0]
            confirm_cmd[0] = None
            if ch in ("y", "Y"):
                if kind == "config":
                    print(format_config(enabled[idx]))
                else:
                    _enqueue(idx, kind)
            else:
                print(f"  cancelled: {label}")
            return

        # 2) A coin is selected -> its action menu is open; expect a number
        if selected_idx[0] is not None:
            idx = selected_idx[0]
            sym = enabled[idx]["symbol"]
            if ch in ("\n", "\r", "0"):
                selected_idx[0] = None
                print("  (menu cancelled)")
                return
            if ch in ("t", "T"):   # lettered action -- numbered menu is full (1-9)
                selected_idx[0] = None
                dp = enabled[idx].get("drop_pct", 0.0)
                print(
                    f"  {sym}: clear targets -- re-baseline first-buy to "
                    f"-{dp:.1%} from current price.  Type Y to confirm, any other key cancels"
                )
                confirm_cmd[0] = (idx, "clear_targets", "clear targets")
                return
            if not ch.isdigit():
                print("  type a menu number (or 't'), or 0/ENTER to cancel")
                return
            n = int(ch)
            if not (1 <= n <= len(ACTIONS)):
                print(f"  no action {n} (valid 1-{len(ACTIONS)})")
                return
            label, kind, needs_confirm = ACTIONS[n - 1]
            selected_idx[0] = None
            if not needs_confirm:   # only 'live stats' -- show immediately
                print(format_stats(states[sym], enabled[idx], last_price[sym], wallet, balance_holder))
                return
            # everything else asks for a Y confirmation first
            if kind == "clear_stats":
                st = states[sym]
                msg = f"  CLEAR {sym}: zeroes realized PnL + cycle"
                if st.positions:
                    base = sym.split("/")[0]
                    msg += (f" AND wipes {len(st.positions)} open position(s) "
                            f"({st.total_qty_coin:.8f} {base}) -- the bot will FORGET them "
                            f"(they stay on Kraken!)")
                msg += ".  Coin will be PAUSED after.  Type Y to confirm:"
                print(msg)
            else:
                print(f"  {sym}: {label} -- type Y to confirm, any other key cancels")
            confirm_cmd[0] = (idx, kind, label)
            return

        # 3) Idle -- coin digit, 'a', or '?'
        if ch == "?":
            print(HELP_TEXT)
            return
        if ch in ("a", "A"):
            print(format_all_stats(states, enabled, last_price, wallet, balance_holder))
            return
        if ch.isdigit() and ch != "0":
            idx = int(ch) - 1
            if 0 <= idx < len(enabled):
                selected_idx[0] = idx
                print(_action_menu(idx))
            else:
                print(f"  no coin {ch} (valid 1-{len(enabled)})")
            return
        print(f"  type a coin number 1-{len(enabled)}, 'a' = all-coin stats, '?' = help")

    fd, old_term = _enable_keypress()
    loop = asyncio.get_event_loop()
    reader_attached = False
    if fd is not None:
        try:
            loop.add_reader(sys.stdin, _on_keypress)
            reader_attached = True
        except (NotImplementedError, OSError):
            _disable_keypress(fd, old_term)
            fd, old_term = None, None

    bypass_cooldown = bool(simulate_file)
    heartbeat_task = asyncio.create_task(blynk_heartbeat(blynk, states, enabled))
    balance_task = None
    if MODE == "live" and not simulate_file:
        balance_task = asyncio.create_task(kraken_balance_refresher(exchange, balance_holder))
    coin_tasks = [
        asyncio.create_task(
            run_coin(
                exchange, c, states[c["symbol"]], wallet,
                cmd_queues[c["symbol"]], last_price, states, blynk,
                push_notify, dry_run, simulate_file, bypass_cooldown,
            ),
            name=c["symbol"],
        )
        for c in enabled
    ]

    try:
        results = await asyncio.gather(*coin_tasks, return_exceptions=True)
        for c, r in zip(enabled, results):
            if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                logging.error("Coin task %s ended with: %s", c["symbol"], r)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logging.info("Shutdown requested")
    finally:
        heartbeat_task.cancel()
        if balance_task is not None:
            balance_task.cancel()
        if reader_attached:
            try:
                loop.remove_reader(sys.stdin)
            except Exception:
                pass
        _disable_keypress(fd, old_term)
        for sym, st in states.items():
            save_state(sym, st)
        wallet.save()
        try:
            await exchange.close()
        except Exception:
            pass


def setup_logging() -> None:
    from pathlib import Path
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
    )


def main() -> None:
    global MODE
    parser = argparse.ArgumentParser(description="Crypto grid/DCA bot -- multi-coin (Kraken)")
    parser.add_argument("--live", action="store_true",
                        help="Send real orders to the exchange. Default is paper.")
    parser.add_argument("--simulate", metavar="CSV",
                        help="Replay prices from a CSV (first enabled coin only)")
    parser.add_argument("--dry-run", action="store_true",
                        help="With --live, log intended orders but skip the API call")
    args = parser.parse_args()

    MODE = "live" if args.live else "paper"
    setup_logging()
    enabled_symbols = [c["symbol"] for c in COINS if c.get("enabled", True)]
    if MODE == "live":
        logging.warning("LIVE MODE -- real orders will be placed on %s for %s",
                        "kraken", ", ".join(enabled_symbols))
    asyncio.run(run(args.simulate, args.dry_run))


if __name__ == "__main__":
    main()
