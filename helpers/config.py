"""User-editable configuration for the crypto grid/DCA bot.

Edit values here, then run `python conductor.py` (paper) or
`python conductor.py --live`.

Multi-coin: each entry in COINS is one tracked pair with its own tunables.
The selector digit in manual controls is the entry's index + 1 (so the
first coin is `1`, the second is `2`, etc.).

Optional per-coin fields (omit to keep current behavior):
    "order_type":            "market" | "limit"   (default "market"; sells only)
    "limit_sell_offset_pct": 0.001                 (default 0.001; only used
                                                    when order_type == "limit".
                                                    Limit price = last * (1+offset).
                                                    Make it larger than the
                                                    typical bid/ask spread so
                                                    the order rests as a maker;
                                                    otherwise it may match
                                                    immediately as a taker.)
The manual `f` key always market-sells regardless of order_type. Automatic
trail-fires and the `t` (arm-sell-trail) flow honor order_type.

Secrets (API keys/tokens) are NOT stored here -- they load from a project-root
`.env` file (see `.env.example`). Copy `.env.example` to `.env` and fill it in.
"""

import os
from pathlib import Path


def _load_dotenv(path: Path) -> None:
    """Minimal `.env` loader -- stdlib only, no extra dependency (matching the
    rest of this project). Parses `KEY=VALUE` lines and pushes them into
    os.environ. Real environment variables always win (setdefault), so you can
    override any value per-shell without editing the file. Missing file is a
    silent no-op."""
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, sep, val = line.partition("=")
        if not sep:
            continue
        key, val = key.strip(), val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]          # strip matching surrounding quotes
        os.environ.setdefault(key, val)


# Load the project-root .env (one level up from this helpers/ dir) before the
# credential lookups below.
_load_dotenv(Path(__file__).resolve().parent.parent / ".env")

#total usd: 1800
# per coin: 450 @4



# --- Coins ------------------------------------------------------------------
COINS = [
    {
        "symbol":          	"VVV/USD",
        "price_prec":      	3,
        "drop_pct":        	0.02, #was 0.017
        "trail_buy_pct":   	0.001, # was .0008
        "trail_sell_pct":  	0.0065,
	"buy_order_type":	"limit",
	"limit_buy_offset_pct":	0.0,
	"order_type": 		"limit",
	"limit_sell_offset_pct":0.0,
        "take_profit_pct": 	0.003,
        "usd_per_buy":     	37.5,
        "max_grid_levels": 	12,
        "enabled":         	True,
        "blynk_pin":       	"V1",
    },
    {
	#Jun2 - initial config
	# Jun3 - raised limit_buy_offset_pct 0.00001 -> 0.001 to cut post-only
	#        cancels + widen cancel buffer (was filling only 5/21 limit buys)
        "symbol":          	"HYPE/USD",
        "price_prec":      	3,
        "drop_pct":        	0.015,
        "trail_buy_pct":   	0.002,
        "trail_sell_pct":  	0.004,
        "buy_order_type":       "limit",
        "limit_buy_offset_pct": 0.001,
        "order_type": 		"limit",
        "limit_sell_offset_pct":0.0,
        "take_profit_pct": 	0.04,
        "usd_per_buy":     	37.5,
        "max_grid_levels": 	12,
        "enabled":         	True,
        "blynk_pin":       	"V2",
    },
    {
	# edited 5/29 @15:20 - changed values
	# drop_pct 0.05 -> 0.06, trail_buy_pct 0.01->0.02
	# drop_pct 0.06 -> 0.05,
        "symbol":          	"ZEC/USD",
        "price_prec":      	2,
        "drop_pct":        	0.05,
        "trail_buy_pct":   	0.02,
        "trail_sell_pct":  	0.008,
        "buy_order_type":       "limit",
        "limit_buy_offset_pct": 0.00003,
        "order_type": 		"limit",
        "limit_sell_offset_pct":0.0,
        "take_profit_pct": 	0.08,
        "usd_per_buy":     	37.5,
        "max_grid_levels": 	15,
        "enabled":         	True,
        "blynk_pin":       	"V3",
    },
    {
        "symbol":          	"NEAR/USD",
        "price_prec":      	4,
        "drop_pct":        	0.013, #was 0.01
        "trail_buy_pct":   	0.003,
        "trail_sell_pct":  	0.009,
        "buy_order_type":       "limit",
        "limit_buy_offset_pct": 0.003,
        "order_type":	   	"limit",
        "limit_sell_offset_pct":0.0,
        "take_profit_pct": 	0.04,
        "usd_per_buy":     	37.5,
        "max_grid_levels": 	14,
        "enabled":         	True,
        "blynk_pin":       	"V4",
    },
    {
	# edited 5/29 @15:33 - changed values
	# lower limit_buy_offset_price .00005 -> .00002
        "symbol":          	"BTC/USD",
        "price_prec":      	1,
        "drop_pct":        	0.015, # was .01
        "trail_buy_pct":   	0.0003, # was .0008
        "trail_sell_pct":  	0.0004, # was 0.008
        "buy_order_type":       "limit",
        "limit_buy_offset_pct": 0.00002,
        "order_type": 		"limit",
        "limit_sell_offset_pct":0.0,
        "take_profit_pct": 	0.06,  #was .4
        "usd_per_buy":     	37.5,
        "max_grid_levels": 	15,
        "enabled":         	True,
        "blynk_pin":       	"V5",
    },
    {
	# edited 5/29 @15:46 - changed values
	# edited - changed lbop 0.0004 -> 0.00005
        "symbol":               "XMR/USD",
        "price_prec":           1,
        "drop_pct":             0.025,
        "trail_buy_pct":        0.006,
        "trail_sell_pct":       0.007,
        "buy_order_type":       "limit",
        "limit_buy_offset_pct": 0.00002,
        "order_type":           "limit",
        "limit_sell_offset_pct":0.0,
        "take_profit_pct":      0.08,
        "usd_per_buy":          37.5,
        "max_grid_levels":      14,
        "enabled":              True,
        "blynk_pin":            "V6",
    },
    {
        # edited -
        "symbol":               "TRX/USD",
        "price_prec":           4,
        "drop_pct":             0.009,
        "trail_buy_pct":        0.0008,
        "trail_sell_pct":       0.001,
        "buy_order_type":       "limit",
        "limit_buy_offset_pct": 0.00003,
        "order_type":           "limit",
        "limit_sell_offset_pct":0.0,
        "take_profit_pct":      0.04,
        "usd_per_buy":          37.5,
        "max_grid_levels":      10,
        "enabled":              True,
        "blynk_pin":            "V7",
    },
    {
        # edited -
        "symbol":               "TAO/USD",
        "price_prec":           2,
        "drop_pct":             0.03,
        "trail_buy_pct":        0.0075,
        "trail_sell_pct":       0.0075,
        "buy_order_type":       "limit",
        "limit_buy_offset_pct": 0.0001,
        "order_type":           "limit",
        "limit_sell_offset_pct":0.0,
        "take_profit_pct":      0.08,
        "usd_per_buy":          50,
        "max_grid_levels":      12,
        "enabled":              True,
        "blynk_pin":            "V8",
    },
]

# --- Exchange / paths -------------------------------------------------------
EXCHANGE_ID = "kraken"
STATE_DIR   = "data"
WALLET_FILE = "data/wallet.json"
LOG_FILE    = "data/grid_bot.log"

# --- Fees -------------------------------------------------------------------
TAKER_FEE_PCT = 0.004           # taker fee for paper-mode fee math (market orders)
MAKER_FEE_PCT = 0.002          # maker fee for paper-mode fee math (limit-sell fills)

# --- Paper wallet + pacing --------------------------------------------------
PAPER_STARTING_USD  = 1800.0    # shared paper USD across all coins
ACTION_COOLDOWN_SEC = 10.0       # min gap between order events per coin (bypassed under --simulate)
LIVE_BUY_FILL_TIMEOUT_SEC = 15.0  # max wait for market-buy fill confirmation via fetch_order polling

# --- Kraken API credentials (only needed for --live) -----------------------
# Secrets load from the project-root .env file (see .env.example).
KRAKEN_API_KEY    = os.getenv("KRAKEN_API_KEY", "")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET", "")

# --- Pushover notifications ------------------------------------------------
PUSHOVER_ENABLED = True
PUSHOVER_TOKEN   = os.getenv("PUSHOVER_TOKEN", "")
PUSHOVER_USER    = os.getenv("PUSHOVER_USER", "")
PUSHOVER_SOUND   = os.getenv("PUSHOVER_SOUND", "cashregister")

# --- Blynk (push realized PnL to virtual pins) ------------------------------
BLYNK_ENABLED       = True                     # master switch -- set False to disable all Blynk pushes
BLYNK_TOKEN         = os.getenv("BLYNK_TOKEN", "")  # Blynk Cloud token (set in .env)
BLYNK_TOTAL_PNL_PIN = "V0"                     # pin for sum-of-all-coins realized PnL
BLYNK_HEARTBEAT_SEC = 120                       # also push every N seconds even with no trades
BLYNK_BASE_URL      = "https://blynk.cloud"
