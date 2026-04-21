"""Centralized configuration for crypto-trader.

All settings loaded from environment variables (with sensible defaults).
In production, non-secret values come from a K8s ConfigMap and secrets
come from a K8s Secret — both mounted via envFrom.
"""

from __future__ import annotations

import ast
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent  # src/
DATA_DIR = BASE_DIR / "data"

POSITIONS_PATH = Path(os.getenv("POSITIONS_PATH", str(DATA_DIR / "positions.json")))
EMA_PARAMS_PATH = DATA_DIR / "ema_params.json"

# ── QuestDB connection ───────────────────────────────────────────────────────
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_PORT = int(os.getenv("QUESTDB_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DBNAME = os.getenv("QUESTDB_DBNAME", "qdb")

# ── Kraken API credentials ───────────────────────────────────────────────────
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY", "")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET", "")

# ── Prometheus ───────────────────────────────────────────────────────────────
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9095"))

# ── Runtime ──────────────────────────────────────────────────────────────────
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
CYCLE_INTERVAL = int(os.getenv("CYCLE_INTERVAL", "15"))       # minutes between rebalances
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "5"))
SETTLEMENT_DELAY_SECONDS = float(os.getenv("SETTLEMENT_DELAY_SECONDS", "5.0"))

# ── Strategy parameters ──────────────────────────────────────────────────────
MOMENTUM_LOOKBACK = 1000
QUANTILE_WINDOW = 5000
QUANTILE_STRIDE = 50

FAST_LIST = (5, 10, 20, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000)
SLOW_LIST = (20, 50, 100, 150, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
             1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000)

BUY_THRESH = 2.0
SELL_THRESH = -1.0
N_POSITIONS = MAX_POSITIONS
SLIP = 0.005
MIN_HOLD_BARS = 8
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.08"))

_tiers_raw = os.getenv("PROFIT_TAKE_TIERS", "0.10,0.20")
PROFIT_TAKE_TIERS = [float(t.strip()) for t in _tiers_raw.split(",")]
PROFIT_TAKE_FRACTION = float(os.getenv("PROFIT_TAKE_FRACTION", "0.50"))

BARS_PER_DAY = 96  # 24h × 4 bars/hour (15-min bars, crypto 24/7)

# ── Limit order settings ─────────────────────────────────────────────────────
# Seconds between each fill-status poll
LIMIT_ORDER_POLL_INTERVAL = float(os.getenv("LIMIT_ORDER_POLL_INTERVAL", "5.0"))
# Seconds to wait per attempt before checking price drift
LIMIT_ORDER_FILL_TIMEOUT = float(os.getenv("LIMIT_ORDER_FILL_TIMEOUT", "60.0"))
# Fraction of price movement (e.g. 0.005 = 0.5%) that triggers a cancel+repost
LIMIT_ORDER_DRIFT_PCT = float(os.getenv("LIMIT_ORDER_DRIFT_PCT", "0.005"))
# Maximum cancel+repost cycles before giving up on an order
LIMIT_ORDER_MAX_RETRIES = int(os.getenv("LIMIT_ORDER_MAX_RETRIES", "5"))

# ── LLM signal gate ──────────────────────────────────────────────────────────
SIGNAL_GATE_ENABLED            = os.getenv("SIGNAL_GATE_ENABLED", "false").lower() == "true"
SIGNAL_CONFIDENCE_THRESHOLD    = float(os.getenv("SIGNAL_CONFIDENCE_THRESHOLD", "0.65"))
SIGNAL_BEARISH_RATIO_THRESHOLD = float(os.getenv("SIGNAL_BEARISH_RATIO_THRESHOLD", "0.6"))


# ── Coin list ────────────────────────────────────────────────────────────────

def load_coin_list() -> list[str]:
    """Load coin list from env var, QuestDB watchlist, or hardcoded defaults.

    Priority:
      1. ``crypto_list`` env var (Python list or CSV)
      2. QuestDB ``crypto_watchlist`` table
      3. Hardcoded default list
    """
    raw = os.getenv("crypto_list", "")
    if raw:
        try:
            coins = ast.literal_eval(raw)
            if isinstance(coins, list):
                return [str(c).strip() for c in coins]
        except (ValueError, SyntaxError):
            pass
        return [c.strip().strip('"').strip("'") for c in raw.split(",") if c.strip()]

    # Try QuestDB watchlist
    try:
        import psycopg
        with psycopg.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            dbname=QUESTDB_DBNAME,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol
                    FROM crypto_watchlist
                    LATEST ON updated_at PARTITION BY symbol
                    ORDER BY symbol
                """)
                rows = cur.fetchall()
                if rows:
                    coins = [str(row[0]) for row in rows]
                    log.info("Loaded %d symbols from QuestDB watchlist", len(coins))
                    return coins
    except Exception:
        log.warning("Could not load watchlist from QuestDB; using default list")

    return [
        "BTC", "ETH", "XRP", "SOL", "ADA", "DOGE", "LINK",
        "DOT", "AVAX", "LTC", "BCH", "XLM", "ETC", "ATOM",
        "XMR", "ZEC", "AAVE", "UNI", "ALGO", "NEAR",
        "ICP", "QNT", "APT", "SHIB", "TRX",
    ]
