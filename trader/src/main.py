"""Entry point for crypto-trader.

Loads config from environment, initialises all components, and runs the
strategy + trade-execution engine until interrupted.

Subcommands:
  (none)   Start the live trading engine (default)
  refit    Run weekly EMA parameter grid-search and exit
"""

from __future__ import annotations

import logging
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv optional in production (env vars injected by k8s)

from trader.src.config import (
    KRAKEN_API_KEY,
    KRAKEN_API_SECRET,
    POSITIONS_PATH,
    PROMETHEUS_PORT,
    DRY_RUN,
    CYCLE_INTERVAL,
    MAX_POSITIONS,
    SETTLEMENT_DELAY_SECONDS,
    EMA_PARAMS_PATH,
    load_coin_list,
)
from trader.src.engine.ema_filter import load_ema_params
from trader.src.engine.engine import Engine
from trader.src.market.questdb_client import QuestDBClient
from trader.src.metrics import start_metrics_server
from trader.src.positions.positions_cache import init_positions
from trader.src.trader.client import KrakenTrader


def _setup_logging() -> logging.Logger:
    level = logging.DEBUG if os.getenv("VERBOSE", "").lower() == "true" else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    return logging.getLogger("crypto-trader")


def cmd_refit() -> None:
    from trader.src.engine.ema_refit import refit_ema_params

    log = _setup_logging()
    log.info("EMA refit job starting ...")

    coin_list = load_coin_list()
    db_client = QuestDBClient(log=log)

    refit_ema_params(
        db_client=db_client,
        symbols=coin_list,
        output_path=str(EMA_PARAMS_PATH),
        log=log,
    )
    log.info("EMA refit job complete.")


def cmd_serve() -> None:
    log = _setup_logging()

    log.info("crypto-trader starting up ...")
    log.info("DRY_RUN=%s  CYCLE_INTERVAL=%d min  MAX_POSITIONS=%d", DRY_RUN, CYCLE_INTERVAL, MAX_POSITIONS)

    if not DRY_RUN and not KRAKEN_API_KEY:
        log.error("KRAKEN_API_KEY is not set and DRY_RUN is false — refusing to start")
        sys.exit(1)

    start_metrics_server(PROMETHEUS_PORT)

    db_client = QuestDBClient(log=log)
    ema_map = load_ema_params(log=log)
    coin_list = load_coin_list()

    log.info("Loaded %d EMA params, %d coins in watchlist", len(ema_map), len(coin_list))

    init_positions(str(POSITIONS_PATH), MAX_POSITIONS)

    trader = KrakenTrader(
        api_key=KRAKEN_API_KEY,
        api_secret=KRAKEN_API_SECRET,
        dry_run=DRY_RUN,
        logger=log,
    )

    engine = Engine(
        trader=trader,
        db_client=db_client,
        ema_map=ema_map,
        coin_list=coin_list,
        positions_path=str(POSITIONS_PATH),
        cycle_interval=CYCLE_INTERVAL,
        max_positions=MAX_POSITIONS,
        settlement_delay=SETTLEMENT_DELAY_SECONDS,
        log=log,
    )

    engine.run()


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "refit":
        cmd_refit()
    else:
        cmd_serve()


if __name__ == "__main__":
    main()
