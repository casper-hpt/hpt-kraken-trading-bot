from __future__ import annotations

import argparse
import logging
import signal
import time

from .config import Config
from .dedup_store import DedupStore
from .fetchers.gdelt_fetcher import GDELTFetcher
from .llm_classifier import LLMClassifier
from .metrics import CYCLES_TOTAL, CLASSIFIED_TOTAL, CYCLE_DURATION, start_metrics_server
from .fetchers.rss_fetcher import FeedItem, RSSFetcher
from .storage.signal_schema import SignalSchemaManager
from .storage.signal_writer import SignalWriter

log = logging.getLogger(__name__)

_shutdown_requested = False


def _signal_handler(signum: int, frame: object) -> None:
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    log.info("Received %s, initiating graceful shutdown...", sig_name)
    _shutdown_requested = True


def _maybe_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]
        load_dotenv()
    except ImportError:
        pass


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _fetch_all_sources(
    cfg: Config,
    rss: RSSFetcher,
    gdelt: GDELTFetcher,
) -> list[FeedItem]:
    """Merge items from RSS and GDELT, deduplicating by signal_id."""
    seen: set[str] = set()
    merged: list[FeedItem] = []

    rss_items = rss.fetch_all(list(cfg.rss_feed_urls))
    log.info("RSS: fetched %d items", len(rss_items))
    for item in rss_items:
        if item.signal_id not in seen:
            seen.add(item.signal_id)
            merged.append(item)

    if cfg.gdelt_enabled:
        gdelt_items = gdelt.fetch(
            query=cfg.gdelt_query,
            max_records=cfg.gdelt_max_records,
            timespan=cfg.gdelt_timespan,
        )
        log.info("GDELT: fetched %d items", len(gdelt_items))
        for item in gdelt_items:
            if item.signal_id not in seen:
                seen.add(item.signal_id)
                merged.append(item)

    log.info("Total after merge+dedup: %d items", len(merged))
    return merged


def run_cycle(
    cfg: Config,
    rss: RSSFetcher,
    gdelt: GDELTFetcher,
    classifier: LLMClassifier,
    writer: SignalWriter,
    dedup: DedupStore,
) -> int:
    cycle_start = time.monotonic()

    items = _fetch_all_sources(cfg, rss, gdelt)

    new_ids = set(dedup.filter_new([item.signal_id for item in items]))
    new_items = [item for item in items if item.signal_id in new_ids]
    log.info("%d new items after dedup", len(new_items))

    capped = new_items[: cfg.llm_max_items_per_cycle]
    if len(new_items) > cfg.llm_max_items_per_cycle:
        log.info("Capped to %d items (llm_max_items_per_cycle)", cfg.llm_max_items_per_cycle)

    signals = classifier.classify_batch(capped)
    log.info("Classified %d signals", len(signals))

    written = 0
    if signals:
        written = writer.write_signals(signals)
        log.info("Wrote %d signals to QuestDB", written)
        for s in signals:
            dedup.mark_seen(s.signal_id)
        CLASSIFIED_TOTAL.inc(len(signals))

    CYCLES_TOTAL.inc()
    CYCLE_DURATION.set(time.monotonic() - cycle_start)
    return written


def cmd_bootstrap(cfg: Config) -> int:
    SignalSchemaManager(cfg.questdb_exec_url).ensure_schema()
    return 0


def _build_fetchers(cfg: Config) -> tuple[RSSFetcher, GDELTFetcher]:
    return RSSFetcher(timeout_s=cfg.rss_timeout_s), GDELTFetcher(timeout_s=cfg.rss_timeout_s)


def cmd_run_once(cfg: Config) -> int:
    rss, gdelt = _build_fetchers(cfg)
    classifier = LLMClassifier(
        api_key=cfg.llm_api_key,
        base_url=cfg.llm_api_base_url,
        model=cfg.llm_model,
        timeout_s=cfg.llm_timeout_s,
    )
    writer = SignalWriter(cfg.questdb_ilp_conf)
    dedup = DedupStore()
    written = run_cycle(cfg, rss, gdelt, classifier, writer, dedup)
    log.info("run-once complete. signals_written=%d", written)
    return 0


def cmd_serve(cfg: Config) -> int:
    global _shutdown_requested

    start_metrics_server(cfg.prometheus_port)
    log.info("Prometheus metrics available on :%d/metrics", cfg.prometheus_port)

    SignalSchemaManager(cfg.questdb_exec_url).ensure_schema()

    rss, gdelt = _build_fetchers(cfg)
    classifier = LLMClassifier(
        api_key=cfg.llm_api_key,
        base_url=cfg.llm_api_base_url,
        model=cfg.llm_model,
        timeout_s=cfg.llm_timeout_s,
    )
    writer = SignalWriter(cfg.questdb_ilp_conf)
    dedup = DedupStore()
    interval_s = cfg.signal_poll_interval_minutes * 60

    log.info("Starting serve loop. poll_interval_minutes=%d", cfg.signal_poll_interval_minutes)

    while not _shutdown_requested:
        try:
            run_cycle(cfg, rss, gdelt, classifier, writer, dedup)
        except Exception:
            log.exception("Signal cycle failed, will retry next interval")

        if not _shutdown_requested:
            log.info("Sleeping %ds until next cycle...", interval_s)
            deadline = time.monotonic() + interval_s
            while not _shutdown_requested and time.monotonic() < deadline:
                time.sleep(1)

    log.info("Serve loop terminated gracefully")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="crypto-signal-service",
        description="LLM-based crypto signal classifier",
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("bootstrap", help="Create/ensure QuestDB schema for crypto_signals")
    sub.add_parser("run-once", help="Run one classification cycle")
    sub.add_parser("serve", help="Run poll loop (bootstraps schema on startup)")
    return p


def main(argv: list[str] | None = None) -> None:
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    _maybe_load_dotenv()
    cfg = Config.from_env()
    _setup_logging(cfg.log_level)

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "bootstrap":
        rc = cmd_bootstrap(cfg)
    elif args.cmd == "run-once":
        rc = cmd_run_once(cfg)
    elif args.cmd == "serve":
        rc = cmd_serve(cfg)
    else:
        raise SystemExit(2)

    raise SystemExit(rc)


if __name__ == "__main__":
    main()
