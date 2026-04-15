import logging

from apscheduler.schedulers.background import BackgroundScheduler

from .config import (
    THEORY_CRON_HOUR,
    THEORY_CRON_MINUTE,
    THEORY_SCAN_TIMESPAN,
    CRYPTO_EVENT_INTERVAL_MINUTES,
    CRYPTO_EVENT_TIMESPAN,
    CRYPTO_EVENT_KAFKA_TOPIC,
)

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _nightly_theory_job():
    """Scheduled job: generate supply-chain theory and publish signal to Kafka."""
    from .tools.supply_chain_theory import generate_supply_chain_theory

    logger.info("Nightly theory generation starting (timespan=%s)...", THEORY_SCAN_TIMESPAN)
    try:
        result = generate_supply_chain_theory(timespan=THEORY_SCAN_TIMESPAN)
        logger.info("Nightly theory generation complete: %s", result[:200])
    except Exception:
        logger.exception("Nightly theory generation failed")


def _crypto_event_job():
    """Scheduled job: classify crypto news events and publish to QuestDB + Kafka."""
    from .tools.crypto_event_classifier import run_crypto_event_pipeline

    logger.info(
        "Crypto event classification starting (timespan=%s, topic=%s)...",
        CRYPTO_EVENT_TIMESPAN,
        CRYPTO_EVENT_KAFKA_TOPIC,
    )
    try:
        n = run_crypto_event_pipeline(
            timespan=CRYPTO_EVENT_TIMESPAN,
            kafka_topic=CRYPTO_EVENT_KAFKA_TOPIC,
        )
        logger.info("Crypto event classification complete: %d events published", n)
    except Exception:
        logger.exception("Crypto event classification failed")


def start_scheduler():
    """Start the background scheduler with all scheduled jobs."""
    global _scheduler
    if _scheduler is not None:
        return

    _scheduler = BackgroundScheduler()

    # Nightly supply-chain theory generation
    _scheduler.add_job(
        _nightly_theory_job,
        trigger="cron",
        hour=THEORY_CRON_HOUR,
        minute=THEORY_CRON_MINUTE,
        id="nightly_theory",
        name="Nightly supply chain theory generation",
        misfire_grace_time=3600,
    )

    # Crypto event classification — runs every N minutes
    _scheduler.add_job(
        _crypto_event_job,
        trigger="interval",
        minutes=CRYPTO_EVENT_INTERVAL_MINUTES,
        id="crypto_event_classifier",
        name=f"Crypto event classification (every {CRYPTO_EVENT_INTERVAL_MINUTES}min)",
        misfire_grace_time=300,
    )

    _scheduler.start()
    logger.info(
        "Scheduler started: nightly theory at %02d:%02d UTC, "
        "crypto events every %d min",
        THEORY_CRON_HOUR,
        THEORY_CRON_MINUTE,
        CRYPTO_EVENT_INTERVAL_MINUTES,
    )
