from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

_DEFAULT_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/"
    ";https://cointelegraph.com/rss"
    ";https://decrypt.co/feed"
    ";https://cryptoslate.com/feed/"
)


@dataclass(frozen=True)
class Config:
    llm_api_base_url: str = "https://api.openai.com/v1"
    llm_api_key: str = ""
    llm_model: str = "gpt-4o-mini"
    llm_timeout_s: float = 30.0
    llm_max_items_per_cycle: int = 20

    rss_feed_urls: tuple[str, ...] = ()
    rss_timeout_s: float = 15.0

    gdelt_enabled: bool = True
    gdelt_query: str = "(bitcoin OR ethereum OR crypto OR stablecoin OR defi OR inflation OR recession OR tariff OR sanctions OR regulation OR hack OR bankruptcy) sourcelang:english"
    gdelt_max_records: int = 50
    gdelt_timespan: str = "2h"

    signal_poll_interval_minutes: int = 60

    questdb_host: str = "localhost"
    questdb_http_port: int = 9000

    prometheus_port: int = 9093

    log_level: str = "INFO"

    @property
    def questdb_exec_url(self) -> str:
        return f"http://{self.questdb_host}:{self.questdb_http_port}/exec"

    @property
    def questdb_ilp_conf(self) -> str:
        return f"http::addr={self.questdb_host}:{self.questdb_http_port};"

    @classmethod
    def from_env(cls) -> "Config":
        llm_api_key = os.getenv("LLM_API_KEY", "")
        if not llm_api_key:
            log.warning("LLM_API_KEY is not set — LLM classification will fail")

        raw_feeds = os.getenv("RSS_FEED_URLS", _DEFAULT_FEEDS)
        feed_urls = tuple(u.strip() for u in raw_feeds.split(";") if u.strip())

        return cls(
            llm_api_base_url=os.getenv("LLM_API_BASE_URL", "https://api.openai.com/v1"),
            llm_api_key=llm_api_key,
            llm_model=os.getenv("LLM_MODEL", "gpt-4o-mini"),
            llm_timeout_s=float(os.getenv("LLM_TIMEOUT_S", "30.0")),
            llm_max_items_per_cycle=int(os.getenv("LLM_MAX_ITEMS_PER_CYCLE", "20")),
            rss_feed_urls=feed_urls,
            rss_timeout_s=float(os.getenv("RSS_TIMEOUT_S", "15.0")),
            gdelt_enabled=os.getenv("GDELT_ENABLED", "true").lower() == "true",
            gdelt_query=os.getenv(
                "GDELT_QUERY",
                "(bitcoin OR ethereum OR crypto OR stablecoin OR defi OR inflation OR recession OR tariff OR sanctions OR regulation OR hack OR bankruptcy) sourcelang:english",
            ),
            gdelt_max_records=int(os.getenv("GDELT_MAX_RECORDS", "50")),
            gdelt_timespan=os.getenv("GDELT_TIMESPAN", "2h"),
            signal_poll_interval_minutes=int(os.getenv("SIGNAL_POLL_INTERVAL_MINUTES", "60")),
            questdb_host=os.getenv("QUESTDB_HOST", "localhost"),
            questdb_http_port=int(os.getenv("QUESTDB_HTTP_PORT", "9000")),
            prometheus_port=int(os.getenv("PROMETHEUS_PORT", "9093")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
