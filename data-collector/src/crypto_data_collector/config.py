from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    # QuestDB
    questdb_host: str = "localhost"
    questdb_http_port: int = 9000

    # Scheduling
    ingest_interval_minutes: int = 15

    # Prometheus
    prometheus_port: int = 9092

    # Logging
    log_level: str = "INFO"

    # Watchlist override — if non-empty, used instead of the JSON file
    crypto_list: tuple[str, ...] = field(default_factory=tuple)

    @property
    def questdb_exec_url(self) -> str:
        return f"http://{self.questdb_host}:{self.questdb_http_port}/exec"

    @property
    def questdb_ilp_conf(self) -> str:
        """ILP/HTTP configuration string for questdb python client."""
        return f"http::addr={self.questdb_host}:{self.questdb_http_port};"

    @classmethod
    def from_env(cls) -> "Config":
        raw = os.getenv("CRYPTO_LIST", "")
        crypto_list: tuple[str, ...] = tuple(
            s.strip().upper() for s in raw.split(",") if s.strip()
        ) if raw else ()

        return cls(
            questdb_host=os.getenv("QUESTDB_HOST", "localhost"),
            questdb_http_port=int(os.getenv("QUESTDB_HTTP_PORT", "9000")),
            ingest_interval_minutes=int(os.getenv("INGEST_INTERVAL_MINUTES", "15")),
            prometheus_port=int(os.getenv("PROMETHEUS_PORT", "9092")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            crypto_list=crypto_list,
        )
