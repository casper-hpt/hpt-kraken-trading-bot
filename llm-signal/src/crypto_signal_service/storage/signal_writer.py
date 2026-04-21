from __future__ import annotations

import logging

import pandas as pd
from questdb.ingress import Sender, IngressError  # type: ignore

from ..llm_classifier import CryptoSignal

log = logging.getLogger(__name__)


class SignalWriter:
    def __init__(self, ilp_conf: str):
        self._ilp_conf = ilp_conf

    def write_signals(self, signals: list[CryptoSignal]) -> int:
        if not signals:
            return 0

        now = pd.Timestamp.now("UTC").tz_convert(None)

        rows = []
        for s in signals:
            ts = pd.Timestamp(s.ts)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("UTC").tz_localize(None)
            rows.append({
                "ts": ts,
                "signal_id": s.signal_id,
                "event_type": s.event_type,
                "asset_scope": s.asset_scope,
                "affected_symbols": ",".join(s.affected_symbols),
                "time_horizon": s.time_horizon,
                "direction": s.direction,
                "confidence": float(s.confidence),
                "novelty": float(s.novelty),
                "tradability": float(s.tradability),
                "catalyst_score": float(s.catalyst_score),
                "fallout_days": int(s.fallout_days),
                "key_reason": s.key_reason,
                "headline": s.headline,
                "source_url": s.source_url,
                "ingested_at": now,
            })

        df = pd.DataFrame(rows)

        try:
            with Sender.from_conf(self._ilp_conf) as sender:
                sender.dataframe(
                    df,
                    table_name="crypto_signals",
                    symbols=["event_type", "asset_scope", "time_horizon", "direction"],
                    at="ts",
                )
            return len(df)
        except IngressError:
            log.exception("QuestDB ILP ingest error for crypto_signals")
            raise
