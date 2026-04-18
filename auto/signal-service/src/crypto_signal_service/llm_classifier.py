from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from openai import OpenAI

from .fetchers.rss_fetcher import FeedItem

log = logging.getLogger(__name__)

_VALID_EVENT_TYPES = frozenset({
    "hack", "regulatory", "partnership", "listing", "macro",
    "earnings", "adoption", "technical", "other",
})
_VALID_ASSET_SCOPES = frozenset({"BTC", "ETH", "alt", "market-wide", "other"})
_VALID_HORIZONS = frozenset({"intraday", "1-7d", "1-4w", "structural", "unclear"})
_VALID_DIRECTIONS = frozenset({"bullish", "bearish", "neutral"})

_SYSTEM_PROMPT = (
    "You are a crypto market signal classifier. Given a news headline and summary, "
    "return a JSON object with these fields:\n"
    "- event_type: one of hack|regulatory|partnership|listing|macro|earnings|adoption|technical|other\n"
    "- asset_scope: one of BTC|ETH|alt|market-wide|other\n"
    "- affected_symbols: list of ticker strings (e.g. [\"BTC\", \"ETH\"]) or []\n"
    "- time_horizon: one of intraday|1-7d|1-4w|structural|unclear\n"
    "- direction: one of bullish|bearish|neutral\n"
    "- confidence: float 0-1 (how confident you are in this classification)\n"
    "- novelty: float 0-1 (how novel/surprising is this news)\n"
    "- tradability: float 0-1 (how likely to move the market)\n"
    "- key_reason: one sentence explaining the classification\n\n"
    "Respond ONLY with the JSON object, no other text."
)


@dataclass
class CryptoSignal:
    signal_id: str
    ts: datetime
    event_type: str
    asset_scope: str
    affected_symbols: list[str]
    time_horizon: str
    direction: str
    confidence: float
    novelty: float
    tradability: float
    catalyst_score: float  # novelty * confidence * tradability
    key_reason: str
    headline: str
    source_url: str


class LLMClassifier:
    def __init__(self, api_key: str, base_url: str, model: str, timeout_s: float = 30.0):
        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url,
            default_headers={"x-api-key": api_key},
        )
        self._model = model
        self._timeout_s = timeout_s

    def classify(self, item: FeedItem) -> CryptoSignal | None:
        user_content = f"Headline: {item.title}\nSummary: {item.summary[:500]}"
        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                response_format={"type": "json_object"},
                timeout=self._timeout_s,
            )
        except Exception:
            log.warning("LLM call failed for signal_id=%s", item.signal_id, exc_info=True)
            return None

        raw = response.choices[0].message.content or ""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("LLM returned non-JSON for signal_id=%s: %r", item.signal_id, raw[:200])
            return None

        return self._parse(item, data)

    def _parse(self, item: FeedItem, data: dict) -> CryptoSignal | None:
        required = (
            "event_type", "asset_scope", "affected_symbols", "time_horizon",
            "direction", "confidence", "novelty", "tradability", "key_reason",
        )
        for key in required:
            if key not in data:
                log.warning("LLM response missing key=%s for signal_id=%s", key, item.signal_id)
                return None

        if data["event_type"] not in _VALID_EVENT_TYPES:
            log.warning("Invalid event_type=%r for signal_id=%s", data["event_type"], item.signal_id)
            return None
        if data["asset_scope"] not in _VALID_ASSET_SCOPES:
            log.warning("Invalid asset_scope=%r for signal_id=%s", data["asset_scope"], item.signal_id)
            return None
        if data["time_horizon"] not in _VALID_HORIZONS:
            log.warning("Invalid time_horizon=%r for signal_id=%s", data["time_horizon"], item.signal_id)
            return None
        if data["direction"] not in _VALID_DIRECTIONS:
            log.warning("Invalid direction=%r for signal_id=%s", data["direction"], item.signal_id)
            return None

        try:
            confidence = float(data["confidence"])
            novelty = float(data["novelty"])
            tradability = float(data["tradability"])
        except (TypeError, ValueError):
            log.warning("LLM returned non-numeric scores for signal_id=%s", item.signal_id)
            return None

        affected = data.get("affected_symbols", [])
        if not isinstance(affected, list):
            affected = []
        affected_symbols = [str(s).strip().upper() for s in affected if s]

        return CryptoSignal(
            signal_id=item.signal_id,
            ts=datetime.now(timezone.utc),
            event_type=data["event_type"],
            asset_scope=data["asset_scope"],
            affected_symbols=affected_symbols,
            time_horizon=data["time_horizon"],
            direction=data["direction"],
            confidence=confidence,
            novelty=novelty,
            tradability=tradability,
            catalyst_score=novelty * confidence * tradability,
            key_reason=str(data.get("key_reason", "")),
            headline=item.title,
            source_url=item.source_url,
        )

    def classify_batch(self, items: list[FeedItem]) -> list[CryptoSignal]:
        results: list[CryptoSignal] = []
        for item in items:
            signal = self.classify(item)
            if signal is not None:
                results.append(signal)
        return results
