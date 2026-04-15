import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from ..config import OLLAMA_MODEL, OLLAMA_URL, REQUEST_TIMEOUT_S
from ..kafka_producer import publish_signal
from ..metrics import (
    THEORY_ARTICLES_SCANNED,
    THEORY_DO_NOT_TRADE,
    THEORY_ERRORS_TOTAL,
    THEORY_RUNS_TOTAL,
    THEORY_RUN_DURATION,
    THEORY_SIGNAL_PUBLISHED_TOTAL,
    THEORY_SYMBOLS_SCORED,
)
from ..questdb import get_watchlist_details
from .gdelt import fetch_all_topics
from .base import register_tool
from .historical import cluster_articles, build_fingerprint, search_similar_historical_incidents

logger = logging.getLogger(__name__)

THEORY_DIR = Path(os.getenv("THEORY_DIR", "/app/theories"))

ROUND1_BATCH_SIZE = int(os.getenv("ROUND1_BATCH_SIZE", "10"))
ROUND2_BATCH_SIZE = int(os.getenv("ROUND2_BATCH_SIZE", "10"))
ROUND2_KEEP = int(os.getenv("ROUND2_KEEP", "20"))
TOURNAMENT_SEED = os.getenv("TOURNAMENT_SEED")
TRANSCRIPT_LIMIT = int(os.getenv("TRANSCRIPT_LIMIT", "500"))
HISTORICAL_LOOKBACK_YEARS = int(os.getenv("HISTORICAL_LOOKBACK_YEARS", "10"))
HISTORICAL_TOP_K = int(os.getenv("HISTORICAL_TOP_K", "5"))
ENABLE_HISTORICAL_ANALOGS = os.getenv("ENABLE_HISTORICAL_ANALOGS", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

NEWS_DIGEST_PROMPT = """\
You are a senior commodity and supply chain analyst. Analyze the following news \
headlines from GDELT and produce a concise market-moving themes summary.

## Your task:
1. Identify the key disruptions, geopolitical events, and macro trends currently moving markets.
2. For each theme, note the affected sectors, commodities, and geographic regions.
3. Assess severity (Low / Medium / High / Critical) for each theme.
4. Do NOT recommend any stocks yet — focus purely on the themes and their market implications.

## Format:
Return a structured markdown report with:
- Executive Summary (2-3 sentences)
- Key Themes (table: Theme | Severity | Affected Sectors | Geographic Scope | Price Direction)
- Cascading Effects (how themes interconnect)

## Headlines:

{news_data}
"""

TOURNAMENT_RANK_PROMPT = """\
You are a quantitative equity analyst running a comparative ranking tournament. \
Given the current market themes below, rank these {n_symbols} symbols from most \
positively affected to most negatively affected, THEN assign scores consistent \
with your ranking.

## Current Market Themes:
{themes_summary}

## Scoring Rules:
- Score each symbol from -1.00 to +1.00
- +1.00 = strong LONG opportunity (company benefits significantly from these themes)
- -1.00 = strong SHORT opportunity (company is severely hurt by these themes)
- 0.00 = neutral / unaffected
- COMPARE the symbols against each other — relative ranking matters
- Only score away from 0.00 if there is a clear, specific connection to the themes above
- Consider the company's sector, industry, geography, and business description
- If earnings are coming soon, note the additional risk/opportunity

## Stocks to Rank:

{symbols_info}

## Output:
Return ONLY a JSON array sorted by score descending (no markdown, no explanation). Each element:
{{"symbol": "<TICKER>", "rank": <1-N>, "score": <-1.00 to +1.00>, "reasoning": "<1-2 sentences>"}}
"""

FINAL_SIGNAL_PROMPT = """\
You are a quantitative trading signal generator. You are given:
1. The current market themes
2. The top {n_long} LONG candidates (high scores, companies that benefit)
3. The top {n_short} SHORT candidates (low scores, companies that suffer)

Review these candidates and produce a single JSON object (no markdown, no explanation, \
ONLY valid JSON) with this exact schema:

{{
  "theme": "<short description of the main disruption theme>",
  "confidence": <0.0-1.0, how confident you are in the overall thesis>,
  "novelty": <0.0-1.0, how new/surprising is this vs already priced in>,
  "market_relevance": <0.0-1.0, how much does this matter to equity markets>,
  "time_urgency": <0.0-1.0, how time-sensitive is acting on this>,
  "cross_source_agreement": <0.0-1.0, how many independent sources confirm this>,
  "tradability": <0.0-1.0, how actionable is this with liquid equities/ETFs>,
  "expected_half_life_days": <integer, how many days until the signal loses half its value>,
  "directional_clarity": <0.0-1.0, how clear is the long/short direction>,
  "tickers": [
    {{"symbol": "<TICKER>", "direction": "long" or "short", "score": <-1.00 to +1.00>}}
  ],
  "do_not_trade": <true if confidence*tradability < 0.3 or directional_clarity < 0.4>,
  "reason": "<1-2 sentence summary of why this is or isn't tradable>"
}}

Rules:
- Include ALL tickers from both the LONG and SHORT candidate lists.
- LONG candidates get direction "long", SHORT candidates get direction "short".
- Re-score each ticker from -1.00 to +1.00 based on strength of evidence. Positive = long, negative = short. Be conservative; only beyond +/-0.70 if evidence is very strong.
- Set do_not_trade=true if the overall signal is too weak or unclear.

## Market Themes:
{themes_summary}

## Top LONG Candidates:
{long_candidates}

## Top SHORT Candidates:
{short_candidates}
"""

HISTORICAL_CONTEXT_PROMPT = """\
You are a senior geopolitical and market analyst. You have been given today's news \
themes AND a set of similar historical episodes retrieved from a 10-year news archive.

## Your task:
1. For each current storyline, compare it to the historical analogs provided.
2. Note which historical episodes are most similar and why.
3. Describe what happened to markets after each historical episode (if discernible from the headlines).
4. Assess whether the current situation is likely to follow a similar pattern or diverge.
5. Highlight any escalation/de-escalation signals.

## Format:
Return a structured markdown report with:
- For each storyline: Current vs Historical comparison table
- Market precedent assessment (what happened last time)
- Key differences from historical analogs
- Risk assessment (higher/lower risk than historical precedent)

## Current Market Themes:
{themes_summary}

## Current Storylines and Historical Analogs:
{storyline_analogs}
"""


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

def _call_ollama(system: str, user: str) -> str:
    """Send a chat request to Ollama and return the content string."""
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
    }
    resp = requests.post(OLLAMA_URL, json=payload, timeout=REQUEST_TIMEOUT_S)
    resp.raise_for_status()
    return (resp.json().get("message", {}).get("content", "") or "").strip()


def _parse_json_response(raw: str) -> dict | list | None:
    """Parse a JSON response, stripping markdown fences if present."""
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON from LLM: %s\nRaw: %s", e, text[:500])
        return None


# ---------------------------------------------------------------------------
# Loop 0: News digest
# ---------------------------------------------------------------------------

def _format_news(news_data: dict) -> str:
    """Format GDELT news data into markdown for prompts."""
    formatted = ""
    for topic, articles in news_data.items():
        label = topic.replace("_", " ").title()
        formatted += f"\n### {label}\n"
        if not articles:
            formatted += "No recent articles found.\n"
            continue
        for a in articles[:10]:
            formatted += f"- **{a['title']}** ({a['source']}, {a['date']})\n"
    return formatted


def _generate_news_digest(news_data: dict) -> str:
    """Loop 0: Summarize news into key market-moving themes (no stock picks)."""
    formatted_news = _format_news(news_data)
    prompt = NEWS_DIGEST_PROMPT.format(news_data=formatted_news)

    logger.info("Loop 0: Generating news digest...")
    result = _call_ollama(
        "You are a senior commodity and supply chain analyst.",
        prompt,
    )
    logger.info("Loop 0: News digest generated (%d chars)", len(result))
    return result


# ---------------------------------------------------------------------------
# Loop 0.5: Historical analog search
# ---------------------------------------------------------------------------


def _find_historical_analogs(news_data: dict) -> tuple[list[dict], str]:
    """Cluster today's articles into storylines, search for historical analogs.

    Returns (analog_results, formatted_context) where analog_results is a list
    of dicts (one per storyline) and formatted_context is markdown for the LLM.
    """
    # Flatten all articles
    all_articles = []
    for topic, articles in news_data.items():
        for a in articles:
            a["topic"] = topic
            all_articles.append(a)

    if not all_articles:
        return [], ""

    # Cluster into distinct storylines
    storylines = cluster_articles(all_articles, min_articles=2)
    logger.info("Found %d distinct storylines from %d articles", len(storylines), len(all_articles))

    if not storylines:
        return [], ""

    # Search historical analogs for each storyline (top 3 storylines max)
    analog_results = []
    for sl in storylines[:3]:
        fp = build_fingerprint(sl)
        logger.info("Searching history for storyline: %s", fp.label)

        try:
            result = search_similar_historical_incidents(
                fingerprint=fp,
                lookback_years=HISTORICAL_LOOKBACK_YEARS,
                top_k=HISTORICAL_TOP_K,
            )
            analog_results.append({
                "storyline": fp.label,
                "fingerprint": fp.to_dict(),
                "episodes": result["episodes"],
                "stats": result["stats"],
            })
        except Exception:
            logger.exception("Historical search failed for storyline: %s", fp.label)
            analog_results.append({
                "storyline": fp.label,
                "fingerprint": fp.to_dict(),
                "episodes": [],
                "stats": {"error": "search failed"},
            })

    # Format for LLM prompt
    formatted = _format_historical_context(analog_results)
    return analog_results, formatted


def _format_historical_context(analog_results: list[dict]) -> str:
    """Format historical analog results into markdown for LLM prompts."""
    if not analog_results:
        return "No historical analogs found."

    sections = []
    for result in analog_results:
        section = f"### Storyline: {result['storyline']}\n"
        section += f"**Actors:** {', '.join(result['fingerprint'].get('actors', []))}\n"
        section += f"**Countries:** {', '.join(result['fingerprint'].get('countries', []))}\n"
        section += f"**Event types:** {', '.join(result['fingerprint'].get('event_types', []))}\n\n"

        episodes = result.get("episodes", [])
        if not episodes:
            section += "*No similar historical episodes found.*\n"
        else:
            section += f"**{len(episodes)} similar historical episodes found:**\n\n"
            for i, ep in enumerate(episodes):
                section += f"{i + 1}. **{ep['title'][:100]}** ({ep['date_range']})\n"
                section += f"   - Similarity: {ep['similarity_score']}\n"
                section += f"   - Countries: {', '.join(ep.get('countries', []))}\n"
                section += f"   - Event types: {', '.join(ep.get('event_types', []))}\n"
                why = ep.get("why_matched", {})
                if why.get("country_overlap"):
                    section += f"   - Country overlap: {', '.join(why['country_overlap'])}\n"
                if why.get("event_type_overlap"):
                    section += f"   - Event overlap: {', '.join(why['event_type_overlap'])}\n"
                headlines = ep.get("sample_headlines", [])
                if headlines:
                    section += f"   - Headlines: {headlines[0][:80]}\n"
                section += "\n"

        sections.append(section)

    return "\n---\n\n".join(sections)


def _generate_historical_comparison(themes_summary: str, historical_context: str) -> str:
    """Use LLM to compare current events against historical analogs."""
    prompt = HISTORICAL_CONTEXT_PROMPT.format(
        themes_summary=themes_summary,
        storyline_analogs=historical_context,
    )

    logger.info("Loop 0.5: Generating historical comparison...")
    result = _call_ollama(
        "You are a senior geopolitical and market analyst with deep knowledge of historical events.",
        prompt,
    )
    logger.info("Loop 0.5: Historical comparison generated (%d chars)", len(result))
    return result


# ---------------------------------------------------------------------------
# Loop 1: Batch scoring
# ---------------------------------------------------------------------------

def _format_symbol_info(symbol_details: list[dict]) -> str:
    """Format a batch of symbol details for the scoring prompt."""
    lines = []
    for s in symbol_details:
        parts = [f"**{s['symbol']}**"]
        if s.get("description"):
            parts.append(f"Description: {s['description'][:300]}")
        if s.get("sector"):
            parts.append(f"Sector: {s['sector']}")
        if s.get("industry"):
            parts.append(f"Industry: {s['industry']}")
        if s.get("country"):
            parts.append(f"Country: {s['country']}")
        if s.get("next_earnings_date"):
            parts.append(f"Next Earnings: {s['next_earnings_date']}")
        if s.get("earnings_transcript"):
            transcript = str(s["earnings_transcript"])[:TRANSCRIPT_LIMIT]
            parts.append(f"Earnings Excerpt: {transcript}")
        lines.append("\n".join(parts))
    return "\n\n---\n\n".join(lines)


def _score_batch(themes_summary: str, batch: list[dict]) -> list[dict]:
    """Score a batch of symbols via tournament ranking. Returns scored dicts."""
    symbols_info = _format_symbol_info(batch)
    prompt = TOURNAMENT_RANK_PROMPT.format(
        n_symbols=len(batch),
        themes_summary=themes_summary,
        symbols_info=symbols_info,
    )

    symbols_in_batch = [s["symbol"] for s in batch]
    logger.info("Tournament: Ranking batch: %s", symbols_in_batch)

    raw = _call_ollama(
        "You output only valid JSON arrays. No markdown fences, no explanation.",
        prompt,
    )

    parsed = _parse_json_response(raw)
    if not isinstance(parsed, list):
        logger.warning("Tournament: Bad response for batch %s, skipping", symbols_in_batch)
        return []

    # Validate and normalize scores to [-1, +1]
    scored = []
    for item in parsed:
        if not isinstance(item, dict) or "symbol" not in item or "score" not in item:
            continue
        try:
            score = float(item["score"])
            score = round(max(-1.0, min(1.0, score)), 2)
        except (ValueError, TypeError):
            continue
        scored.append({
            "symbol": item["symbol"],
            "score": score,
            "reasoning": item.get("reasoning", ""),
        })
    return scored


def _pick_round1_winners(scored_batch: list[dict]) -> tuple[dict | None, dict | None]:
    """Extract the best long (highest score) and best short (lowest score) from a scored batch."""
    if not scored_batch:
        return None, None
    sorted_batch = sorted(scored_batch, key=lambda x: x["score"], reverse=True)
    best_long = sorted_batch[0] if sorted_batch[0]["score"] > 0.0 else None
    best_short = sorted_batch[-1] if sorted_batch[-1]["score"] < 0.0 else None
    return best_long, best_short


def _run_tournament(
    themes_summary: str, watchlist: list[dict]
) -> tuple[list[dict], list[dict], list[dict]]:
    """Run 2-round tournament. Returns (round1_all_scores, final_longs, final_shorts)."""
    detail_lookup = {s["symbol"]: s for s in watchlist}

    # Small watchlist shortcut: skip tournament, score all in one batch
    if len(watchlist) < 2 * ROUND1_BATCH_SIZE:
        logger.info("Tournament: Small watchlist (%d symbols), scoring in one batch", len(watchlist))
        scores = _score_batch(themes_summary, watchlist)
        sorted_scores = sorted(scores, key=lambda x: x["score"], reverse=True)
        longs = [s for s in sorted_scores if s["score"] > 0.0][:ROUND2_KEEP]
        shorts = [s for s in reversed(sorted_scores) if s["score"] < 0.0][:ROUND2_KEEP]
        return scores, longs, shorts

    # ── Round 1: Shuffle and batch ────────────────────────────────────────
    symbols = list(watchlist)
    rng = random.Random(int(TOURNAMENT_SEED) if TOURNAMENT_SEED else None)
    rng.shuffle(symbols)

    long_candidates = []
    short_candidates = []
    round1_all = []

    for i in range(0, len(symbols), ROUND1_BATCH_SIZE):
        batch = symbols[i : i + ROUND1_BATCH_SIZE]
        scored = _score_batch(themes_summary, batch)
        round1_all.extend(scored)

        best_long, best_short = _pick_round1_winners(scored)
        if best_long:
            long_candidates.append(best_long)
        if best_short:
            short_candidates.append(best_short)

        logger.info(
            "Round 1: Batch %d/%d done, %d long candidates, %d short candidates",
            i // ROUND1_BATCH_SIZE + 1,
            (len(symbols) + ROUND1_BATCH_SIZE - 1) // ROUND1_BATCH_SIZE,
            len(long_candidates),
            len(short_candidates),
        )

    logger.info(
        "Round 1 complete: %d long candidates, %d short candidates from %d symbols",
        len(long_candidates), len(short_candidates), len(round1_all),
    )

    # ── Round 2L: Re-rank long candidates ─────────────────────────────────
    final_longs = _run_round2(themes_summary, long_candidates, detail_lookup, "long")

    # ── Round 2S: Re-rank short candidates ────────────────────────────────
    final_shorts = _run_round2(themes_summary, short_candidates, detail_lookup, "short")

    return round1_all, final_longs, final_shorts


def _run_round2(
    themes_summary: str,
    candidates: list[dict],
    detail_lookup: dict[str, dict],
    direction: str,
) -> list[dict]:
    """Re-rank candidates in Round 2 batches, return top ROUND2_KEEP."""
    if not candidates:
        return []

    # Build enriched batches using full detail from watchlist
    enriched = []
    for c in candidates:
        detail = detail_lookup.get(c["symbol"])
        if detail:
            enriched.append(detail)
        else:
            # Fallback: use minimal info
            enriched.append({"symbol": c["symbol"]})

    all_rescored = []
    for i in range(0, len(enriched), ROUND2_BATCH_SIZE):
        batch = enriched[i : i + ROUND2_BATCH_SIZE]
        scored = _score_batch(themes_summary, batch)
        all_rescored.extend(scored)
        logger.info(
            "Round 2 (%s): Batch %d/%d done",
            direction,
            i // ROUND2_BATCH_SIZE + 1,
            (len(enriched) + ROUND2_BATCH_SIZE - 1) // ROUND2_BATCH_SIZE,
        )

    if direction == "long":
        # Keep top ROUND2_KEEP by highest score
        sorted_scores = sorted(all_rescored, key=lambda x: x["score"], reverse=True)
        return sorted_scores[:ROUND2_KEEP]
    else:
        # Keep bottom ROUND2_KEEP by lowest score
        sorted_scores = sorted(all_rescored, key=lambda x: x["score"])
        return sorted_scores[:ROUND2_KEEP]


# ---------------------------------------------------------------------------
# Loop 2: Final signal from top candidates
# ---------------------------------------------------------------------------


def _format_candidates(candidates: list[dict]) -> str:
    """Format candidate list for the final signal prompt."""
    lines = []
    for c in candidates:
        lines.append(
            f"- **{c['symbol']}** (score: {c['score']:+.2f}) — {c['reasoning']}"
        )
    return "\n".join(lines) if lines else "None"


def _generate_final_signal(
    themes_summary: str, longs: list[dict], shorts: list[dict]
) -> dict | None:
    """Loop 2: Generate the final trading signal JSON from top candidates."""
    prompt = FINAL_SIGNAL_PROMPT.format(
        n_long=len(longs),
        n_short=len(shorts),
        themes_summary=themes_summary,
        long_candidates=_format_candidates(longs),
        short_candidates=_format_candidates(shorts),
    )

    logger.info("Loop 2: Generating final signal from %d longs + %d shorts...",
                len(longs), len(shorts))

    raw = _call_ollama(
        "You output only valid JSON. No markdown fences, no explanation.",
        prompt,
    )

    signal = _parse_json_response(raw)
    if not isinstance(signal, dict):
        logger.error("Loop 2: Failed to parse final signal")
        return None

    signal["generated_at"] = datetime.now(timezone.utc).isoformat()
    return signal


# ---------------------------------------------------------------------------
# Theory file saving
# ---------------------------------------------------------------------------

def _save_theory(
    themes_summary: str,
    round1_scores: list[dict],
    longs: list[dict],
    shorts: list[dict],
    news_data: dict,
    historical_comparison: str = "",
) -> Path:
    """Save the full theory report as a markdown file."""
    THEORY_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    filename = f"theory_{now.strftime('%Y%m%d_%H%M%S')}.md"
    filepath = THEORY_DIR / filename

    header = f"# Supply Chain Disruption Theory\n\n"
    header += f"**Generated:** {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
    header += f"**Model:** {OLLAMA_MODEL}\n"
    header += f"**Data Source:** GDELT News API\n"
    header += f"**Symbols Scored (Round 1):** {len(round1_scores)}\n"
    header += f"**Score Range:** -1.00 to +1.00\n\n---\n\n"

    # Themes section
    body = "## Market Themes\n\n" + themes_summary + "\n\n---\n\n"

    # Top picks (Round 2 final)
    body += f"## Top {len(longs)} LONG Candidates (Round 2)\n\n"
    body += "| Ticker | Score | Reasoning |\n|--------|-------|-----------|\n"
    for c in longs:
        body += f"| {c['symbol']} | {c['score']:+.2f} | {c['reasoning']} |\n"

    body += f"\n## Top {len(shorts)} SHORT Candidates (Round 2)\n\n"
    body += "| Ticker | Score | Reasoning |\n|--------|-------|-----------|\n"
    for c in shorts:
        body += f"| {c['symbol']} | {c['score']:+.2f} | {c['reasoning']} |\n"

    # Round 1 scores
    body += "\n\n---\n\n## Round 1 Scores (All Symbols)\n\n"
    body += "| Ticker | Score | Reasoning |\n|--------|-------|-----------|\n"
    for s in sorted(round1_scores, key=lambda x: x["score"], reverse=True):
        body += f"| {s['symbol']} | {s['score']:+.2f} | {s['reasoning']} |\n"

    # Historical comparison
    if historical_comparison:
        body += "\n\n---\n\n## Historical Precedent Analysis\n\n"
        body += historical_comparison + "\n"

    # Source articles
    body += "\n\n---\n\n## Source Articles\n\n"
    for topic, articles in news_data.items():
        label = topic.replace("_", " ").title()
        body += f"\n### {label} ({len(articles)} articles)\n\n"
        if not articles:
            body += "No articles found.\n"
            continue
        for a in articles:
            title = a.get("title", "Untitled")
            source = a.get("source", "Unknown")
            date = a.get("date", "")
            url = a.get("url", "")
            if url:
                body += f"- [{title}]({url}) — *{source}*, {date}\n"
            else:
                body += f"- **{title}** — *{source}*, {date}\n"

    full_doc = header + body
    filepath.write_text(full_doc)

    latest = THEORY_DIR / "latest_theory.md"
    latest.write_text(full_doc)

    _cleanup_old_theories()

    logger.info("Theory saved to %s", filepath)
    return filepath


MAX_THEORIES = int(os.getenv("MAX_THEORIES", "5"))


def _cleanup_old_theories() -> None:
    """Remove old theory files, keeping only the most recent MAX_THEORIES."""
    theory_files = sorted(
        THEORY_DIR.glob("theory_*.md"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old_file in theory_files[MAX_THEORIES:]:
        old_file.unlink()
        logger.info("Removed old theory: %s", old_file.name)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

@register_tool(
    name="generate_supply_chain_theory",
    description=(
        "Scan GDELT news for supply chain disruptions (oil, sanctions, war, shipping, "
        "critical minerals), then score every watchlist symbol on how affected it is, "
        "and produce a trading signal with the top 20 longs and 20 shorts. "
        "Saves the theory as a markdown file and publishes the signal to Kafka."
    ),
    parameters={
        "type": "object",
        "properties": {
            "timespan": {
                "type": "string",
                "description": "How far back to scan news (e.g. '24h', '72h', '7d'). Default '72h'.",
            },
        },
        "required": [],
    },
)
def generate_supply_chain_theory(timespan: str = "72h") -> str:
    """Generate a full supply chain disruption theory using three-loop scoring."""
    THEORY_RUNS_TOTAL.inc()
    t0 = time.monotonic()

    try:
        return _run_theory_pipeline(timespan)
    except Exception:
        THEORY_ERRORS_TOTAL.inc()
        raise
    finally:
        THEORY_RUN_DURATION.set(time.monotonic() - t0)


def _run_theory_pipeline(timespan: str) -> str:
    """Inner pipeline logic, separated for metrics wrapping."""

    # ── Fetch data ────────────────────────────────────────────────────────
    watchlist = get_watchlist_details()
    logger.info("Watchlist: %d symbols loaded with details", len(watchlist))

    if not watchlist:
        return json.dumps({"status": "error", "message": "No watchlist symbols found in QuestDB."})

    logger.info("Fetching GDELT news (timespan=%s)...", timespan)
    news_data = fetch_all_topics(timespan=timespan)
    total_articles = sum(len(v) for v in news_data.values())
    if total_articles == 0:
        return json.dumps({"status": "error", "message": "No GDELT articles found. API may be down."})

    # ── Loop 0: News digest ───────────────────────────────────────────────
    themes_summary = _generate_news_digest(news_data)
    if not themes_summary:
        return json.dumps({"status": "error", "message": "LLM returned empty news digest."})

    # ── Loop 0.5: Historical analog search ────────────────────────────────
    historical_comparison = ""
    analog_results = []
    if ENABLE_HISTORICAL_ANALOGS:
        try:
            analog_results, historical_context = _find_historical_analogs(news_data)
            if historical_context:
                historical_comparison = _generate_historical_comparison(
                    themes_summary, historical_context
                )
                # Enrich themes with historical context for tournament scoring
                themes_summary += (
                    "\n\n---\n\n## Historical Precedent Analysis\n\n"
                    + historical_comparison
                )
                logger.info(
                    "Historical analogs: %d storylines, %d total episodes",
                    len(analog_results),
                    sum(len(r.get("episodes", [])) for r in analog_results),
                )
        except Exception:
            logger.exception("Historical analog search failed, continuing without it")

    # ── Loop 1: Tournament scoring ──────────────────────────────────────
    round1_scores, longs, shorts = _run_tournament(themes_summary, watchlist)
    if not round1_scores:
        return json.dumps({"status": "error", "message": "No symbols were scored. LLM may have failed."})

    # ── Loop 2: Generate final signal from tournament winners ─────────────
    signal = _generate_final_signal(themes_summary, longs, shorts)

    # ── Update metrics ────────────────────────────────────────────────────
    THEORY_ARTICLES_SCANNED.set(total_articles)
    THEORY_SYMBOLS_SCORED.set(len(round1_scores))

    # ── Save and publish ──────────────────────────────────────────────────
    filepath = _save_theory(
        themes_summary, round1_scores, longs, shorts, news_data,
        historical_comparison=historical_comparison,
    )

    kafka_published = False
    if signal:
        kafka_published = publish_signal(signal)
        if kafka_published:
            THEORY_SIGNAL_PUBLISHED_TOTAL.inc()
        THEORY_DO_NOT_TRADE.set(1 if signal.get("do_not_trade") else 0)
        logger.info(
            "Signal: theme=%s, do_not_trade=%s, tickers=%d",
            signal.get("theme"),
            signal.get("do_not_trade"),
            len(signal.get("tickers", [])),
        )
    else:
        logger.warning("Could not generate final trading signal")

    return json.dumps({
        "status": "success",
        "articles_scanned": total_articles,
        "symbols_scored": len(round1_scores),
        "longs_selected": len(longs),
        "shorts_selected": len(shorts),
        "historical_storylines": len(analog_results),
        "historical_episodes": sum(len(r.get("episodes", [])) for r in analog_results),
        "theory_file": str(filepath),
        "kafka_published": kafka_published,
        "signal": signal,
        "summary": themes_summary[:500] + "..." if len(themes_summary) > 500 else themes_summary,
    })
