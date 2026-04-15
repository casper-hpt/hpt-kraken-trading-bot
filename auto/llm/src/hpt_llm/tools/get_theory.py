import json
import logging
import os
from pathlib import Path

from .base import register_tool

logger = logging.getLogger(__name__)

THEORY_DIR = Path(os.getenv("THEORY_DIR", "/app/theories"))


@register_tool(
    name="get_latest_theory",
    description=(
        "Retrieve the latest supply chain disruption theories. Returns the full markdown "
        "theory reports including disruptions, price predictions, and risk analysis. "
        "Use this when someone asks about supply chain issues, price predictions, or "
        "the current theory. Supports fetching the last N theories (default 1)."
    ),
    parameters={
        "type": "object",
        "properties": {
            "count": {
                "type": "integer",
                "description": "Number of recent theories to return (default 1, max 5).",
            },
        },
        "required": [],
    },
)
def get_latest_theory(count: int = 1) -> str:
    """Return the contents of the last N saved theories."""
    count = max(1, min(int(count), 5))

    theory_files = sorted(
        THEORY_DIR.glob("theory_*.md"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not theory_files:
        return json.dumps({
            "status": "no_theory",
            "message": (
                "No theory has been generated yet. "
                "Use the generate_supply_chain_theory tool to create one first."
            ),
        })

    theories = []
    for f in theory_files[:count]:
        theories.append({
            "filename": f.name,
            "last_modified": str(f.stat().st_mtime),
            "theory": f.read_text(),
        })

    return json.dumps({
        "status": "success",
        "count": len(theories),
        "theories": theories,
    })
