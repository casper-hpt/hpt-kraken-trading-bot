import logging
import time
from typing import Any

import requests

from . import tools
from .config import MAX_SLACK_REPLY_LEN, OLLAMA_MODEL, OLLAMA_URL, REQUEST_TIMEOUT_S
from .metrics import (
    OLLAMA_CALL_DURATION,
    OLLAMA_CALLS_TOTAL,
    OLLAMA_ERRORS_TOTAL,
    TOOL_CALLS_TOTAL,
    TOOL_DURATION,
    TOOL_ERRORS_TOTAL,
)

logger = logging.getLogger(__name__)


def _build_system_prompt() -> str:
    return (
        "You are a helpful Slack assistant running on a local Ollama model. "
        "Be concise, accurate, and practical. "
        "Use tools when needed. "
        "If tool output is missing something, say exactly what is missing."
    )


def _call_ollama(messages: list[dict[str, Any]]) -> dict[str, Any]:
    """Send a chat request to Ollama and return the assistant message."""
    payload = {
        "model": OLLAMA_MODEL,
        "messages": messages,
        "tools": tools.get_ollama_tools(),
        "stream": False,
    }

    logger.info("Calling Ollama model=%s", OLLAMA_MODEL)
    OLLAMA_CALLS_TOTAL.inc()
    t0 = time.monotonic()
    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
    except Exception:
        OLLAMA_ERRORS_TOTAL.inc()
        raise
    finally:
        OLLAMA_CALL_DURATION.set(time.monotonic() - t0)

    data = resp.json()

    if "message" not in data:
        raise ValueError(f"Unexpected Ollama response: {data}")

    return data["message"]


def _execute_tool_calls(tool_calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Execute each tool call and return tool-role messages."""
    results = []
    for tc in tool_calls:
        fn = tc.get("function", {})
        fn_name = fn.get("name", "")
        fn_args = fn.get("arguments", {}) or {}

        logger.info("Executing tool: %s args=%s", fn_name, fn_args)
        TOOL_CALLS_TOTAL.labels(tool=fn_name).inc()
        t0 = time.monotonic()
        try:
            result = tools.execute_tool(fn_name, fn_args)
        except Exception:
            TOOL_ERRORS_TOTAL.labels(tool=fn_name).inc()
            raise
        finally:
            TOOL_DURATION.labels(tool=fn_name).set(time.monotonic() - t0)

        results.append({"role": "tool", "name": fn_name, "content": result})
    return results


def _truncate(text: str, limit: int = MAX_SLACK_REPLY_LEN) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def chat(user_message: str) -> str:
    """Send a message through the LLM, handle tool calls, return final text."""
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _build_system_prompt()},
        {"role": "user", "content": user_message},
    ]

    reply = _call_ollama(messages)
    messages.append(reply)

    tool_calls = reply.get("tool_calls") or []
    if tool_calls:
        messages.extend(_execute_tool_calls(tool_calls))
        reply = _call_ollama(messages)
        messages.append(reply)

    content = (reply.get("content") or "").strip()
    if not content:
        content = "I got your message, but I returned an empty response."

    return _truncate(content)
