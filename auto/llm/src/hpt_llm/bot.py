import logging

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from .config import PROMETHEUS_PORT, SLACK_APP_TOKEN, SLACK_BOT_TOKEN
from .llm import chat
from .metrics import SLACK_MESSAGES_TOTAL, SLACK_RESPONSE_ERRORS_TOTAL, start_metrics_server
from .scheduler import start_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = App(token=SLACK_BOT_TOKEN)


def _clean_mention(text: str) -> str:
    """Remove the leading Slack user mention token, if present."""
    if not text:
        return ""
    if ">" in text:
        return text.split(">", 1)[-1].strip()
    return text.strip()


@app.event("app_mention")
def handle_mention(event, say):
    """Respond when @mentioned in a channel."""
    SLACK_MESSAGES_TOTAL.labels(type="mention").inc()
    try:
        user_message = _clean_mention(event.get("text", ""))
        if not user_message:
            say("Send me a message after the mention and I'll reply.")
            return
        say(chat(user_message))
    except Exception as exc:
        SLACK_RESPONSE_ERRORS_TOTAL.inc()
        logger.exception("Error handling app mention")
        say(f"Error handling mention: {exc}")


@app.event("message")
def handle_dm(event, say):
    """Respond to direct messages only."""
    try:
        if event.get("bot_id") or event.get("subtype"):
            return
        if event.get("channel_type") != "im":
            return
        text = (event.get("text") or "").strip()
        if not text:
            return
        SLACK_MESSAGES_TOTAL.labels(type="dm").inc()
        say(chat(text))
    except Exception as exc:
        SLACK_RESPONSE_ERRORS_TOTAL.inc()
        logger.exception("Error handling DM")
        say(f"Error handling DM: {exc}")


def main():
    start_metrics_server(PROMETHEUS_PORT)
    start_scheduler()
    logger.info("Bot is running! Ctrl+C to stop.")
    SocketModeHandler(app, SLACK_APP_TOKEN).start()


if __name__ == "__main__":
    main()
