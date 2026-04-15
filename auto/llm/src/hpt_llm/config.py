import os

from dotenv import load_dotenv

load_dotenv()

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://192.168.2.38:11434/api/chat")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b-instruct")

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN = os.environ["SLACK_APP_TOKEN"]

REQUEST_TIMEOUT_S = int(os.getenv("REQUEST_TIMEOUT_S", "120"))
MAX_SLACK_REPLY_LEN = int(os.getenv("MAX_SLACK_REPLY_LEN", "3500"))

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_PRODUCER_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "news-signals")

# QuestDB (PGWire)
QUESTDB_HOST = os.getenv("DB_HOST", "questdb")
QUESTDB_PORT = int(os.getenv("DB_PORT", "8812"))
QUESTDB_USER = os.getenv("DB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("DB_PASSWORD", "quest")
QUESTDB_DATABASE = os.getenv("DB_NAME", "qdb")

# Prometheus
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9096"))

# Scheduler
THEORY_CRON_HOUR = int(os.getenv("THEORY_CRON_HOUR", "2"))
THEORY_CRON_MINUTE = int(os.getenv("THEORY_CRON_MINUTE", "0"))
THEORY_SCAN_TIMESPAN = os.getenv("THEORY_SCAN_TIMESPAN", "24h")

# Crypto event classifier
CRYPTO_EVENT_INTERVAL_MINUTES = int(os.getenv("CRYPTO_EVENT_INTERVAL_MINUTES", "30"))
CRYPTO_EVENT_TIMESPAN = os.getenv("CRYPTO_EVENT_TIMESPAN", "4h")
CRYPTO_EVENT_KAFKA_TOPIC = os.getenv("CRYPTO_EVENT_KAFKA_TOPIC", "crypto-event-signals")
