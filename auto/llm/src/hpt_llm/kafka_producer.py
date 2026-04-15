import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_OUTPUT_TOPIC
from .metrics import KAFKA_PUBLISH_ERRORS_TOTAL

logger = logging.getLogger(__name__)

_producer: KafkaProducer | None = None


def _get_producer() -> KafkaProducer:
    """Lazy-init a singleton KafkaProducer."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="PLAINTEXT",
            acks="all",
            compression_type="gzip",
            linger_ms=5,
            retries=3,
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
    return _producer


def publish_signal(message: dict, topic: str | None = None) -> bool:
    """Publish a news-signal message to Kafka. Returns True on success."""
    topic = topic or KAFKA_OUTPUT_TOPIC
    try:
        producer = _get_producer()
        producer.send(topic, message)
        producer.flush()
        logger.info("Published signal to %s: theme=%s", topic, message.get("theme", "?"))
        return True
    except KafkaError as e:
        KAFKA_PUBLISH_ERRORS_TOTAL.inc()
        logger.error("Kafka error publishing signal: %s", e, exc_info=True)
        return False
    except Exception as e:
        KAFKA_PUBLISH_ERRORS_TOTAL.inc()
        logger.error("Unexpected error publishing signal: %s", e, exc_info=True)
        return False
