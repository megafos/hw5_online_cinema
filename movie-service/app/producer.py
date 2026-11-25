import os
import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pathlib import Path


logger = logging.getLogger(__name__)

bootstrap_servers = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", 
    "localhost:9092"
)

schema_registry_url = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://localhost:8081"
)

producer = Producer({
    "bootstrap.servers": bootstrap_servers,
    "acks": "all",
    "enable.idempotence": True,
    "retries": 10,
    "retry.backoff.ms": 200,
    "retry.backoff.max.ms": 5000
})

schema_registry_client = SchemaRegistryClient({
    "url": schema_registry_url
})

schema_path = os.getenv(
    "SCHEMA_PATH", 
    "schemas/movie_event.avsc"
)

schema_text = Path(schema_path).read_text(encoding="utf-8")

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_text,
    conf={
        "auto.register.schemas": False
    }
)


def serialize_event(event: dict) -> bytes:
    topic = "movie-events"

    context = SerializationContext(
        topic,
        MessageField.VALUE
    )

    event_data = event.copy()
    event_data.setdefault("progress_seconds", None)

    serialized_value = avro_serializer(event_data, context)

    return serialized_value


def send_event(event: dict):
    serialized_value = serialize_event(event)

    delivery = {
        "confirmed": False,
        "error": None
    }

    def on_delivery(err, msg):
        delivery["confirmed"] = err is None
        delivery["error"] = err
        if err is not None:
            logger.error(
                "Kafka delivery failed: event_id=%s, event_type=%s, timestamp=%s, error=%s",
                event["event_id"],
                event["event_type"],
                event["timestamp"],
                err
            )
        else:
            logger.info(
                "Kafka event published: event_id=%s, event_type=%s, timestamp=%s, partition=%s, offset=%s",
                event["event_id"],
                event["event_type"],
                event["timestamp"],
                msg.partition(),
                msg.offset()
            )

    producer.produce(
        topic="movie-events",
        key=event["user_id"],
        value=serialized_value,
        on_delivery=on_delivery
    )

    remaining = producer.flush(timeout=10)

    if remaining > 0:
        raise TimeoutError("Kafka delivery timed out")

    if delivery["error"] is not None:
        raise RuntimeError(f"Kafka delivery failed: {delivery['error']}")

    if not delivery["confirmed"]:
        raise RuntimeError("Kafka delivery was not confirmed")
