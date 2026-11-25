import os
import sys
import logging
from flask import Flask, request, jsonify
from uuid import UUID
from producer import send_event
from confluent_kafka import KafkaException


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout
)

app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/events")
def create_event():
    event = request.get_json(silent=True)
    if not isinstance(event, dict):
        return jsonify({"error": "Expected a JSON object"}), 400

    required_fields = {
        "event_id",
        "user_id",
        "movie_id",
        "event_type",
        "timestamp",
        "device_type",
        "session_id"
    }

    missing_fields = required_fields - event.keys()

    if missing_fields:
        return jsonify({
            "error": "Missing required fields",
            "fields": sorted(missing_fields)
        }), 400

    allowed_event_types = {
        "VIEW_STARTED",
        "VIEW_FINISHED",
        "VIEW_PAUSED",
        "VIEW_RESUMED",
        "LIKED",
        "SEARCHED"
    }

    if not isinstance(event["event_type"], str) or event["event_type"] not in allowed_event_types:
        return jsonify({"error": "Invalid event_type"}), 400

    allowed_device_types = {
        "MOBILE",
        "DESKTOP",
        "TV",
        "TABLET"
    }

    if not isinstance(event["device_type"], str) or event["device_type"] not in allowed_device_types:
        return jsonify({"error": "Invalid device_type"}), 400

    string_fields = ["user_id", "movie_id", "session_id"]
    for field in string_fields:
        value = event[field]
        if not isinstance(value, str) or value.strip() == "":
            return jsonify({"error": f"Invalid {field}"}), 400

    if type(event["timestamp"]) is not int or event["timestamp"] < 0 or event["timestamp"] > 2**63 - 1:
        return jsonify({"error": "Invalid timestamp"}), 400

    progress = event.get("progress_seconds")

    if event["event_type"].startswith("VIEW_") and progress is None:
        return jsonify({
            "error": "progress_seconds is required for viewing events"
        }), 400

    if progress is not None:
        if type(progress) is not int or progress < 0 or progress > 2**31 - 1:
            return jsonify({"error": "Invalid progress_seconds"}), 400

    if not isinstance(event["event_id"], str):
        return jsonify({"error": "Invalid event_id"}), 400

    try:
        UUID(event["event_id"])
    except ValueError:
        return jsonify({"error": "Invalid event_id"}), 400

    try:
        send_event(event)
    except (TimeoutError, RuntimeError, KafkaException, BufferError):
        app.logger.exception(
            "Kafka publication failed: event_id=%s",
            event["event_id"]
        )
        return jsonify({"error": "Kafka publication failed"}), 503

    return jsonify({
        "status": "published",
        "event_id": event["event_id"]
    }), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)