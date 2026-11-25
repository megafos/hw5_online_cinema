import pytest
import requests
import time
from uuid import uuid4


def test_event_reaches_clickhouse():
    timestamp_ms = int(time.time() * 1000)
    event_id = str(uuid4())

    start_event = {
        "event_id": event_id,
        "user_id": "user_e2e_test",
        "movie_id": "movie_e2e_test",
        "event_type": "VIEW_STARTED",
        "timestamp": timestamp_ms,
        "device_type": "DESKTOP",
        "session_id": "session_e2e_test",
        "progress_seconds": 0
    }

    response = requests.post("http://localhost:5000/events", json=start_event, timeout=15)

    assert response.status_code == 202, response.text
    assert response.json()["event_id"] == event_id


    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:      
        response = requests.post(
            "http://localhost:8123/",
            data=f"""
                SELECT event_id, user_id, movie_id, event_type, 
                toUnixTimestamp64Milli(timestamp) as timestamp_ms, device_type, session_id, progress_seconds
                FROM cinema.movie_events
                WHERE event_id = '{event_id}'
                FORMAT JSON
            """,
            auth=("cinema", "cinema_dev_password"),
            timeout=5
        )

        assert response.status_code == 200, response.text

        rows = response.json()["data"]

        if rows:
            row = rows[0]

            assert row["event_id"] == event_id
            assert row["user_id"] == start_event["user_id"]
            assert row["movie_id"] == start_event["movie_id"]
            assert row["event_type"] == start_event["event_type"]
            assert int(row["timestamp_ms"]) == start_event["timestamp"]
            assert row["device_type"] == start_event["device_type"]
            assert row["session_id"] == start_event["session_id"]
            assert row["progress_seconds"] == start_event["progress_seconds"]

            return

        time.sleep(0.5)

    pytest.fail(f"Event {event_id} was not found in ClickHouse within 30 seconds")


def test_invalid_event():
    timestamp_ms = int(time.time() * 1000)
    event_id = str(uuid4())

    start_event = {
        "event_id": event_id,
        "user_id": "user_e2e_test",
        "movie_id": "movie_e2e_test",
        "event_type": "VIEW_STARTED",
        "timestamp": timestamp_ms,
        "device_type": "DESKTOP",
        "session_id": "session_e2e_test",
        "progress_seconds": 0
    }

    start_event["event_type"] = "INCORRECT"

    response = requests.post("http://localhost:5000/events", json=start_event, timeout=15)

    assert response.status_code == 400, response.text


@pytest.mark.parametrize("invalid_progress", [-1, -100, -1000])
def test_negative_progress(invalid_progress):
    timestamp_ms = int(time.time() * 1000)
    event_id = str(uuid4())

    start_event = {
        "event_id": event_id,
        "user_id": "user_e2e_test",
        "movie_id": "movie_e2e_test",
        "event_type": "VIEW_STARTED",
        "timestamp": timestamp_ms,
        "device_type": "DESKTOP",
        "session_id": "session_e2e_test",
        "progress_seconds": invalid_progress
    }

    response = requests.post("http://localhost:5000/events", json=start_event, timeout=15)

    assert response.status_code == 400, response.text


def test_missing_required_field():
    timestamp_ms = int(time.time() * 1000)
    event_id = str(uuid4())

    start_event = {
        "event_id": event_id,
        "user_id": "user_e2e_test",
        "movie_id": "movie_e2e_test",
        "event_type": "VIEW_STARTED",
        "timestamp": timestamp_ms,
        "device_type": "DESKTOP",
        "session_id": "session_e2e_test",
        "progress_seconds": 0
    }

    start_event.pop("user_id")

    response = requests.post("http://localhost:5000/events", json=start_event, timeout=15)

    assert response.status_code == 400, response.text


@pytest.mark.parametrize("event_type", ["VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED"])
@pytest.mark.parametrize("missing", [True, False], ids=["missing", "null"])
def test_view_requires_progress(event_type, missing):
    event = {
        "event_id": str(uuid4()),
        "user_id": "user_validation_test",
        "movie_id": "movie_validation_test",
        "event_type": event_type,
        "timestamp": int(time.time() * 1000),
        "device_type": "DESKTOP",
        "session_id": str(uuid4()),
    }
    if not missing:
        event["progress_seconds"] = None
    response = requests.post("http://localhost:5000/events", json=event, timeout=15)
    assert response.status_code == 400, response.text
    assert "progress_seconds" in response.json()["error"]
