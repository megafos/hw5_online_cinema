import random
import time
from uuid import uuid4


def generate_viewing_session(
    user_id: str,
    movie_id: str,
    device_type: str = "DESKTOP"
) -> list[dict]:
    start_offset_seconds = random.randint(4 * 3600, 24 * 3600)
    timestamp_ms = int(time.time() * 1000) - start_offset_seconds * 1000
    session_id = str(uuid4())

    duration_seconds = random.randint(3600, 3600 * 2)

    has_pause = random.random() < 0.5
    pause_at = random.randint(60, duration_seconds - 60)
    pause_duration = random.randint(1, 600)

    is_completed = random.random() < 0.8

    start_event = {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": "VIEW_STARTED",
        "timestamp": timestamp_ms,
        "device_type": device_type,
        "session_id": session_id,
        "progress_seconds": 0
    }

    if not is_completed:
        stopped_at = random.randint(1, duration_seconds - 1)

        stopped_event = {
            "event_id": str(uuid4()),
            "user_id": user_id,
            "movie_id": movie_id,
            "event_type": "VIEW_PAUSED",
            "timestamp": timestamp_ms + stopped_at * 1000,
            "device_type": device_type,
            "session_id": session_id,
            "progress_seconds": stopped_at
        }

        return [start_event, stopped_event]

    next_timestamp = timestamp_ms + pause_at * 1000

    pause_event = {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": "VIEW_PAUSED",
        "timestamp": next_timestamp,
        "device_type": device_type,
        "session_id": session_id,
        "progress_seconds": pause_at
    }

    next_timestamp = timestamp_ms + (pause_at + pause_duration) * 1000

    resume_event = {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": "VIEW_RESUMED",
        "timestamp": next_timestamp,
        "device_type": device_type,
        "session_id": session_id,
        "progress_seconds": pause_at
    }

    extra_time = pause_duration if has_pause else 0
    next_timestamp = timestamp_ms + (duration_seconds + extra_time) * 1000

    finish_event = {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": "VIEW_FINISHED",
        "timestamp": next_timestamp,
        "device_type": device_type,
        "session_id": session_id,
        "progress_seconds": duration_seconds
    }

    events = [start_event]

    if has_pause:
        events.append(pause_event)
        events.append(resume_event)

    events.append(finish_event)

    return events


def generate_user_activity(
    user_id: str,
    movie_id: str,
    device_type: str = "DESKTOP"
) -> list[dict]:
    events = generate_viewing_session(
        user_id,
        movie_id,
        device_type
    )

    has_search = random.random() < 0.7
    if has_search:
        search_event = events[0].copy()
        search_event.update({
            "event_id": str(uuid4()),
            "event_type": "SEARCHED",
            "timestamp": events[0]["timestamp"] - random.randint(10, 600) * 1000,
            "progress_seconds": None
        })

        events.insert(0, search_event)

    has_like = random.random() < 0.6

    is_finished = any(
        event["event_type"] == "VIEW_FINISHED"
        for event in events
    )

    if is_finished and has_like:
        liked_event = events[-1].copy()
        liked_event.update({
            "event_id": str(uuid4()),
            "event_type": "LIKED",
            "timestamp": events[-1]["timestamp"] + random.randint(10, 600) * 1000,
            "progress_seconds": None
        })

        events.append(liked_event)


    return events
