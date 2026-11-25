import requests
from datetime import datetime, timedelta, timezone, time as dt_time
from uuid import uuid4


def create_retention_event(user_id: str, event_date) -> dict:
    event_datetime = datetime.combine(event_date, dt_time(12, 0), tzinfo=timezone.utc)

    timestamp_ms = int(event_datetime.timestamp() * 1000)

    event = {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "movie_id": "test_movie",
        "event_type": "SEARCHED",
        "timestamp": timestamp_ms,
        "device_type": "DESKTOP",
        "session_id": str(uuid4()),
        "progress_seconds": None
    }

    return event


first_day = datetime.now(timezone.utc).date() - timedelta(days=14)
d1_day = first_day + timedelta(days=1)
d7_day = first_day + timedelta(days=7)

users_test_id = str(uuid4())
users = []
for i in range(3):
    users.append(f"retention_user_{i}_{users_test_id}")


events = [
    create_retention_event(users[0], first_day),
    create_retention_event(users[1], first_day),
    create_retention_event(users[2], first_day),
    create_retention_event(users[0], d1_day),
    create_retention_event(users[1], d1_day),
    create_retention_event(users[0], d7_day)
]

for event in events:
    response = requests.post("http://localhost:5000/events", json=event, timeout=15)
    
    assert response.status_code == 202, response.text


assert len(users) == 3
assert len(set(users)) == 3
assert len(events) == 6

print(f"Retention test ID: {users_test_id}")
print(f"Successfully published: {len(events)} events")