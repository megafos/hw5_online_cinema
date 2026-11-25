import os
import time
import psycopg
import pytest
import requests
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4


MOVIE_SERVICE_URL = os.getenv("MOVIE_SERVICE_URL", "http://localhost:5000").rstrip("/")
AGGREGATION_SERVICE_URL = os.getenv("AGGREGATION_SERVICE_URL", "http://localhost:5001").rstrip("/")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_AUTH = (
    os.getenv("CLICKHOUSE_USER", "cinema"),
    os.getenv("CLICKHOUSE_PASSWORD", "cinema_dev_password"),
)


def clickhouse_query(query: str, **params) -> list[dict]:
    response = requests.post(
        CLICKHOUSE_URL,
        data=query + "\nFORMAT JSON",
        params={f"param_{name}": value for name, value in params.items()},
        auth=CLICKHOUSE_AUTH,
        timeout=15,
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]


def postgres_query(query: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "cinema_analytics"),
        user=os.getenv("POSTGRES_USER", "analytics"),
        password=os.getenv("POSTGRES_PASSWORD", "analytics_dev_password"),
        connect_timeout=5,
    ) as connection:
        return connection.execute(query, params).fetchall()


def find_free_cohort_date() -> date:
    ch_days = clickhouse_query("""
        SELECT DISTINCT toDate(timestamp) AS day FROM cinema.movie_events
        UNION DISTINCT SELECT metric_date AS day FROM cinema.daily_aggregates
        UNION DISTINCT SELECT metric_date AS day FROM cinema.movie_aggregates
        UNION DISTINCT SELECT cohort_date AS day FROM cinema.retention_aggregates
    """)
    pg_days = postgres_query("""
        SELECT metric_date FROM daily_metrics
        UNION SELECT metric_date FROM movie_metrics
        UNION SELECT cohort_date FROM retention_metrics
    """)
    occupied = {date.fromisoformat(row["day"]) for row in ch_days}
    occupied.update(row[0] for row in pg_days)

    today = datetime.now(timezone.utc).date()
    for offset in range(30, 366):
        candidate = today - timedelta(days=offset)
        if all(candidate + timedelta(days=i) not in occupied for i in range(-1, 8)):
            return candidate

    pytest.fail("No unused D-1..D7 interval found in the last year")


def create_event(user_id, movie_id, session_id, event_type, event_time, progress):
    return {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": event_type,
        "timestamp": int(event_time.timestamp() * 1000),
        "device_type": "DESKTOP",
        "session_id": session_id,
        "progress_seconds": progress,
    }


def publish_events(events: list[dict]) -> None:
    for event in events:
        response = requests.post(
            f"{MOVIE_SERVICE_URL}/events", json=event, timeout=15
        )
        assert response.status_code == 202, response.text
        assert response.json()["event_id"] == event["event_id"]


def wait_for_events(events: list[dict], cohort_date: date) -> None:
    expected = Counter(event["event_id"] for event in events)
    deadline = time.monotonic() + 60
    actual = Counter()
    while time.monotonic() < deadline:
        rows = clickhouse_query("""
            SELECT event_id FROM cinema.movie_events
            WHERE toDate(timestamp) >= {start:Date}
              AND toDate(timestamp) <= {end:Date}
        """, start=(cohort_date - timedelta(days=1)).isoformat(),
            end=(cohort_date + timedelta(days=7)).isoformat())
        actual = Counter(row["event_id"] for row in rows)
        assert not (actual - expected), f"Unexpected or duplicate events: {actual - expected}"
        if actual == expected:
            return
        time.sleep(0.5)
    pytest.fail(f"Events not received in ClickHouse: {expected - actual}")


def run_aggregation(metric_date: date) -> None:
    deadline = time.monotonic() + 30
    while True:
        response = requests.post(
            f"{AGGREGATION_SERVICE_URL}/aggregate",
            json={"date": metric_date.isoformat()},
            timeout=90,
        )
        if response.status_code != 409 or time.monotonic() >= deadline:
            break
        time.sleep(0.5)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "success", "metric_date": metric_date.isoformat()
    }


def decimal_value(value):
    return None if value is None else Decimal(str(value))


def read_metrics(metric_date: date, expected: dict) -> dict:
    tables = (
        ("daily", "daily_aggregates", "daily_metrics", "metric_date", ("metric_name",)),
        ("movie", "movie_aggregates", "movie_metrics", "metric_date", ("movie_id", "metric_name")),
        ("retention", "retention_aggregates", "retention_metrics", "cohort_date", ("metric_name",)),
    )
    versions = {}
    for group, ch_table, pg_table, date_column, key_columns in tables:
        keys = ", ".join(key_columns)
        ch_rows = clickhouse_query(f"""
            SELECT {keys}, toString(metric_value) AS metric_value, computed_at
            FROM cinema.{ch_table} FINAL
            WHERE {date_column} = {{day:Date}}
        """, day=metric_date.isoformat())
        pg_rows = postgres_query(f"""
            SELECT {keys}, metric_value, computed_at
            FROM {pg_table}
            WHERE {date_column} = %s
        """, (metric_date,))

        ch_values = {
            tuple(row[key] for key in key_columns): decimal_value(row["metric_value"])
            for row in ch_rows
        }
        pg_values = {tuple(row[:-2]): decimal_value(row[-2]) for row in pg_rows}
        target = expected[group]

        assert len(ch_rows) == len(target), f"{ch_table}: incorrect row count"
        assert len(pg_rows) == len(target), f"{pg_table}: incorrect row count"
        assert ch_values == target, f"{ch_table}: unexpected metrics"
        assert pg_values == target, f"{pg_table}: unexpected metrics"
        assert ch_values == pg_values

        versions[("clickhouse", group)] = {
            tuple(row[key] for key in key_columns): row["computed_at"] for row in ch_rows
        }
        versions[("postgres", group)] = {tuple(row[:-2]): row[-1] for row in pg_rows}
    return versions


def test_aggregation_e2e():
    cohort_date = find_free_cohort_date()
    run_id = uuid4().hex
    users = [f"aggregation_user_{i}_{run_id}" for i in range(3)]
    movie_a, movie_b = (f"aggregation_movie_{i}_{run_id}" for i in range(2))
    sessions = [str(uuid4()) for _ in users]
    d0 = datetime.combine(cohort_date, datetime.min.time(), tzinfo=timezone.utc)
    d0 += timedelta(hours=12)

    events = [
        create_event(users[0], movie_a, str(uuid4()), "SEARCHED", d0 - timedelta(days=1), None),
        create_event(f"search_only_{run_id}", movie_a, str(uuid4()), "SEARCHED", d0, None),
        create_event(users[0], movie_a, sessions[0], "VIEW_STARTED", d0, 0),
        create_event(users[0], movie_a, sessions[0], "VIEW_FINISHED", d0 + timedelta(seconds=3600), 3600),
        create_event(users[1], movie_b, sessions[1], "VIEW_STARTED", d0, 0),
        create_event(users[1], movie_b, sessions[1], "VIEW_FINISHED", d0 + timedelta(seconds=5400), 5400),
        create_event(users[2], movie_a, sessions[2], "VIEW_STARTED", d0, 0),
        create_event(users[2], movie_a, sessions[2], "VIEW_PAUSED", d0 + timedelta(seconds=900), 900),
        create_event(users[0], movie_a, str(uuid4()), "SEARCHED", d0 + timedelta(days=1), None),
        create_event(users[1], movie_b, str(uuid4()), "SEARCHED", d0 + timedelta(days=1), None),
        create_event(users[0], movie_a, str(uuid4()), "SEARCHED", d0 + timedelta(days=7), None),
    ]
    events.sort(key=lambda event: event["timestamp"])

    expected = {
        "daily": {
            ("dau",): Decimal("4"),
            ("started_views",): Decimal("3"),
            ("completed_views",): Decimal("2"),
            ("avg_watch_seconds",): Decimal("4500"),
            ("completion_rate",): Decimal("66.67"),
        },
        "movie": {
            (movie_a, "started_views"): Decimal("2"),
            (movie_a, "completed_views"): Decimal("1"),
            (movie_b, "started_views"): Decimal("1"),
            (movie_b, "completed_views"): Decimal("1"),
            (movie_a, "view_rank"): Decimal("1"),
            (movie_b, "view_rank"): Decimal("2"),
        },
        "retention": {
            ("cohort_size",): Decimal("3"),
            ("retained_d1",): Decimal("2"),
            ("retained_d7",): Decimal("1"),
            ("retention_d1",): Decimal("66.67"),
            ("retention_d7",): Decimal("33.33"),
        },
    }

    print(f"Aggregation E2E: cohort_date={cohort_date}, run_id={run_id}")
    publish_events(events)
    wait_for_events(events, cohort_date)

    run_aggregation(cohort_date)
    first_versions = read_metrics(cohort_date, expected)

    run_aggregation(cohort_date)
    second_versions = read_metrics(cohort_date, expected)

    for group, versions in first_versions.items():
        for key, first_version in versions.items():
            assert second_versions[group][key] > first_version, (
                f"{group}: computed_at did not advance for {key}"
            )

