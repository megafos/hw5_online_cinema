import requests
import os
import json
from datetime import date


def get_dau(metric_date: date) -> int:
    CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema_dev_password")

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    query = """
            SELECT uniqExact(user_id) AS dau 
            FROM cinema.movie_events 
            WHERE toDate(timestamp) = {metric_date:Date} 
            FORMAT JSON
            """
    
    params = {
        "param_metric_date": metric_date.isoformat()
    }
    
    response = requests.post(CLICKHOUSE_URL, data=query, params=params, auth=auth, timeout=15)

    response.raise_for_status()

    rows = response.json()["data"]
    dau = int(rows[0]["dau"])

    return dau


def get_daily_metrics(metric_date: date) -> dict:
    CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema_dev_password")

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    query = """
            SELECT 
                uniqExact(user_id) AS dau,
                countIf(event_type = 'VIEW_STARTED') as started_views,
                countIf(event_type = 'VIEW_FINISHED') as completed_views,
                if (completed_views > 0, round(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 2), NULL) as avg_watch_seconds,
                round(100.0 * completed_views / nullIf(started_views, 0), 2) as completion_rate
            FROM cinema.movie_events 
            WHERE toDate(timestamp) = {metric_date:Date} 
            FORMAT JSON
            """
    
    params = {
        "param_metric_date": metric_date.isoformat()
    }
    
    response = requests.post(CLICKHOUSE_URL, data=query, params=params, auth=auth, timeout=15)

    response.raise_for_status()

    row = response.json()["data"][0]
    dau = int(row["dau"])
    started_views = int(row["started_views"])
    completed_views = int(row["completed_views"])
    avg_watch_seconds = float(row["avg_watch_seconds"]) if row["avg_watch_seconds"] is not None else None
    completion_rate = float(row["completion_rate"]) if row["completion_rate"] is not None else None

    daily_metrics = {"dau": dau, "started_views": started_views, "completed_views": completed_views,
                     "avg_watch_seconds": avg_watch_seconds, "completion_rate": completion_rate}
    
    return daily_metrics


def get_movie_metrics(metric_date: date) -> list[dict]:
    CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema_dev_password")

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    query = """
            SELECT 
                movie_id,
                countIf(event_type = 'VIEW_STARTED') as started_views,
                countIf(event_type = 'VIEW_FINISHED') as completed_views,
                row_number() OVER (
                    ORDER BY started_views DESC, movie_id ASC
                ) AS view_rank
            FROM cinema.movie_events 
            WHERE toDate(timestamp) = {metric_date:Date} 
            GROUP BY movie_id
            HAVING (started_views > 0 or completed_views > 0)
            FORMAT JSON
            """
    
    params = {
        "param_metric_date": metric_date.isoformat()
    }
    
    response = requests.post(CLICKHOUSE_URL, data=query, params=params, auth=auth, timeout=15)

    response.raise_for_status()

    rows = response.json()["data"]
    
    movie_metrics = []
    for row in rows:
        movie_id = row["movie_id"]
        started_views = int(row["started_views"])
        completed_views = int(row["completed_views"])
        view_rank = int(row["view_rank"])
        movie_metrics.append({"movie_id": movie_id, "started_views": started_views, 
                              "completed_views": completed_views, "view_rank": view_rank})

    return movie_metrics


def get_retention_metrics(cohort_date: date) -> dict:
    CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema_dev_password")

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    query = """
            WITH first_visits AS (
                SELECT user_id, min(toDate(timestamp)) AS first_day FROM cinema.movie_events 
                WHERE event_type = 'VIEW_STARTED' GROUP BY user_id
            ),
            activity_days AS (
                SELECT DISTINCT user_id, toDate(timestamp) AS activity_day FROM cinema.movie_events
            ),
            user_retention AS (
                SELECT fv.user_id, fv.first_day, ad.activity_day, dateDiff('day', fv.first_day, ad.activity_day) AS days_since_first 
                FROM first_visits AS fv 
                JOIN activity_days AS ad ON fv.user_id = ad.user_id 
            )
            SELECT first_day, 
                countIf(days_since_first = 0) AS cohort_size, 
                countIf(days_since_first = 1) AS retained_d1, 
                countIf(days_since_first = 7) AS retained_d7,
                if (dateDiff('day', first_day, toDate(now('UTC'))) > 1, round(100.0 * retained_d1 / NULLIF(cohort_size, 0), 2), NULL) AS retention_d1,
                if (dateDiff('day', first_day, toDate(now('UTC'))) > 7, round(100.0 * retained_d7 / NULLIF(cohort_size, 0), 2), NULL) AS retention_d7
            FROM user_retention
            WHERE first_day = {cohort_date:Date}
            GROUP BY first_day
            ORDER BY first_day
            FORMAT JSON
            """
    
    params = {
        "param_cohort_date": cohort_date.isoformat()
    }
    
    response = requests.post(CLICKHOUSE_URL, data=query, params=params, auth=auth, timeout=15)

    if not response.ok:
        print("ClickHouse error:")
        print(response.text)

    response.raise_for_status()

    rows = response.json()["data"]

    if not rows:
        retention_metrics = {"cohort_size": 0, "retained_d1": 0, "retained_d7": 0,
                                "retention_d1": None, "retention_d7": None}
        return retention_metrics

    row = rows[0]
    cohort_size = int(row["cohort_size"])
    retained_d1 = int(row["retained_d1"])
    retained_d7 = int(row["retained_d7"])
    retention_d1 = float(row["retention_d1"]) if row["retention_d1"] is not None else None
    retention_d7 = float(row["retention_d7"]) if row["retention_d7"] is not None else None

    retention_metrics = {"cohort_size": cohort_size, "retained_d1": retained_d1, "retained_d7": retained_d7,
                        "retention_d1": retention_d1, "retention_d7": retention_d7}
    
    return retention_metrics


def insert_daily_aggregates(metric_date: date, metrics: dict) -> int:
    CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema_dev_password")

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    query = """
            INSERT INTO cinema.daily_aggregates
                (metric_date, metric_name, metric_value)
            FORMAT JSONEachRow
            """
    
    params = {
        "query": query
    }

    rows = [
        {
            "metric_date": metric_date.isoformat(),
            "metric_name": name,
            "metric_value": value
        }
        for name, value in metrics.items()
    ]

    if not rows:
        return 0

    payload = "\n".join(json.dumps(row, allow_nan=False) for row in rows) + "\n"
    
    response = requests.post(CLICKHOUSE_URL, data=payload, params=params, auth=auth, timeout=15)

    response.raise_for_status()

    return len(rows)


def insert_movie_aggregates(metric_date: date, movies: list[dict]) -> int:
    CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema_dev_password")

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    query = """
            INSERT INTO cinema.movie_aggregates
                (metric_date, movie_id, metric_name, metric_value)
            FORMAT JSONEachRow
            """
    
    params = {
        "query": query
    }

    rows = []
    for movie in movies:
        movie_id = movie["movie_id"]

        for metric_name in ("started_views", "completed_views", "view_rank"):
            rows.append(
                {
                    "metric_date": metric_date.isoformat(),
                    "movie_id": movie_id,
                    "metric_name": metric_name,
                    "metric_value": movie[metric_name]
                })

    if not rows:
        return 0

    payload = "\n".join(json.dumps(row, allow_nan=False) for row in rows) + "\n"
    
    response = requests.post(CLICKHOUSE_URL, data=payload, params=params, auth=auth, timeout=15)

    response.raise_for_status()

    return len(rows)


def insert_retention_aggregates(cohort_date: date, metrics: dict) -> int:
    CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "cinema")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema_dev_password")

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)

    query = """
            INSERT INTO cinema.retention_aggregates
                (cohort_date, metric_name, metric_value)
            FORMAT JSONEachRow
            """
    
    params = {
        "query": query
    }

    rows = [
        {
            "cohort_date": cohort_date.isoformat(),
            "metric_name": name,
            "metric_value": value
        }
        for name, value in metrics.items()
    ]

    if not rows:
        return 0

    payload = "\n".join(json.dumps(row, allow_nan=False) for row in rows) + "\n"
    
    response = requests.post(CLICKHOUSE_URL, data=payload, params=params, auth=auth, timeout=15)

    response.raise_for_status()

    return len(rows)


if __name__ == "__main__":
    # daily = get_daily_metrics(date(2026, 11, 22))
    # print(daily)

    # movies = get_movie_metrics(date(2026, 11, 22))
    # for movie in movies:
    #     print(movie)

    # assert sum(movie["started_views"] for movie in movies) == daily["started_views"]
    # assert sum(movie["completed_views"] for movie in movies) == daily["completed_views"]

    # result = get_retention_metrics(date(2026, 11, 9))
    # print(result)

    # result = get_retention_metrics(date(2025, 11, 22))
    # print(result)

    movies = get_movie_metrics(date(2026, 11, 22))
    rows = insert_movie_aggregates(date(2026, 11, 22), movies)

    print("Movies:", len(movies))
    print("Inserted rows:", rows)