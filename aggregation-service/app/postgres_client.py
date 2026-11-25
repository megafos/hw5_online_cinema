import os
import psycopg
import logging
from datetime import date
from clickhouse_client import get_movie_metrics


logger = logging.getLogger(__name__)

def upsert_daily_metric(metric_date: date, metric_name: str, metric_value) -> None:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "cinema_analytics")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics_dev_password")

    query = """
            INSERT INTO daily_metrics (
                metric_date,
                metric_name,
                metric_value
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (metric_date, metric_name)
            DO UPDATE SET metric_value = EXCLUDED.metric_value, computed_at = NOW();
            """
    
    with psycopg.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD) as conn:
        conn.execute(query, (metric_date, metric_name, metric_value))

        
def upsert_daily_metrics(metric_date: date, metrics: dict) -> None:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "cinema_analytics")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics_dev_password")

    query = """
            INSERT INTO daily_metrics (
                metric_date,
                metric_name,
                metric_value
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (metric_date, metric_name)
            DO UPDATE SET metric_value = EXCLUDED.metric_value, computed_at = NOW();
            """
    try:
        with psycopg.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD) as conn:
            for metric_name, metric_value in metrics.items():
                conn.execute(query, (metric_date, metric_name, metric_value))

        logger.info(
                    "Saved daily metrics: date=%s, rows=%d",
                    metric_date,
                    len(metrics)
                )
    except Exception:
        logger.exception(
            "Failed to save daily metrics: date=%s",
            metric_date
        )
        raise


def upsert_movie_metrics(metric_date: date, movies: list[dict]) -> None:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "cinema_analytics")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics_dev_password")

    query = """
            INSERT INTO movie_metrics (
                metric_date,
                movie_id,
                metric_name,
                metric_value
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (metric_date, movie_id, metric_name)
            DO UPDATE SET metric_value = EXCLUDED.metric_value, computed_at = NOW();
            """

    
    try:
        with psycopg.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD) as conn:  
            conn.execute(
                "DELETE FROM movie_metrics WHERE metric_date = %s",
                (metric_date,)
            )
            
            for movie in movies:
                movie_id = movie["movie_id"]
                metrics = {
                    "started_views": movie["started_views"],
                    "completed_views": movie["completed_views"],
                    "view_rank": movie["view_rank"]
                }

            
                for metric_name, metric_value in metrics.items():
                    conn.execute(query, (metric_date, movie_id, metric_name, metric_value))

        logger.info(
            "Saved movie metrics: date=%s, movies=%d, rows=%d",
            metric_date,
            len(movies),
            3 * len(movies)
        )
    except Exception:
        logger.exception(
            "Failed to save movie metrics: date=%s",
            metric_date
        )
        raise

def upsert_retention_metrics(cohort_date: date, metrics: dict) -> None:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "cinema_analytics")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics_dev_password")

    query = """
            INSERT INTO retention_metrics (
                cohort_date,
                metric_name,
                metric_value
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (cohort_date, metric_name)
            DO UPDATE SET metric_value = EXCLUDED.metric_value, computed_at = NOW();
            """
    try:
        with psycopg.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD) as conn:
            for metric_name, metric_value in metrics.items():
                conn.execute(query, (cohort_date, metric_name, metric_value))

        logger.info(
            "Saved retention metrics: cohort_date=%s, rows=%d",
            cohort_date,
            len(metrics)
        )
    except Exception:
        logger.exception(
            "Failed to save retention metrics: cohort_date=%s",
            cohort_date
        )
        raise


if __name__ == "__main__":
    # movies = get_movie_metrics(date(2026, 11, 22))

    # upsert_movie_metrics(date(2026, 11, 22), movies)

    # test_metrics = {
    #     "dau": 17,
    #     "started_views": 53,
    #     "completed_views": 43,
    #     "avg_watch_seconds": 5576.3,
    #     "completion_rate": 81.13
    # }
    # upsert_daily_metric(date(2026, 11, 22), test_metrics)

    # invalid_metrics = {
    #     "dau": 10,
    #     "started_views": 20,
    #     "completed_views": -5
    # }

    # try:
    #     upsert_daily_metric(date(2025, 11, 22), invalid_metrics)
    # except psycopg.errors.CheckViolation:
    #     print("Transaction rolled back")

    test_metrics = {
        "cohort_size": 4,
        "retained_d1": 3,
        "retained_d7": 2,
        "retention_d1": 75.0,
        "retention_d7": 50.0
    }

    upsert_retention_metrics(date(2026, 11, 9), test_metrics)
