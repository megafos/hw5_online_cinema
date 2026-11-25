import logging
import time
from datetime import date
from clickhouse_client import (get_daily_metrics, get_movie_metrics, get_retention_metrics, 
                               insert_daily_aggregates, insert_movie_aggregates, insert_retention_aggregates)
from postgres_client import upsert_daily_metrics, upsert_movie_metrics, upsert_retention_metrics


logger = logging.getLogger(__name__)

def aggregate_daily_metrics(metric_date: date) -> int:
    metrics = get_daily_metrics(metric_date)
    insert_daily_aggregates(metric_date, metrics)
    upsert_daily_metrics(metric_date, metrics)
    return len(metrics)


def aggregate_movie_metrics(metric_date: date) -> int:
    movies = get_movie_metrics(metric_date)
    insert_movie_aggregates(metric_date, movies)
    upsert_movie_metrics(metric_date, movies)
    return 3 * len(movies)


def aggregate_retention_metrics(cohort_date: date) -> int:
    metrics = get_retention_metrics(cohort_date)
    insert_retention_aggregates(cohort_date, metrics)
    upsert_retention_metrics(cohort_date, metrics)
    return len(metrics)


def aggregate_all(metric_date: date, cohort_dates: list[date]) -> None:
    logger.info(
        "Aggregation started: metric_date=%s, cohort_dates=%s",
        metric_date,
        cohort_dates
    )

    start_time = time.perf_counter()
    try:
        daily_rows = aggregate_daily_metrics(metric_date)
        movie_rows = aggregate_movie_metrics(metric_date)
        retention_rows = 0
        for cohort_date in cohort_dates:
            retention_rows += aggregate_retention_metrics(cohort_date)

        total_rows = daily_rows + movie_rows + retention_rows

        duration = time.perf_counter() - start_time

        logger.info(
            "Aggregation completed: daily_rows=%d, movie_rows=%d, "
            "retention_rows=%d, total_rows=%d, duration_seconds=%.2f",
            daily_rows,
            movie_rows,
            retention_rows,
            total_rows,
            duration
        )
    except Exception:
        logger.exception(
            "Aggregation failed: metric_date=%s, duration_seconds=%.2f",
            metric_date,
            time.perf_counter() - start_time
        )
        raise


if __name__ == "__main__":
    aggregate_all(
        date(2026, 11, 22),
        [
            date(2026, 11, 9),
            date(2026, 11, 22)
        ]
    )
