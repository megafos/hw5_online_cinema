import os
import argparse
import time
import requests
import psycopg
import logging
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from aggregator import aggregate_all
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify


app = Flask(__name__)
logger = logging.getLogger(__name__)
aggregation_lock = threading.Lock()


class AggregationBusyError(Exception):
    pass


def run_auto_aggregation() -> None:
    today_utc = datetime.now(timezone.utc).date()
    metric_date = today_utc - timedelta(days=1)

    cohort_dates = [
        metric_date,
        metric_date - timedelta(days=1),
        metric_date - timedelta(days=7)
    ]

    logger.info(
        "Automatic aggregation dates: metric_date=%s, cohort_dates=%s",
        metric_date,
        cohort_dates
    )

    try:
        aggregate_with_retry(metric_date, cohort_dates)
    except AggregationBusyError:
        logger.warning(
            "Scheduled aggregation skipped: another aggregation is running"
        )


def run_schedule_aggregation() -> None:
    interval_minutes = int(os.getenv("AGGREGATION_INTERVAL_MINUTES", "1440"))

    if interval_minutes <= 0:
        raise ValueError("Aggregation interval must be positive")

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        run_auto_aggregation,
        trigger="interval",
        minutes=interval_minutes,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc)
    )

    print(f"Aggregation interval: {interval_minutes} minutes")
    scheduler.start()

    app.run(host="0.0.0.0", port=5001, use_reloader=False)


def aggregate_with_retry(metric_date: date, cohort_dates: list[date]) -> None:
    if not aggregation_lock.acquire(blocking=False):
        raise AggregationBusyError("Aggregation is already running")
    
    try:
        retries = 3
        for attempt in range(1, retries + 1):
            try:
                aggregate_all(metric_date, cohort_dates)
                logger.info(
                    "Aggregation succeeded on attempt %s",
                    attempt
                )
                return
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, psycopg.OperationalError) as e:
                logger.warning(
                    "Aggregation attempt %s/%s failed: %s",
                    attempt, retries, e
                )
                if attempt == retries:
                    raise

                time.sleep(5)
    finally:
        aggregation_lock.release()


@app.post("/aggregate")
def trigger_aggregation():
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify({"error": "Expected JSON object"}), 400

    date_str = data.get("date")

    if not isinstance(date_str, str) or not date_str:
        return jsonify({"error": "Field 'date' is required"}), 400

    try:
        metric_date = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": "Invalid date format. Expected YYYY-MM-DD"}), 400

    cohort_dates = [metric_date]

    try:
        aggregate_with_retry(metric_date, cohort_dates)
    except AggregationBusyError:
        return jsonify({"error": "Aggregation is already running"}), 409
    except (requests.exceptions.RequestException, psycopg.OperationalError):
        logger.exception(
            "HTTP aggregation failed: metric_date=%s",
            metric_date
        )

        return jsonify({"error": "Analytics database unavailable"}), 503

    return jsonify({"status": "success", "metric_date": metric_date.isoformat()}), 200


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout
    )

    parser = argparse.ArgumentParser(description = "Cinema analytics aggregation service")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date")
    group.add_argument("--auto", action="store_true")
    group.add_argument("--schedule", action="store_true")

    parser.add_argument("--cohort-date", action="append", default=[])

    args = parser.parse_args()

    if args.auto:
        run_auto_aggregation()
    elif args.schedule:
        run_schedule_aggregation()
    else:
        metric_date = date.fromisoformat(args.date)

        cohort_dates = [
            date.fromisoformat(value) for value in args.cohort_date
        ]

        print(f"Metric date: {metric_date}")
        print(f"Cohort dates: {cohort_dates}")

        aggregate_with_retry(metric_date, cohort_dates)
