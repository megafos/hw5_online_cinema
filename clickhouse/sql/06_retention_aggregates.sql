CREATE TABLE IF NOT EXISTS cinema.retention_aggregates
(
    cohort_date Date,
    metric_name LowCardinality(String),
    metric_value Nullable(Decimal(18, 4)),
    computed_at DateTime64(6, 'UTC') DEFAULT now64(6)
)

ENGINE = ReplacingMergeTree(computed_at)
PARTITION by toYYYYMM(cohort_date)
ORDER BY (cohort_date, metric_name);