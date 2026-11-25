CREATE TABLE IF NOT EXISTS cinema.daily_aggregates
(
    metric_date Date,
    metric_name LowCardinality(String),
    metric_value Nullable(Decimal(18, 4)),
    computed_at DateTime64(6, 'UTC') DEFAULT now64(6)
)

ENGINE = ReplacingMergeTree(computed_at)
PARTITION by toYYYYMM(metric_date)
ORDER BY (metric_date, metric_name);