CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date DATE NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(18, 4) CHECK(metric_value >= 0),
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_date, metric_name)
); 


CREATE TABLE IF NOT EXISTS movie_metrics (
    metric_date DATE NOT NULL,
    movie_id VARCHAR(255) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(18, 4) CHECK(metric_value >= 0),
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_date, movie_id, metric_name)
); 


CREATE TABLE IF NOT EXISTS retention_metrics (
    cohort_date DATE NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(18, 4) CHECK(metric_value >= 0),
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cohort_date, metric_name)
); 
