CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Nullable(Int32)
)

ENGINE = MergeTree
PARTITION by toYYYYMM(timestamp)
ORDER BY (user_id, timestamp, event_id);