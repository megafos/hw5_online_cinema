CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka
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

ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'cinema-clickhouse-v1',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    format_avro_schema_registry_url = 'http://schema-registry:8081'
;