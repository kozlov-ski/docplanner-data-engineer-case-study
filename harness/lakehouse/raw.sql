CREATE SCHEMA IF NOT EXISTS lakehouse.bronze
WITH (location = 's3://nomly-lakehouse/bronze/');

CREATE TABLE IF NOT EXISTS lakehouse.bronze.raw_order_events (
    payload VARBINARY NOT NULL,
    ingested_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    exchange VARCHAR NOT NULL,
    routing_key VARCHAR NOT NULL,
    redelivered BOOLEAN NOT NULL,
    batch_id VARCHAR NOT NULL
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['day(ingested_at)']
);
