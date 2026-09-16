{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail',
    views_enabled=false,
    properties={'format': "'PARQUET'", 'format_version': '2', 'partitioning': "ARRAY['day(ingested_at)']"}
) }}

-- Ingestion-day bounds prune both tables. Batch + day prevents duplicate capture
-- without losing the second partition of a batch that crosses UTC midnight.
with decoded as (
    select
        payload, ingested_at, exchange, routing_key, redelivered, batch_id,
        from_utf8(payload) as body,
        try(json_parse(from_utf8(payload))) as document
    from {{ source('nomly', 'raw_order_events') }} as raw
    {{ incremental_partition_filter(
        source_alias='raw',
        partition_column='ingested_at',
        key_columns=['batch_id'],
        lookback_days=var('delivery_input_lookback_days', 2),
        as_of_date=var('delivery_input_as_of_date', none)
    ) }}
),
extracted as (
    select
        payload, ingested_at, exchange, routing_key, redelivered, batch_id, body,
        try_cast(document as map(varchar, json)) as object_fields,
        json_extract_scalar(document, '$.event_type') as event_type,
        try_cast(json_extract_scalar(document, '$.event_id') as uuid) as event_uuid,
        try_cast(json_extract_scalar(document, '$.order_id') as uuid) as order_uuid,
        json_extract_scalar(document, '$.occurred_at') as occurrence_text
    from decoded
    where routing_key = 'order_delivered'
       or json_extract_scalar(document, '$.event_type') = 'order_delivered'
),
typed as (
    select
        payload, ingested_at, exchange, routing_key, redelivered, batch_id,
        body, object_fields, event_type, occurrence_text,
        cast(event_uuid as varchar) as event_id,
        cast(order_uuid as varchar) as order_id,
        try(cast(at_timezone(from_iso8601_timestamp_nanos(occurrence_text), 'UTC')
            as timestamp(6) with time zone)) as occurred_at
    from extracted
)
select
    payload, ingested_at, exchange, routing_key, redelivered, batch_id,
    event_id, order_id, event_type, occurred_at,
    cast(case
        when to_utf8(body) <> payload then 'invalid_utf8'
        when object_fields is null then 'invalid_json_object'
        when event_id is null then 'invalid_event_id'
        when order_id is null then 'invalid_order_id'
        when event_type is distinct from 'order_delivered' then 'invalid_event_type'
        -- Iceberg v2 timestamps retain microseconds; reject excess precision rather than round a window boundary.
        when occurred_at is null or not coalesce(regexp_like(occurrence_text,
            '^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d{1,6})?([Zz]|[+-]\d{2}:\d{2})$'), false)
            then 'invalid_occurred_at'
        else null
    end as varchar) as rejection_reason
from typed
