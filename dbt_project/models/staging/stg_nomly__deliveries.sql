select
    event_id,
    order_id,
    occurred_at,
    ingested_at
from {{ source('nomly', 'deliveries') }}
