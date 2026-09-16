select
    event_id,
    order_id,
    occurred_at,
    min(ingested_at) as ingested_at
from {{ ref('stg_nomly__delivery_inputs') }}
where rejection_reason is null
group by event_id, order_id, occurred_at
