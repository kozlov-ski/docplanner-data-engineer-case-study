select *
from {{ ref('delivery_counts') }}
where delivered_events <= 0
