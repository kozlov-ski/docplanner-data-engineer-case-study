select window_start
from {{ ref('delivery_counts') }}
where window_start <> date_trunc('minute', window_start)
   or mod(minute(window_start), 5) <> 0
