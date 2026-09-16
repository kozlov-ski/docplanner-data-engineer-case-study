-- Pass explicit, five-minute-aligned UTC bounds with --vars when compiling.
-- Ingestion must be stopped and the checked build must have succeeded.
select window_start, delivered_events
from {{ ref('delivery_counts') }}
where window_start >= '{{ var("window_start", "2000-01-01T00:00:00Z") }}'::timestamptz
  and window_start < '{{ var("window_end", "2000-01-01T01:00:00Z") }}'::timestamptz
order by window_start
