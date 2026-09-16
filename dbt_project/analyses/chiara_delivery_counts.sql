-- Pass explicit, five-minute-aligned UTC bounds with --vars when compiling.
-- The latest checked build must have succeeded; a failed build leaves stale counts.
select window_start, delivered_events
from {{ ref('delivery_counts') }}
where window_start >= from_iso8601_timestamp('{{ var("window_start", "2000-01-01T00:00:00Z") }}')
  and window_start < from_iso8601_timestamp('{{ var("window_end", "2000-01-01T01:00:00Z") }}')
order by window_start
