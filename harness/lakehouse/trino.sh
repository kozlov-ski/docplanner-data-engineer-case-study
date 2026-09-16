#!/bin/sh
set -eu
query() { trino --server http://localhost:8080 --user bootstrap --client-request-timeout 5s --output-format CSV_UNQUOTED "$@"; }

if [ "${1:-init}" = check ]; then
    test -f /tmp/lakehouse-table-ready
    query --execute 'SELECT payload, ingested_at, exchange, routing_key, redelivered, batch_id FROM lakehouse.bronze.raw_order_events LIMIT 0' >/dev/null
    exit
fi

exec > /proc/1/fd/1 2>&1
rm -f /tmp/lakehouse-table-ready
attempt=0
until query --execute 'SELECT 1' >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        echo 'Trino initialization timed out waiting for SQL readiness' >&2
        exit 1
    fi
    sleep 2
done
query --file /lakehouse/raw.sql

columns=$(query --execute "SELECT array_join(array_agg(column_name || ':' || data_type || ':' || is_nullable ORDER BY ordinal_position), ',') FROM lakehouse.information_schema.columns WHERE table_schema = 'bronze' AND table_name = 'raw_order_events'")
expected='payload:varbinary:NO,ingested_at:timestamp(6) with time zone:NO,exchange:varchar:NO,routing_key:varchar:NO,redelivered:boolean:NO,batch_id:varchar:NO'
if [ "$columns" != "$expected" ]; then
    echo "Incompatible raw_order_events columns: $columns" >&2
    exit 1
fi
definition=$(query --execute 'SHOW CREATE TABLE lakehouse.bronze.raw_order_events')
for property in "format = 'PARQUET'" 'format_version = 2' "partitioning = ARRAY['day(ingested_at)']" 's3://nomly-lakehouse/bronze/'; do
    if ! printf '%s\n' "$definition" | grep -F "$property" >/dev/null; then
        echo "Incompatible raw_order_events property: expected $property" >&2
        exit 1
    fi
done
touch /tmp/lakehouse-table-ready
echo 'lakehouse.bronze.raw_order_events is ready'
