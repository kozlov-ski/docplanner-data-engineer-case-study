#!/bin/sh
set -eu
export PGPASSWORD="$POSTGRES_PASSWORD" PGCONNECT_TIMEOUT=3
query() { psql -X -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 "$@"; }

if [ "${1:-init}" = check ]; then
    test -f /tmp/lakehouse-catalog-ready
    query -c 'SELECT iceberg_type FROM iceberg_catalog.iceberg_tables LIMIT 0;
              SELECT property_value FROM iceberg_catalog.iceberg_namespace_properties LIMIT 0;' >/dev/null
    exit
fi

rm -f /tmp/lakehouse-catalog-ready
attempt=0
until query -c 'SELECT 1' >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        echo 'Postgres initialization timed out waiting for the database' >&2
        exit 1
    fi
    sleep 2
done
# PostgreSQL logs SQL errors itself. Keep hook output available even when Compose
# only prints the exit status; its server-owned stdout pipe cannot be reopened.
query -f /lakehouse/catalog.sql > /tmp/lakehouse-init.log 2>&1 || {
    cat /tmp/lakehouse-init.log >&2
    exit 1
}
touch /tmp/lakehouse-catalog-ready
