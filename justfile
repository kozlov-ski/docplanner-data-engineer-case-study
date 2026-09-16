compose := "docker compose -f harness/docker-compose.yml"

# Start the stack, then consume continuously; Ctrl-C stops only ingestion.
up:
    {{ compose }} up -d --wait --wait-timeout 240
    uv run --locked harness/ingestion/consume.py

# Build and test delivery-event counts from the current Bronze data.
dbt vars='{}':
    uv run --locked --project dbt_project dbt build --project-dir dbt_project --profiles-dir dbt_project --vars {{ quote(vars) }}

# Test existing models; run just dbt first to include new Bronze arrivals.
dbt-test:
    uv run --locked --project dbt_project dbt test --project-dir dbt_project --profiles-dir dbt_project

# Build/test live models, then verify replay, conflicts and window assumptions in isolation.
dbt-verify: dbt
    uv run --locked --project dbt_project python dbt_project/verify.py

# Require recent rows and a real Parquet object in MinIO.
check:
    #!/bin/sh
    set -eu
    stats=$({{ compose }} exec -T trino trino --output-format CSV_UNQUOTED --execute "SELECT count(*), max(ingested_at), coalesce(max(ingested_at) >= current_timestamp - INTERVAL '60' SECOND, false) FROM lakehouse.bronze.raw_order_events")
    printf 'rows, latest ingestion (UTC), fresh within 60s\n%s\n' "$stats"
    case "$stats" in
        *,true) ;;
        *) echo 'No recent receipts. Start ingestion with just up and wait for a committed batch.' >&2; exit 1 ;;
    esac
    object=$({{ compose }} exec -T trino trino --output-format CSV_UNQUOTED --execute 'SELECT file_path FROM lakehouse.bronze."raw_order_events$files" WHERE file_format = '\''PARQUET'\'' LIMIT 1')
    case "$object" in
        s3://nomly-lakehouse/*) ;;
        *) echo 'No Parquet object found in the expected MinIO bucket.' >&2; exit 1 ;;
    esac
    {{ compose }} exec -T minio mc stat "local/${object#s3://}"
    echo 'PASS: recent events are queryable and Parquet data exists in MinIO.'

# Stop the foreground consumer first; retain containers and all data.
stop:
    {{ compose }} stop

# DESTRUCTIVE: delete this harness's containers and attached data volumes.
nuke:
    {{ compose }} down --volumes --remove-orphans
