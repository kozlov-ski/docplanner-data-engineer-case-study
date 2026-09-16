#!/bin/sh
set -eu
if [ "${1:-init}" = check ]; then
    test -f /tmp/lakehouse-bucket-ready
    mc stat local/nomly-lakehouse >/dev/null
    exit
fi

exec > /proc/1/fd/1 2>&1
rm -f /tmp/lakehouse-bucket-ready
attempt=0
until curl --fail --silent --max-time 3 http://localhost:9000/minio/health/ready >/dev/null; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
        echo 'MinIO initialization timed out waiting for the API' >&2
        exit 1
    fi
    sleep 2
done
mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing local/nomly-lakehouse
touch /tmp/lakehouse-bucket-ready
