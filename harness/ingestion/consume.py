# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["pika==1.4.4", "trino==0.339.0"]
# ///

"""Append all RabbitMQ payloads to the raw Iceberg table through Trino."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
import os
import re
import sys
import time
import uuid

import pika
from trino.dbapi import connect


TABLE = "lakehouse.bronze.raw_order_events"
QUEUE = "lakehouse.raw_order_events"


def table_name(value):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", value):
        raise ValueError("Expected a lowercase catalog.schema.table identifier")
    return ".".join(f'"{part}"' for part in value.split("."))


def trino_connection():
    return connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "ingestion"),
        timezone="UTC", request_timeout=10, max_attempts=1,
        session_properties={"query_max_run_time": "60s"},
    )


def broker_connection():
    parameters = pika.URLParameters(os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"))
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 60
    return pika.BlockingConnection(parameters)


def query(statement, parameters=None):
    connection = trino_connection()
    try:
        return connection.cursor().execute(statement, parameters).fetchall()
    finally:
        connection.close()


def write_batch(table, rows):
    # One INSERT creates one Iceberg snapshot. No application retry after ambiguous commits.
    placeholders = ",".join(["(?,?,?,?,?,?)"] * len(rows))
    query(f"INSERT INTO {table_name(table)} "
          "(payload, ingested_at, exchange, routing_key, redelivered, batch_id) VALUES "
          + placeholders, [value for row in rows for value in row])


def flush(broker, channel, writer, table, pending):
    batch_id = uuid.uuid4().hex
    rows = [(*item[1:], batch_id) for item in pending]
    print(json.dumps({"batch_id": batch_id, "status": "writing", "messages": len(rows)}), flush=True)
    future = writer.submit(write_batch, table, rows)
    while not future.done():
        # Pika stays on its owning thread; Trino's blocking HTTP calls use the worker.
        broker.process_data_events(time_limit=0.2)
        time.sleep(0.05)
    future.result()  # Failure leaves the entire batch unacknowledged.
    channel.basic_ack(pending[-1][0], multiple=True)
    print(json.dumps({"batch_id": batch_id, "committed": len(rows)}), flush=True)
    return batch_id


def consume(*, queue=QUEUE, table=TABLE, seconds=None, limit=None,
            batch_size=500, flush_seconds=5, bind=True):
    table_name(table)
    for name, value in (("seconds", seconds), ("limit", limit),
                        ("batch_size", batch_size), ("flush_seconds", flush_seconds)):
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ValueError(f"{name} must be positive and finite")
    query(f"SELECT payload, ingested_at, exchange, routing_key, redelivered, batch_id "
          f"FROM {table_name(table)} LIMIT 0")
    broker = broker_connection()
    pending = []
    received = 0
    batches = []
    started = time.monotonic()
    first_pending = None
    try:
        channel = broker.channel()
        channel.queue_declare(queue, durable=True)
        if bind:
            channel.exchange_declare("nomly.events", exchange_type="topic", durable=True)
            channel.queue_bind(queue, "nomly.events", routing_key="#")
        channel.basic_qos(prefetch_count=batch_size)
        # ponytail: one writer and small batches; compact files and scale only after measuring backlog.
        with ThreadPoolExecutor(max_workers=1) as writer:
            for method, properties, body in channel.consume(queue, inactivity_timeout=0.2):
                now = time.monotonic()
                if method is not None:
                    if not pending:
                        first_pending = now
                    pending.append((method.delivery_tag, body, datetime.now(timezone.utc),
                                    method.exchange, method.routing_key, method.redelivered))
                    received += 1
                stopping = ((seconds is not None and now - started >= seconds)
                            or (limit is not None and received >= limit))
                if pending and (len(pending) >= batch_size or stopping
                                or now - first_pending >= flush_seconds):
                    batches.append(flush(broker, channel, writer, table, pending))
                    pending.clear()
                if stopping:
                    break
            channel.cancel()
    finally:
        if broker.is_open:
            broker.close()  # Uncommitted/unacknowledged messages are eligible for redelivery.
    result = {"received": received, "committed_batches": batches, "table": table}
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, help="Stop after this duration, then flush")
    parser.add_argument("--limit", type=int, help="Stop after this many messages, then flush")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--flush-seconds", type=float, default=5)
    args = parser.parse_args()
    try:
        consume(**vars(args))
    except KeyboardInterrupt:
        print("Interrupted; unacknowledged messages can be redelivered.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Ingestion failed; unacknowledged messages can be redelivered: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
