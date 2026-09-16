# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["pika==1.4.4", "trino==0.339.0"]
# ///

"""Real broker/storage checks, using only a disposable queue, exchange and Iceberg table."""

from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time
from unittest.mock import patch
import uuid

import pika

import consume


ROOT = Path(__file__).resolve().parents[2]


def main():
    suffix = uuid.uuid4().hex[:12]
    table = f"lakehouse.bronze.verify_{suffix}"
    queue = f"verify.{suffix}"
    exchange = f"verify.{suffix}"
    name = consume.table_name(table)
    ddl = (ROOT / "harness/lakehouse/raw.sql").read_text().split(";", 1)[1]
    consume.query(ddl.replace(consume.TABLE, table).strip().rstrip(";"))
    broker = consume.broker_connection()
    channel = broker.channel()
    channel.exchange_declare(exchange, exchange_type="topic", auto_delete=True)
    channel.queue_declare(queue, durable=True)
    channel.queue_bind(queue, exchange, routing_key="#")
    channel.confirm_delivery()

    def publish(key, body):
        channel.basic_publish(exchange, key, body, pika.BasicProperties(delivery_mode=2), mandatory=True)

    def read_rows():
        return consume.query(f"SELECT payload, exchange, routing_key, redelivered FROM {name}")

    try:
        types = ["order_placed", "courier_assigned", "order_picked_up", "order_delivered", "order_cancelled"]
        messages = [(kind, json.dumps({"event_id": str(uuid.uuid4()), "event_type": kind,
                                      "occurred_at": "2026-01-01T10:00:00Z"}).encode()) for kind in types]
        messages += [messages[3], ("order_delivered", b'{"occurred_at":"N/A"}'),
                     ("order_teleported", b"{not json"), ("binary", b"\xff\x00\xfe")]
        for key, body in messages:
            publish(key, body)
        with patch.object(consume, "datetime", wraps=datetime) as clock:
            clock.now.return_value = datetime(2026, 9, 16, 11, tzinfo=timezone.utc)
            result = consume.consume(queue=queue, table=table, bind=False, limit=len(messages), seconds=20,
                                     batch_size=5, flush_seconds=0.5)
        assert result["received"] == len(messages)
        assert len(result["committed_batches"]) == 2  # Size flush + partial bounded flush.
        assert Counter((r[2], r[0]) for r in read_rows()) == Counter(messages)
        assert all(r[1] == exchange for r in read_rows())

        # Two receipt dates span midnight. Old/malformed event timestamps cannot affect partitions.
        partition_batch = uuid.uuid4().hex
        consume.write_batch(table, [
            (b"before midnight", datetime(2026, 1, 1, 23, 59, 59, tzinfo=timezone.utc), exchange, "boundary", False, partition_batch),
            (b"after midnight", datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc), exchange, "boundary", False, partition_batch),
        ])
        assert consume.query(f'SELECT count(*) FROM lakehouse.bronze."verify_{suffix}$partitions"')[0][0] == 3

        # A real failed INSERT must not acknowledge its message or change raw rows.
        before = Counter(tuple(r) for r in read_rows())
        publish("failed_write", b"retry me")
        def fail_insert(*_):
            consume.query(f"INSERT INTO {name} (column_that_does_not_exist) VALUES (1)")
        with patch.object(consume, "write_batch", side_effect=fail_insert):
            try:
                consume.consume(queue=queue, table=table, bind=False, limit=1, seconds=10)
            except Exception as exc:
                assert "column_that_does_not_exist" in str(exc), exc
            else:
                raise AssertionError("Failed INSERT was accepted")
        assert Counter(tuple(r) for r in read_rows()) == before
        recovered = consume.consume(queue=queue, table=table, bind=False, limit=1, seconds=10)
        assert recovered["received"] == 1
        assert any(r[0] == b"retry me" and r[3] for r in read_rows())

        # Commit succeeds, then the broker connection disappears before acknowledgement.
        publish("commit_before_ack", b"already committed")
        interrupted = consume.broker_connection()
        interrupted_channel = interrupted.channel()
        method, _, body = interrupted_channel.basic_get(queue, auto_ack=False)
        assert method and body == b"already committed"
        consume.write_batch(table, [(body, datetime.now(timezone.utc), method.exchange,
                                     method.routing_key, method.redelivered, uuid.uuid4().hex)])
        interrupted.close()
        recovered = consume.consume(queue=queue, table=table, bind=False, limit=1, seconds=10)
        assert recovered["received"] == 1
        duplicates = [r for r in read_rows() if r[0] == body]
        assert len(duplicates) == 2 and sorted(r[3] for r in duplicates) == [False, True]

        # A two-second negotiated heartbeat survives a five-second blocking writer.
        publish("heartbeat", b"slow insert")
        actual_write = consume.write_batch
        def short_heartbeat_connection():
            parameters = pika.URLParameters(os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/"))
            parameters.heartbeat = 2
            return pika.BlockingConnection(parameters)
        def slow_write(*args):
            time.sleep(5)
            return actual_write(*args)
        with patch.object(consume, "broker_connection", side_effect=short_heartbeat_connection), \
                patch.object(consume, "write_batch", side_effect=slow_write):
            assert consume.consume(queue=queue, table=table, bind=False, limit=1, seconds=15)["received"] == 1

        # Time-based flush while the consumer keeps running with fewer than 500 messages.
        publish("timer", b"timer flush")
        written_at = []
        def timed_write(*args):
            written_at.append(time.monotonic())
            return actual_write(*args)
        with patch.object(consume, "write_batch", side_effect=timed_write):
            timed = consume.consume(queue=queue, table=table, bind=False, seconds=3, flush_seconds=0.5)
        assert timed["received"] == 1 and len(timed["committed_batches"]) == 1
        assert time.monotonic() - written_at[0] >= 1, "Batch was only flushed at the run deadline"
        assert channel.queue_declare(queue, passive=True).method.message_count == 0
        files = consume.query(f'SELECT file_format, file_path FROM lakehouse.bronze."verify_{suffix}$files"')
        assert files and all(row[0] == "PARQUET" for row in files)
        path = files[0][1].replace("s3://", "local/", 1)
        subprocess.run(["docker", "compose", "-f", str(ROOT / "harness/docker-compose.yml"),
                        "exec", "-T", "minio", "mc", "stat", path], check=True)
        evidence = {"fixture_messages": len(messages), "exact_bytes_preserved": True,
                    "receipt_partitions_checked": 3, "failed_write_redelivered": True,
                    "commit_before_ack_preserves_duplicates": True, "heartbeat_during_write": True,
                    "size_time_partial_flush": True, "parquet_files_checked": len(files)}
        (Path(__file__).parent / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2))
    finally:
        if broker.is_open:
            channel.queue_delete(queue)
            # The auto-delete exchange may already have disappeared with its last binding.
            broker.close()
        consume.query(f"DROP TABLE {name}")


if __name__ == "__main__":
    main()
