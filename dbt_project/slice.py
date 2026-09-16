"""One registered event contract, a Postgres sink, and a checked dbt report."""

import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
import pika
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


ROOT = Path(__file__).resolve().parent
EVENT_VALIDATOR = Draft202012Validator(
    {
        "type": "object",
        "required": ["event_id", "event_type", "order_id", "occurred_at"],
        "properties": {
            "event_id": {"type": "string", "format": "uuid"},
            "event_type": {"const": "order_delivered"},
            "order_id": {"type": "string", "format": "uuid"},
            "occurred_at": {"type": "string", "format": "date-time"},
        },
        "additionalProperties": True,
    },
    format_checker=FormatChecker(),
)


def run_dbt(*args):
    executable = Path(sys.executable).parent / "dbt"
    subprocess.run(
        [str(executable), *args, "--project-dir", str(ROOT), "--profiles-dir", str(ROOT)],
        cwd=ROOT, check=True,
    )


def validate_manifest(manifest):
    sources = [s for s in manifest["sources"].values() if s["source_name"] == "nomly"]
    if len(sources) != 1 or sources[0]["name"] != "deliveries":
        raise ValueError("Declare exactly one nomly source table: deliveries")
    source = sources[0]
    meta = source["config"].get("meta", {})
    if meta.get("event_type") != "order_delivered":
        raise ValueError("Only the registered order_delivered contract is supported")
    resources = [source] + [
        n for n in manifest["nodes"].values()
        if n["resource_type"] == "model" and n["package_name"] == "nomly"
    ]
    for resource in resources:
        settings = resource["config"].get("meta", {})
        if not isinstance(settings.get("owner"), str) or not settings["owner"].strip():
            raise ValueError(f"Missing owner: {resource['name']}")
        if settings.get("freshness_class") != "critical":
            raise ValueError(f"Unsupported freshness_class: {resource['name']}")
    for identifier in (source["schema"], source["identifier"]):
        if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", identifier):
            raise ValueError("Source identifiers must be lowercase SQL names up to 63 characters")
    if source["database"] != os.getenv("PGDATABASE", "nomly"):
        raise ValueError("Source database must match PGDATABASE")
    return source


def load_source():
    run_dbt("parse", "--no-partial-parse")
    return validate_manifest(json.loads((ROOT / "target/manifest.json").read_text()))


def connect_db():
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5432"),
        user=os.getenv("PGUSER", "nomly"), password=os.getenv("PGPASSWORD", "nomly"),
        dbname=os.getenv("PGDATABASE", "nomly"), connect_timeout=10,
        options="-c timezone=UTC", autocommit=True,
    )


def connect_broker():
    return pika.BlockingConnection(pika.URLParameters(
        os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    ))


def relation(source, quarantine=False):
    return sql.Identifier(source["schema"], "quarantine" if quarantine else source["identifier"])


def bootstrap(conn, source):
    conn.execute(sql.SQL("create schema if not exists {}").format(sql.Identifier(source["schema"])))
    conn.execute(sql.SQL("""
        create table if not exists {} (
            event_id text primary key,
            order_id text not null,
            occurred_at timestamptz not null,
            ingested_at timestamptz not null,
            payload jsonb not null
        )
    """).format(relation(source)))
    conn.execute(sql.SQL("""
        create table if not exists {} (
            body_hash text not null,
            kind text not null check (kind in ('invalid', 'conflict')),
            body bytea not null,
            reason text not null,
            event_id text,
            ingested_at timestamptz not null,
            primary key (body_hash, kind)
        )
    """).format(relation(source, True)))


def timestamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamp requires an explicit timezone")
    return result.astimezone(timezone.utc)


def invalid_constant(value):
    raise ValueError(f"Invalid JSON constant: {value}")


def decode_event(body):
    event = json.loads(body, parse_constant=invalid_constant)
    EVENT_VALIDATOR.validate(event)
    occurred_at = timestamp(event["occurred_at"])
    # PostgreSQL text/jsonb cannot represent NUL, even inside otherwise valid JSON.
    def check_strings(value):
        if isinstance(value, str):
            if "\x00" in value:
                raise ValueError("NUL is not representable in Postgres JSON")
            value.encode("utf-8")
        elif isinstance(value, dict):
            for key, item in value.items():
                check_strings(key)
                check_strings(item)
        elif isinstance(value, list):
            for item in value:
                check_strings(item)
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError("Non-finite JSON number")
    check_strings(event)
    return event, occurred_at


def quarantine(conn, source, body, received_at, kind, reason, event_id=None):
    conn.execute(sql.SQL("""
        insert into {} (body_hash, kind, body, reason, event_id, ingested_at)
        values (%s, %s, %s, %s, %s, %s) on conflict do nothing
    """).format(relation(source, True)),
        (hashlib.sha256(body).hexdigest(), kind, body, reason, event_id, received_at))


def store_message(conn, source, body, received_at):
    # Returning from the transaction context commits before the caller can acknowledge.
    with conn.transaction():
        try:
            event, occurred_at = decode_event(body)
        except (ValueError, UnicodeError, RecursionError, ValidationError) as exc:
            quarantine(conn, source, body, received_at, "invalid", str(exc))
            return "invalid"
        inserted = conn.execute(sql.SQL("""
            insert into {} (event_id, order_id, occurred_at, ingested_at, payload)
            values (%s, %s, %s, %s, %s)
            on conflict (event_id) do nothing returning event_id
        """).format(relation(source)),
            (event["event_id"], event["order_id"], occurred_at, received_at, Jsonb(event))).fetchone()
        if inserted:
            return "accepted"
        same = conn.execute(sql.SQL("select payload = %s from {} where event_id = %s")
                            .format(relation(source)), (Jsonb(event), event["event_id"])).fetchone()[0]
        if same:
            return "duplicate"
        quarantine(conn, source, body, received_at, "conflict", "Existing event_id has a different payload", event["event_id"])
        return "conflict"


def declare_queue(channel, source, queue, bind):
    channel.queue_declare(queue, durable=True)
    if bind:
        channel.exchange_declare("nomly.events", exchange_type="topic", durable=True)
        channel.queue_bind(queue, "nomly.events", routing_key=source["config"]["meta"]["event_type"])


def ingest(source, *, queue=None, bind=True, limit=None, seconds=None, capture=None):
    queue = queue or f"nomly.{source['schema']}.{source['identifier']}"
    counts = Counter()
    started = time.monotonic()
    capture_file = None
    broker = None
    try:
        if capture:
            path = Path(capture)
            path.parent.mkdir(parents=True, exist_ok=True)
            capture_file = path.open("x")
        with connect_db() as conn:
            bootstrap(conn, source)
            broker = connect_broker()
            channel = broker.channel()
            declare_queue(channel, source, queue, bind)
            channel.basic_qos(prefetch_count=1)
            # ponytail: one transaction per message; batch commits if measured throughput requires it.
            for method, properties, body in channel.consume(queue, inactivity_timeout=0.5):
                if method is not None:
                    received_at = datetime.now(timezone.utc)
                    if capture_file:
                        capture_file.write(json.dumps({
                            "body_b64": base64.b64encode(body).decode("ascii"),
                            "received_at": received_at.isoformat(),
                        }) + "\n")
                        capture_file.flush()
                        os.fsync(capture_file.fileno())
                    result = store_message(conn, source, body, received_at)
                    counts[result] += 1
                    counts["received"] += 1
                    channel.basic_ack(method.delivery_tag)
                if limit is not None and counts["received"] >= limit:
                    break
                if seconds is not None and time.monotonic() - started >= seconds:
                    break
            channel.cancel()
    finally:
        if broker is not None and broker.is_open:
            broker.close()
        if capture_file:
            capture_file.close()
    return dict(counts)


def read_capture(path):
    records = []
    for line in Path(path).read_text().splitlines():
        record = json.loads(line)
        records.append((base64.b64decode(record["body_b64"], validate=True), timestamp(record["received_at"])))
    return records


def replay(source, capture):
    records = read_capture(capture)
    if not records:
        raise ValueError("Capture is empty")
    queue = f"nomly.replay.{uuid.uuid4().hex}"
    broker = connect_broker()
    try:
        channel = broker.channel()
        declare_queue(channel, source, queue, False)
        channel.confirm_delivery()
        for body, _ in records:
            channel.basic_publish("", queue, body, pika.BasicProperties(delivery_mode=2), mandatory=True)
    finally:
        broker.close()
    counts = ingest(source, queue=queue, bind=False, limit=len(records), seconds=60)
    if counts.get("received") != len(records):
        raise RuntimeError(f"Incomplete replay; remaining messages are in {queue}")
    broker = connect_broker()
    try:
        broker.channel().queue_delete(queue, if_empty=True, if_unused=True)
    finally:
        broker.close()
    return counts


def assert_no_conflicts(source):
    with connect_db() as conn:
        count = conn.execute(sql.SQL("select count(*) from {} where kind = 'conflict'")
                             .format(relation(source, True))).fetchone()[0]
    if count:
        raise ValueError(f"Reporting blocked: {count} unresolved conflicting payload(s)")


def checked_build(source):
    assert_no_conflicts(source)
    run_dbt("build")
    assert_no_conflicts(source)


def report_rows(start, end):
    if start >= end or any(t.second or t.microsecond or t.minute % 5 for t in (start, end)):
        raise ValueError("Report requires increasing, five-minute-aligned timezone-aware bounds")
    with connect_db() as conn:
        return conn.execute(sql.SQL("""
            select window_start, delivered_events from {}
            where window_start >= %s and window_start < %s order by window_start
        """).format(sql.Identifier(os.getenv("NOMLY_ANALYTICS_SCHEMA", "analytics"), "delivery_counts")),
            (start, end)).fetchall()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="Parse dbt and validate the registered source and metadata")
    consume = commands.add_parser("ingest", help="Consume continuously unless bounded")
    consume.add_argument("--limit", type=int)
    consume.add_argument("--seconds", type=float)
    consume.add_argument("--capture", help="New JSONL file; existing files are never overwritten")
    commands.add_parser("build", help="Run conflict gate and dbt build; stop ingestion first")
    replay_parser = commands.add_parser("replay", help="Replay a bounded capture through an isolated queue")
    replay_parser.add_argument("capture")
    report = commands.add_parser("report", help="Build, validate, then query a fixed UTC window; stop ingestion first")
    report.add_argument("--start", required=True)
    report.add_argument("--end", required=True)
    args = parser.parse_args()
    if args.command == "ingest" and any(v is not None and v <= 0 for v in (args.limit, args.seconds)):
        parser.error("--limit and --seconds must be positive")
    source = load_source()
    if args.command == "ingest":
        print(json.dumps(ingest(source, limit=args.limit, seconds=args.seconds, capture=args.capture)))
    elif args.command == "replay":
        print(json.dumps(replay(source, args.capture)))
    elif args.command == "build":
        checked_build(source)
    elif args.command == "report":
        start, end = timestamp(args.start), timestamp(args.end)
        checked_build(source)
        print(json.dumps({"start": start.isoformat(), "end": end.isoformat(),
                          "rows": report_rows(start, end)}, default=str, indent=2))
    else:
        print("Configuration valid: nomly.deliveries -> order_delivered")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg.Error, pika.exceptions.AMQPError, subprocess.CalledProcessError) as exc:
        print(f"Slice failed: {exc}", file=sys.stderr)
        sys.exit(1)
