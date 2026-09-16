"""Executable end-to-end checks; real RabbitMQ/Postgres, isolated names, no test framework."""

import argparse
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import tempfile
from unittest.mock import patch
import uuid

import pika
from jsonschema.exceptions import ValidationError
from psycopg import sql

import dbt_project.slice as slice


def publish(source, bodies, queue):
    broker = slice.connect_broker()
    try:
        channel = broker.channel()
        slice.declare_queue(channel, source, queue, False)
        channel.confirm_delivery()
        for body in bodies:
            channel.basic_publish(
                "", queue, body, pika.BasicProperties(delivery_mode=2), mandatory=True
            )
    finally:
        broker.close()


def delete_queue(queue):
    broker = slice.connect_broker()
    try:
        broker.channel().queue_delete(queue, if_empty=True, if_unused=True)
    finally:
        broker.close()


def snapshot(source):
    with slice.connect_db() as conn:
        accepted = conn.execute(
            sql.SQL("select * from {} order by event_id").format(slice.relation(source))
        ).fetchall()
        rejected = conn.execute(
            sql.SQL("select * from {} order by body_hash, kind").format(
                slice.relation(source, True)
            )
        ).fetchall()
    return accepted, rejected


def assert_capture_matches(source, capture):
    # Independent oracle for known harness fields; does not call the consumer decoder
    # or read the database to establish the expected input IDs/window counts.
    accepted = {}
    invalid = set()
    for body, _ in slice.read_capture(capture):
        try:
            event = json.loads(body)
            assert isinstance(event, dict) and event["event_type"] == "order_delivered"
            uuid.UUID(event["event_id"])
            uuid.UUID(event["order_id"])
            occurred = datetime.fromisoformat(
                event["occurred_at"].replace("Z", "+00:00")
            )
            assert occurred.tzinfo is not None
            if event["event_id"] in accepted:
                assert accepted[event["event_id"]][0] == event, (
                    "Unexpected conflicting live input"
                )
            accepted[event["event_id"]] = (event, occurred)
        except (ValueError, TypeError, KeyError, AssertionError):
            invalid.add(body)
    rows, rejected = snapshot(source)
    assert {r[0]: r[4] for r in rows} == {
        key: item[0] for key, item in accepted.items()
    }
    assert {bytes(r[2]) for r in rejected} == invalid
    assert all(r[1] == "invalid" for r in rejected)
    counts = Counter()
    for _, occurred in accepted.values():
        seconds = int(occurred.timestamp())
        counts[datetime.fromtimestamp(seconds - seconds % 300, timezone.utc)] += 1
    assert counts, "No valid delivery events captured"
    start, end = min(counts), max(counts) + timedelta(minutes=5)
    slice.checked_build(source)
    actual = slice.report_rows(start, end)
    assert actual == sorted(counts.items()), (actual, counts)
    return {
        "messages": len(slice.read_capture(capture)),
        "unique_valid_ids": len(accepted),
        "quarantined_payloads": len(invalid),
        "start": start,
        "end": end,
        "rows": actual,
    }


def fixture_check(source, directory, prefix):
    base = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
    events = []
    for i in range(100):
        occurred = base + (
            timedelta(minutes=(1, 4)[i])
            if i < 2
            else timedelta(minutes=5, seconds=(i - 2) * 3)
        )
        events.append(
            {
                "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"delivery-{i}")),
                "order_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"order-{i}")),
                "event_type": "order_delivered",
                "occurred_at": occurred.isoformat(),
                "new_optional_field": "preserved",
            }
        )
    malformed = {**events[0], "event_id": str(uuid.uuid4()), "occurred_at": "N/A"}
    bodies = [json.dumps(e).encode() for e in events + events[:5] + [malformed]]
    random.Random(42).shuffle(bodies)
    queue = f"{prefix}.fixture"
    publish(source, bodies, queue)
    capture = directory / "fixture.jsonl"
    # Control only the receipt clock. Events still pass through real RabbitMQ and Postgres.
    with patch.object(slice, "datetime", wraps=datetime) as clock:
        clock.now.return_value = base + timedelta(hours=1)
        ingestion = slice.ingest(
            source, queue=queue, bind=False, limit=106, seconds=30, capture=capture
        )
    delete_queue(queue)
    assert ingestion == {"accepted": 100, "duplicate": 5, "invalid": 1, "received": 106}
    evidence = assert_capture_matches(source, capture)
    assert evidence["rows"] == [(base, 2), (base + timedelta(minutes=5), 98)]
    assert (
        slice.report_rows(
            base + timedelta(hours=1), base + timedelta(hours=1, minutes=5)
        )
        == []
    )
    before = snapshot(source)
    replay = slice.replay(source, capture)
    assert replay == {"received": 106, "duplicate": 105, "invalid": 1}
    assert snapshot(source) == before
    assert assert_capture_matches(source, capture) == evidence

    # A changed payload with the same ID cannot overwrite an already accepted event.
    conflict = json.dumps({**events[0], "order_id": str(uuid.uuid4())}).encode()
    queue = f"{prefix}.conflict"
    publish(source, [conflict], queue)
    assert (
        slice.ingest(source, queue=queue, bind=False, limit=1, seconds=10)["conflict"]
        == 1
    )
    delete_queue(queue)
    assert snapshot(source)[0] == before[0]
    try:
        slice.checked_build(source)
    except ValueError as exc:
        assert "Reporting blocked" in str(exc)
    else:
        raise AssertionError("Conflicting payload did not block reporting")
    return {
        **evidence,
        "ingestion": ingestion,
        "replay": replay,
        "identical_after_replay": True,
        "conflict_blocks_reporting": True,
    }


def recovery_check(source, prefix):
    recovery_source = {**source, "schema": source["schema"] + "_recovery"}
    queue = f"{prefix}.recovery"
    event = {
        "event_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "event_type": "order_delivered",
        "occurred_at": "2026-01-01T10:01:00Z",
    }
    body = json.dumps(event).encode()
    publish(recovery_source, [body], queue)
    broker = slice.connect_broker()
    channel = broker.channel()
    method, _, delivered = channel.basic_get(queue, auto_ack=False)
    assert method and delivered == body
    with slice.connect_db() as conn:
        slice.bootstrap(conn, recovery_source)
        assert (
            slice.store_message(
                conn, recovery_source, delivered, datetime.now(timezone.utc)
            )
            == "accepted"
        )
    before = snapshot(recovery_source)
    broker.close()  # Commit succeeded, but no acknowledgement was sent.
    result = slice.ingest(recovery_source, queue=queue, bind=False, limit=1, seconds=10)
    assert result == {"duplicate": 1, "received": 1}
    assert snapshot(recovery_source) == before
    delete_queue(queue)
    return True


def negative_config_checks():
    manifest = json.loads((slice.ROOT / "target/manifest.json").read_text())
    for key, value in (
        ("event_type", "order_placed"),
        ("owner", ""),
        ("freshness_class", "unknown"),
    ):
        changed = deepcopy(manifest)
        next(iter(changed["sources"].values()))["config"]["meta"][key] = value
        try:
            slice.validate_manifest(changed)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Invalid {key} was accepted")

    # Use a disposable project copy; negative tests never edit the submitted models.
    with tempfile.TemporaryDirectory(prefix="nomly-dbt-negative-") as directory:
        project = Path(directory)
        shutil.copy(slice.ROOT / "dbt_project.yml", project)
        shutil.copytree(slice.ROOT / "models", project / "models")
        (project / "models/broken_sql.sql").write_text(
            "select 1 + from missing_table\n"
        )
        (project / "models/broken_contract.sql").write_text(
            "{{ config(materialized='table', contract={'enforced': true}) }}\nselect 1::integer as value\n"
        )
        (project / "models/broken_contract.yml").write_text(
            "version: 2\nmodels:\n  - name: broken_contract\n    columns:\n"
            "      - name: value\n        data_type: text\n"
        )
        for model, expected in (
            ("broken_sql", "syntax error"),
            ("broken_contract", "contract"),
        ):
            result = subprocess.run(
                [
                    str(Path(sys.executable).parent / "dbt"),
                    "build",
                    "--select",
                    model,
                    "--project-dir",
                    str(project),
                    "--profiles-dir",
                    str(slice.ROOT),
                ],
                capture_output=True,
                text=True,
            )
            output = result.stdout + result.stderr
            assert result.returncode != 0 and expected in output.lower(), output
        (project / "models/broken_contract.yml").write_text("models: [unterminated\n")
        result = subprocess.run(
            [
                str(Path(sys.executable).parent / "dbt"),
                "parse",
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(slice.ROOT),
            ],
            capture_output=True,
            text=True,
        )
        output = result.stdout + result.stderr
        assert result.returncode != 0 and "parsing error" in output.lower(), output
    return True


def live_check(source, directory, seconds):
    capture = directory / "live.jsonl"
    ingestion = slice.ingest(source, seconds=seconds, capture=capture)
    # No background consumer is running; only this frozen capture feeds these tables.
    evidence = assert_capture_matches(source, capture)
    before = snapshot(source)
    replay = slice.replay(source, capture)
    assert snapshot(source) == before
    assert assert_capture_matches(source, capture) == evidence
    queue = f"nomly.{source['schema']}.{source['identifier']}"
    broker = slice.connect_broker()
    try:
        channel = broker.channel()
        channel.queue_unbind(queue, "nomly.events", routing_key="order_delivered")
        # Only this verification run owns the queue; discard arrivals outside the captured sample.
        channel.queue_delete(queue, if_unused=True)
    finally:
        broker.close()
    return {
        **evidence,
        "ingestion": ingestion,
        "replay": replay,
        "identical_after_replay": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-seconds",
        type=float,
        default=30,
        help="Capture from the running harness; 0 skips live evidence",
    )
    args = parser.parse_args()
    if args.live_seconds < 0:
        parser.error("--live-seconds cannot be negative")
    prefix = "verify_" + uuid.uuid4().hex[:10]
    directory = slice.ROOT / "captures" / prefix
    directory.mkdir(parents=True)
    os.environ["NOMLY_RAW_SCHEMA"] = prefix + "_raw"
    os.environ["NOMLY_ANALYTICS_SCHEMA"] = prefix + "_analytics"
    source = slice.load_source()
    evidence = {
        "run": prefix,
        "fixture_raw_schema": source["schema"],
        "fixture_analytics_schema": os.environ["NOMLY_ANALYTICS_SCHEMA"],
    }
    evidence["fixture"] = fixture_check(source, directory, prefix)
    evidence["commit_before_ack"] = recovery_check(source, prefix)
    evidence["negative_configuration_sql_contract_checks"] = negative_config_checks()
    if args.live_seconds:
        os.environ["NOMLY_RAW_SCHEMA"] = prefix + "_live_raw"
        os.environ["NOMLY_ANALYTICS_SCHEMA"] = prefix + "_live_analytics"
        source = slice.load_source()
        evidence["live_raw_schema"] = source["schema"]
        evidence["live_analytics_schema"] = os.environ["NOMLY_ANALYTICS_SCHEMA"]
        evidence["live"] = live_check(source, directory, args.live_seconds)
    path = directory / "evidence.json"
    path.write_text(json.dumps(evidence, default=str, indent=2) + "\n")
    print(path.read_text())
    print(f"PASS: evidence saved to {path}")


if __name__ == "__main__":
    main()
