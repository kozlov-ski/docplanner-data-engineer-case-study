"""Docker-backed dbt checks; isolated schemas, plus read-only reconciliation of live analytics.

Run: just dbt-verify
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
import sys
import tempfile
import uuid

from trino.dbapi import connect

ROOT = Path(__file__).resolve().parent
CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")
assert re.fullmatch(r"[a-z_][a-z0-9_]*", CATALOG)
UTC = timezone.utc


def query(sql, parameters=None):
    connection = connect(host=os.getenv("TRINO_HOST", "localhost"),
                         port=int(os.getenv("TRINO_PORT", "8080")),
                         user=os.getenv("TRINO_USER", "dbt_verify"), timezone="UTC",
                         request_timeout=30, max_attempts=1)
    try:
        return connection.cursor().execute(sql, parameters).fetchall()
    finally:
        connection.close()


def dbt(env, *args, project=ROOT, success=True):
    if project == ROOT and "--vars" not in args:
        args = (*args, "--vars", '{"delivery_input_as_of_date":"2026-01-01"}')
    result = subprocess.run(
        [str(Path(sys.executable).parent / "dbt"), *args,
         "--project-dir", str(project), "--profiles-dir", str(ROOT)],
        env=env, cwd=ROOT, text=True, capture_output=True, timeout=180)
    if success:
        assert result.returncode == 0, result.stdout + result.stderr
    else:
        assert result.returncode != 0, "Expected dbt failure: " + result.stdout
    return result


def invalid_json_constant(value):
    raise ValueError(value)


def reconcile(schema):
    """Independent Python oracle reads bytes, never dbt's extracted fields."""
    prefix = f"{CATALOG}.{schema}"
    raw = query(f"SELECT payload, ingested_at FROM {prefix}.stg_nomly__delivery_inputs")
    accepted, rejected = {}, 0
    for payload, ingested in raw:
        try:
            event = json.loads(payload.decode("utf-8"), parse_constant=invalid_json_constant)
            assert isinstance(event, dict)
            event_id = str(uuid.UUID(event["event_id"]))
            order_id = str(uuid.UUID(event["order_id"]))
            assert event["event_type"] == "order_delivered"
            timestamp = event["occurred_at"]
            assert re.fullmatch(r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d{1,6})?([Zz]|[+-]\d{2}:\d{2})", timestamp)
            occurred = datetime.fromisoformat(timestamp.upper().replace("Z", "+00:00"))
            assert occurred.tzinfo is not None
            occurred = occurred.astimezone(UTC)
        except (ValueError, TypeError, KeyError, AssertionError, UnicodeError, AttributeError):
            rejected += 1
            continue
        previous = accepted.get(event_id)
        assert previous is None or previous[:2] == (order_id, occurred)
        accepted[event_id] = (order_id, occurred, min(ingested, previous[2]) if previous else ingested)
    expected_rows = sorted((key, *values) for key, values in accepted.items())
    actual_rows = query(f"SELECT event_id, order_id, occurred_at, ingested_at FROM {prefix}.stg_nomly__deliveries ORDER BY event_id")
    assert [tuple(row) for row in actual_rows] == expected_rows
    counts = Counter(value[1].replace(minute=value[1].minute // 5 * 5, second=0, microsecond=0)
                     for value in accepted.values())
    actual_counts = query(f"SELECT window_start, delivered_events FROM {prefix}.delivery_counts ORDER BY window_start")
    assert [tuple(row) for row in actual_counts] == sorted(counts.items())
    actual_rejected = query(f"SELECT count(*) FROM {prefix}.stg_nomly__delivery_inputs WHERE rejection_reason IS NOT NULL")[0][0]
    assert actual_rejected == rejected
    return {"captured_candidates": len(raw), "accepted_ids": len(accepted),
            "rejected_candidates": rejected,
            "window_start": min(counts).isoformat() if counts else None,
            "window_end": (max(counts) + timedelta(minutes=5)).isoformat() if counts else None,
            "query_output": [[time.isoformat(), count] for time, count in actual_counts]}


def main():
    # Fail before creating test storage when the required live build is absent or inconsistent.
    live_schema = os.getenv("NOMLY_ANALYTICS_SCHEMA", "analytics")
    assert re.fullmatch(r"[a-z_][a-z0-9_]*", live_schema), "Invalid analytics schema"
    assert reconcile(live_schema)["accepted_ids"] > 0, "Run just dbt against a non-empty live sample first"
    suffix = uuid.uuid4().hex[:10]
    raw_schema, analytics = f"verify_{suffix}_raw", f"verify_{suffix}_analytics"
    env = {**os.environ, "NOMLY_RAW_SCHEMA": raw_schema, "NOMLY_ANALYTICS_SCHEMA": analytics,
           "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    table = f"{CATALOG}.{raw_schema}.raw_order_events"
    base = datetime(2026, 1, 1, 10, tzinfo=UTC)

    def event(i, **changes):
        value = {"event_id": str(uuid.UUID(int=i + 1)), "order_id": str(uuid.UUID(int=i + 1000)),
                 "event_type": "order_delivered", "occurred_at": (base + timedelta(seconds=i * 7)).isoformat(),
                 "additional": {"retained": True}}
        value.update(changes)
        return value

    def insert(bodies, batch=None, received_at=None):
        batch = batch or uuid.uuid4().hex
        values = ",".join(["(?,?,?,?,?,?)"] * len(bodies))
        args = [value for body, routing in bodies for value in
                (body, received_at or base + timedelta(hours=1), "verify", routing, False, batch)]
        query(f"INSERT INTO {table} VALUES {values}", args)

    def body(value, route="order_delivered"):
        return json.dumps(value).encode(), route

    def snapshot():
        return query(f"SELECT event_id, order_id, occurred_at, ingested_at FROM {CATALOG}.{analytics}.stg_nomly__deliveries ORDER BY event_id")

    def counts():
        return query(f"SELECT * FROM {CATALOG}.{analytics}.delivery_counts ORDER BY window_start")

    def files():
        return query(f'SELECT file_path FROM {CATALOG}.{analytics}."stg_nomly__delivery_inputs$files" ORDER BY file_path')

    def inputs():
        return query(f"SELECT * FROM {CATALOG}.{analytics}.stg_nomly__delivery_inputs "
                     "ORDER BY batch_id, ingested_at, to_hex(payload)")

    def assert_append():
        sql = (ROOT / "target/run/nomly/models/staging/stg_nomly__delivery_inputs.sql").read_text()
        assert "insert into" in sql.lower(), "Expected an incremental INSERT, not table replacement"

    try:
        for schema in (raw_schema, analytics):
            query(f"CREATE SCHEMA {CATALOG}.{schema} "
                  f"WITH (location = 's3://nomly-lakehouse/{schema}/')")
        ddl = (ROOT.parent / "harness/lakehouse/raw.sql").read_text().split(";", 1)[1]
        query(ddl.replace("lakehouse.bronze.raw_order_events", table).strip().rstrip(";"))
        events = [event(i) for i in range(100)]
        for i, timestamp in enumerate(("2026-01-01T10:01:00Z", "2026-01-01T10:04:00Z",
                                        "2026-01-01T10:05:00Z", "2026-01-01T10:04:59.999999Z",
                                        "2026-01-01T12:05:00+02:00")):
            events[i]["occurred_at"] = timestamp
        bodies = [body(value) for value in events] + [body(value) for value in events[:5]]
        bodies += [body(event(100, occurred_at="not-a-timestamp"))]
        random.Random(42).shuffle(bodies)
        insert(bodies)
        # Exercise reversal of the mistakenly materialized delivery detail table.
        query(f"CREATE TABLE {CATALOG}.{analytics}.stg_nomly__deliveries AS SELECT "
              "cast(null as varchar) AS event_id, cast(null as varchar) AS order_id, "
              "cast(null as timestamp(6) with time zone) AS occurred_at, "
              "cast(null as timestamp(6) with time zone) AS ingested_at WHERE false")
        dbt(env, "build")
        assert query(f"SELECT table_type FROM {CATALOG}.information_schema.tables "
                     "WHERE table_schema = ? AND table_name = 'stg_nomly__deliveries'", [analytics]) == [["VIEW"]]
        fixture = reconcile(analytics)
        assert fixture["captured_candidates"] == 106
        assert fixture["accepted_ids"] == 100 and fixture["rejected_candidates"] == 1
        before, before_counts = snapshot(), counts()
        before_inputs = inputs()
        before_files = files()
        dbt(env, "build")
        assert_append()
        assert inputs() == before_inputs and files() == before_files
        insert(bodies, "replay", received_at=base + timedelta(hours=2))
        dbt(env, "build")
        assert_append()
        assert snapshot() == before and counts() == before_counts
        assert len(inputs()) == 212, "Raw duplicates must survive replay under a new batch ID"
        assert set(map(tuple, before_files)) <= set(map(tuple, files())), "Appending a batch rewrote old input files"
        assert reconcile(analytics)["rejected_candidates"] == 2
        print("PASS: 100 IDs, duplicates, rejection, replay, shuffled/delayed arrivals and UTC boundaries", flush=True)

        bad = [(b'{"event_type":"order_delivered"', "order_delivered"),
               (b"\xff", "order_delivered"), (b"[]", "order_delivered"),
               body(event(200, order_id=None)), body(event(201, event_id=None)),
               body(event(202, occurred_at="2026-01-01T10:00:00")),
               body(event(203, occurred_at="2026-01-01T10:04:59.9999999Z")),
               body(event(204, event_type="order_placed"))]
        insert(bad)
        insert([body(event(205, event_type="order_placed"), "order_placed")])
        dbt(env, "build")
        assert reconcile(analytics)["rejected_candidates"] == 10
        assert snapshot() == before
        print("PASS: malformed bytes/JSON, invalid identifiers, timestamp and type rejection; other events ignored", flush=True)

        dbt(env, "build", "--select", "stg_nomly__delivery_inputs")
        captured = query(f"SELECT count(*) FROM {CATALOG}.{analytics}.stg_nomly__delivery_inputs")
        insert([body(event(300), "different_route")], "later_arrival")
        dbt(env, "build", "--select", "stg_nomly__deliveries+")
        assert snapshot() == before and counts() == before_counts
        assert query(f"SELECT count(*) FROM {CATALOG}.{analytics}.stg_nomly__delivery_inputs") == captured
        dbt(env, "build")
        assert reconcile(analytics)["accepted_ids"] == 101
        print("PASS: arrival after input capture appears only on the next full build", flush=True)

        # Commits can carry receipts older than the maximum already in staging.
        insert([body(event(301, occurred_at="2025-12-01T10:01:00Z"))], "old_backfill",
               received_at=base - timedelta(days=1))
        insert([body(events[0])], "earlier_receipt", received_at=base - timedelta(days=2))
        dbt(env, "build")
        assert_append()
        assert reconcile(analytics)["accepted_ids"] == 102
        assert query(f"SELECT ingested_at FROM {CATALOG}.{analytics}.stg_nomly__deliveries "
                     "WHERE event_id = ?", [events[0]["event_id"]]) == [[base + timedelta(hours=1)]]
        assert not query(f"SELECT batch_id FROM {CATALOG}.{analytics}.stg_nomly__delivery_inputs "
                         "WHERE batch_id = 'earlier_receipt'"), "Out-of-window receipts were processed"
        dbt(env, "build", "--vars", '{"delivery_input_as_of_date":"2026-01-01","delivery_input_lookback_days":3}')
        assert query(f"SELECT ingested_at FROM {CATALOG}.{analytics}.stg_nomly__deliveries "
                     "WHERE event_id = ?", [events[0]["event_id"]]) == [[base - timedelta(days=2)]]
        incremental_rows, incremental_counts = snapshot(), counts()
        incremental_inputs = inputs()
        before_files = files()
        dbt(env, "build")
        assert_append()
        assert files() == before_files and inputs() == incremental_inputs and snapshot() == incremental_rows
        dbt(env, "build", "--full-refresh")
        assert snapshot() == incremental_rows and counts() == incremental_counts
        assert inputs() == incremental_inputs
        assert reconcile(analytics)["accepted_ids"] == 102
        print("PASS: two-day ingestion window, explicit older backfill, no-op stability and full-refresh equivalence", flush=True)

        # One atomic batch spans midnight: day two must be captured even though day one is known.
        midnight = base.replace(hour=0) + timedelta(days=1)
        query(f"INSERT INTO {table} VALUES (?, ?, 'verify', 'order_delivered', false, 'midnight'), "
              "(?, ?, 'verify', 'order_delivered', false, 'midnight')",
              [body(event(400))[0], midnight - timedelta(microseconds=1),
               body(event(401))[0], midnight])
        dbt(env, "build")
        assert query(f"SELECT count(*) FROM {CATALOG}.{analytics}.stg_nomly__delivery_inputs WHERE batch_id = 'midnight'") == [[1]]
        dbt(env, "build", "--vars", '{"delivery_input_as_of_date":"2026-01-02","delivery_input_lookback_days":1}')
        assert query(f"SELECT count(*) FROM {CATALOG}.{analytics}.stg_nomly__delivery_inputs WHERE batch_id = 'midnight'") == [[2]]
        assert reconcile(analytics)["accepted_ids"] == 104
        assert "day(ingested_at)" in query(f"SHOW CREATE TABLE {CATALOG}.{analytics}.stg_nomly__delivery_inputs")[0][0]
        assert query(f'SELECT count(*) FROM {CATALOG}.{analytics}."stg_nomly__delivery_inputs$partitions"') == [[4]]
        compiled = (ROOT / "target/compiled/nomly/models/staging/stg_nomly__delivery_inputs.sql").read_text()
        plan = json.loads(query("EXPLAIN (TYPE IO, FORMAT JSON) " + compiled)[0][0])
        scans = plan["inputTableColumnInfos"]
        assert {scan["table"]["schemaTable"]["table"] for scan in scans} == {"raw_order_events", "stg_nomly__delivery_inputs"}
        for scan in scans:
            constraint = next(c for c in scan["constraint"]["columnConstraints"] if c["columnName"] == "ingested_at")
            assert constraint["domain"]["ranges"] == [{
                "low": {"value": "2026-01-02 00:00:00.000000 UTC", "bound": "EXACTLY"},
                "high": {"value": "2026-01-03 00:00:00.000000 UTC", "bound": "BELOW"}}]
        print("PASS: latest-partition mode, exclusive upper bound and cross-midnight batch capture", flush=True)
        print("PASS: query plan prunes raw and captured-input scans to one day across four stored partitions", flush=True)

        for variables in ('{"delivery_input_lookback_days":0}', '{"delivery_input_as_of_date":"invalid"}'):
            dbt(env, "parse", "--vars", variables, success=False)
        print("PASS: invalid lookback/date configuration fails", flush=True)

        preserved_counts = counts()
        for conflict in (json.dumps(events[0], indent=2).encode(),
                         json.dumps({**events[0], "order_id": str(uuid.uuid4())}).encode(),
                         json.dumps({**events[0], "occurred_at": "invalid"}).encode()):
            insert([(conflict, "order_delivered")], "conflict")
            failed = dbt(env, "build", success=False)
            assert "delivery_inputs_no_conflicts" in failed.stdout, failed.stdout + failed.stderr
            assert counts() == preserved_counts
            results = json.loads((ROOT / "target/run_results.json").read_text())["results"]
            assert any(r["unique_id"] == "model.nomly.delivery_counts" and r["status"] == "skipped" for r in results)
            blocked_inputs = inputs()
            blocked_files = files()
            dbt(env, "build", success=False)
            assert inputs() == blocked_inputs and files() == blocked_files, "Failed build retry recaptured a batch"
            assert counts() == preserved_counts
            query(f"DELETE FROM {table} WHERE batch_id = 'conflict'")
            # Captured inputs are durable; source corrections require a full refresh.
            dbt(env, "build", "--full-refresh")
            assert counts() == preserved_counts
        print("PASS: changed fields, formatting-only and invalid variants block new counts and preserve old counts", flush=True)

        with tempfile.TemporaryDirectory(prefix="nomly_dbt_negative_") as directory:
            project = Path(directory)
            shutil.copy(ROOT / "dbt_project.yml", project)
            (project / "models").mkdir()
            (project / "models/broken_sql.sql").write_text("select THIS IS INVALID SQL")
            (project / "models/broken_contract.sql").write_text(
                "{{ config(materialized='table', contract={'enforced': true}) }} select cast(1 as bigint) as value")
            contract = project / "models/broken_contract.yml"
            contract.write_text("version: 2\nmodels:\n  - name: broken_contract\n    columns:\n      - name: value\n        data_type: varchar\n")
            for model, marker in (("broken_sql", "syntax"), ("broken_contract", "contract")):
                failure = dbt(env, "build", "--select", model, project=project, success=False)
                assert marker in (failure.stdout + failure.stderr).lower()
            contract.write_text("models: [unterminated")
            dbt(env, "parse", project=project, success=False)
        print("PASS: invalid SQL, contract mismatch and malformed YAML fail", flush=True)

        # Live inputs are already frozen by just dbt; do not change or stop the live consumer.
        live = reconcile(live_schema)
        assert live["accepted_ids"] > 0, "Run just dbt against a non-empty live sample first"
        evidence = {"verified_at": datetime.now(UTC).isoformat(), "fixture": fixture,
                    "replay_unchanged": True, "conflict_blocks_counts": True,
                    "incremental_matches_full_refresh": True, "captured_batches_not_repeated": True,
                    "partition_window_and_backfill": True, "cross_midnight_batch": True,
                    "partition_scan_constraints": True,
                    "stable_build_inputs": True, "negative_checks": True, "live": live}
    finally:
        for name in ("delivery_counts", "stg_nomly__deliveries", "stg_nomly__delivery_inputs"):
            kind = "VIEW" if name == "stg_nomly__deliveries" else "TABLE"
            query(f"DROP {kind} IF EXISTS {CATALOG}.{analytics}.{name}")
        query(f"DROP TABLE IF EXISTS {table}")
        query(f"DROP SCHEMA IF EXISTS {CATALOG}.{analytics}")
        query(f"DROP SCHEMA IF EXISTS {CATALOG}.{raw_schema}")
        # DROP VIEW leaves Iceberg view metadata behind. Only remove this run's exact prefixes,
        # after both dedicated schemas have been dropped successfully; never sweep verify_*.
        mc = ["docker", "compose", "-f", str(ROOT.parent / "harness/docker-compose.yml"),
              "exec", "-T", "minio", "mc"]
        for schema in (raw_schema, analytics):
            assert re.fullmatch(r"verify_[0-9a-f]{10}_(raw|analytics)", schema)
            prefix = f"local/nomly-lakehouse/{schema}/"
            subprocess.run([*mc, "rm", "--recursive", "--force", prefix],
                           check=True, capture_output=True, text=True, timeout=60)
            remaining = subprocess.check_output([*mc, "ls", "--recursive", "--json", prefix],
                                                text=True, timeout=60)
            assert not remaining.strip(), f"Verification objects remain under {prefix}: {remaining}"
        print("PASS: verification schemas and MinIO objects removed", flush=True)
    evidence["test_storage_cleaned"] = True
    path = ROOT / "target/lakehouse_evidence.json"
    path.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({key: value for key, value in evidence.items() if key != "live"}, indent=2), flush=True)
    print(f"Live reconciliation: {live['accepted_ids']} unique deliveries; full output: {path}", flush=True)


if __name__ == "__main__":
    main()
