# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = []
# ///

"""Startup/restart checks in a disposable Compose project with no published ports."""

import json
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import uuid


ROOT = Path(__file__).resolve().parents[2]


def main():
    project = "nomly_init_" + uuid.uuid4().hex[:10]
    config = json.loads(subprocess.check_output(
        ["docker", "compose", "-f", str(ROOT / "harness/docker-compose.yml"), "config", "--format", "json"], text=True))
    config.pop("name", None)
    config["services"] = {k: v for k, v in config["services"].items() if k in ("postgres", "minio", "trino")}
    config["volumes"] = {"minio-data": {}}
    config["networks"] = {"default": {}}
    with tempfile.TemporaryDirectory(prefix=project) as directory:
        temp = Path(directory)
        shutil.copytree(ROOT / "harness/lakehouse", temp / "lakehouse")
        for service in config["services"].values():
            service["ports"] = []
            for volume in service["volumes"]:
                if volume["type"] == "bind" and "/lakehouse" in volume["target"]:
                    original = Path(volume["source"])
                    relative = original.relative_to(ROOT / "harness/lakehouse")
                    volume["source"] = str(temp / "lakehouse" / relative)
        compose_file = temp / "compose.json"
        compose_file.write_text(json.dumps(config))
        command = ["docker", "compose", "-p", project, "-f", str(compose_file)]

        def run(*args, success=True):
            result = subprocess.run([*command, *args], text=True, capture_output=True, timeout=300)
            if success and result.returncode:
                raise AssertionError(result.stdout + result.stderr)
            return result

        def sql(statement):
            return run("exec", "-T", "trino", "trino", "--output-format", "CSV_UNQUOTED",
                       "--execute", statement).stdout.strip()

        def recipe(name):
            return subprocess.run(
                ["just", "--justfile", str(ROOT / "justfile"), "--set", "compose",
                 shlex.join(command), name], text=True, capture_output=True, timeout=300)

        try:
            raw_sql = temp / "lakehouse/raw.sql"
            original_sql = raw_sql.read_text()
            raw_sql.write_text("THIS IS INVALID SQL;\n")
            failed = run("up", "-d", "--wait", "--wait-timeout", "240", success=False)
            assert failed.returncode != 0, "Invalid initialization SQL incorrectly reported success"
            logs = run("logs", "trino").stdout
            assert "mismatched input" in logs, failed.stdout + failed.stderr + logs
            unhealthy = run("exec", "-T", "trino", "/bin/sh", "/lakehouse/trino.sh", "check", success=False)
            assert unhealthy.returncode != 0
            print("PASS: invalid startup SQL fails Compose and readiness", flush=True)

            raw_sql.write_text(original_sql)
            run("up", "-d", "--wait", "--wait-timeout", "240", "--force-recreate", "trino")
            assert sql("SELECT count(*) FROM lakehouse.bronze.raw_order_events") == "0"
            sql("INSERT INTO lakehouse.bronze.raw_order_events VALUES "
                "(X'00FF', TIMESTAMP '2026-01-01 00:00:00 UTC', 'verify', 'startup', false, 'startup-check')")
            before = sql("SELECT to_hex(payload), ingested_at, batch_id FROM lakehouse.bronze.raw_order_events")
            print("PASS: fresh storage creates queryable table without running ingestion", flush=True)

            stale = recipe("check")
            assert stale.returncode != 0 and "No recent receipts" in stale.stderr, stale.stdout + stale.stderr
            print("PASS: just check rejects old rows as proof of live ingestion", flush=True)

            run("up", "-d", "--wait", "--wait-timeout", "240")
            assert sql("SELECT to_hex(payload), ingested_at, batch_id FROM lakehouse.bronze.raw_order_events") == before
            run("restart", "postgres", "minio", "trino")
            run("up", "-d", "--wait", "--wait-timeout", "240")
            assert sql("SELECT to_hex(payload), ingested_at, batch_id FROM lakehouse.bronze.raw_order_events") == before
            print("PASS: repeated startup and service restart preserve stored bytes", flush=True)

            stopped = recipe("stop")
            assert stopped.returncode == 0, stopped.stdout + stopped.stderr
            run("up", "-d", "--wait", "--wait-timeout", "240")
            assert sql("SELECT to_hex(payload), ingested_at, batch_id FROM lakehouse.bronze.raw_order_events") == before
            print("PASS: just stop and startup preserve stored bytes", flush=True)

            sql("ALTER TABLE lakehouse.bronze.raw_order_events ADD COLUMN unexpected varchar")
            failed = run("up", "-d", "--wait", "--wait-timeout", "240", "--force-recreate", "trino", success=False)
            assert failed.returncode != 0, "Incompatible table incorrectly reported ready"
            logs = run("logs", "trino").stdout
            assert "Incompatible raw_order_events columns" in logs, logs
            print("PASS: incompatible existing table fails initialization", flush=True)

            containers = run("ps", "-aq").stdout.split()
            inspected = json.loads(subprocess.check_output(["docker", "inspect", *containers], text=True))
            volumes = {mount["Name"] for container in inspected for mount in container["Mounts"]
                       if mount["Type"] == "volume"}
            assert volumes, "Expected disposable data volumes"
            nuked = recipe("nuke")
            assert nuked.returncode == 0, nuked.stdout + nuked.stderr
            assert not run("ps", "-aq").stdout.strip()
            for volume in volumes:
                assert subprocess.run(["docker", "volume", "inspect", volume], capture_output=True).returncode != 0
            run("up", "-d", "--wait", "--wait-timeout", "240")
            assert sql("SELECT count(*) FROM lakehouse.bronze.raw_order_events") == "0"
            empty = recipe("check")
            assert empty.returncode != 0 and "No recent receipts" in empty.stderr, empty.stdout + empty.stderr
            print("PASS: just nuke removes test volumes; startup recreates an empty landing table", flush=True)
            print("PASS: all lifecycle checks completed in isolated project " + project, flush=True)
        finally:
            # This project's generated volumes contain only the test fixture, never harness data.
            run("down", "-v", "--remove-orphans")


if __name__ == "__main__":
    main()
