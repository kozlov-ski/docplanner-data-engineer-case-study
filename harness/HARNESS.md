# The harness

Everything you need is here. One command.

```bash
docker compose up -d --wait
```

That gives you RabbitMQ with a live stream of Nomly order events, Postgres
pre-seeded with the dimension tables, plus MinIO and Trino. Compose 2.30+ startup hooks
create `lakehouse.bronze.raw_order_events` automatically; the separate consumer populates it.

Both are here because they start in one command — not because they're the architecture
we're proposing. What you'd actually build is the design document's question.

## Connect

| | |
|---|---|
| RabbitMQ (AMQP) | `amqp://guest:guest@localhost:5672/` |
| Postgres | `postgresql://nomly:nomly@localhost:5432/nomly` |
| Trino | `http://localhost:8080` — catalog `lakehouse`, schema `bronze` |
| MinIO S3 | `http://localhost:9000` — `nomlyadmin` / `nomly-local-only` |

## Poke at it in a browser

You don't need to install a client to see what's going on.

| | |
|---|---|
| **RabbitMQ UI** | http://localhost:15672 — `guest` / `guest` |
| **Adminer** (Postgres UI) | http://localhost:8081 — user `nomly`, password `nomly`, database `nomly` |
| **MinIO console** | http://localhost:9001 — `nomlyadmin` / `nomly-local-only` |
| **Trino UI** | http://localhost:8080 — user `ingestion` |

In Adminer the server field is pre-filled; you get a table browser and a SQL console.

In the RabbitMQ UI, *Queues and Streams → orders* shows the depth and the live publish
rate, and the **Get messages** panel lets you read payloads without writing a consumer.
Two things to know about it:

- Leave **Ack Mode** on `Nack message requeue true`. If you set it to `Automatic ack`,
  every message you look at is **permanently deleted**.
- `orders` is FIFO and the backlog grows, so its head is the *oldest* events — you will
  see startup-era `order_placed` messages, not the current moment.

To watch the live edge in the browser instead, give yourself your own queue: *Add a new
queue* → name it `peek` → open it → **Bindings** → *From exchange* `nomly.events`,
*Routing key* `#` → **Bind**. Now **Get messages** on `peek` shows what is being
published right now, with all event types mixed together, and `orders` stays untouched.
Use a routing key of `order_delivered` instead of `#` to subscribe to a single event type.

If either port is already taken on your machine, change the left-hand number in
`docker-compose.yml` — `"8081:8080"` becomes `"9081:8080"` and so on.

Events go to the topic exchange **`nomly.events`**, routing key = event type. There is
already a durable queue **`orders`** bound to `#`, so you can start consuming without
declaring anything:

```python
import pika
ch = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@localhost:5672/")).channel()
for method, props, body in ch.consume("orders"):
    print(body)
    ch.basic_ack(method.delivery_tag)
```

## The events

| Event type | Fields beyond `event_id`, `event_type`, `order_id`, `occurred_at` |
|---|---|
| `order_placed` | `restaurant_id`, `city`, `items_count`, `total_amount` |
| `courier_assigned` | `courier_id`, `algo_version` (`v1` or `v2`) |
| `order_picked_up` | `courier_id` |
| `order_delivered` | — |
| `order_cancelled` | `reason`, `cancelled_by` |

```json
{"event_id": "9f3c...", "event_type": "order_placed", "order_id": "a1b2...",
 "occurred_at": "2026-09-11T09:14:00Z", "restaurant_id": 23, "city": "Warsaw",
 "items_count": 3, "total_amount": 47.5}
```

## Tables in Postgres

`zones(zone_id, city, name)` · `restaurants(restaurant_id, name, zone_id, cuisine, tier, commission_pct)` · `couriers(courier_id, vehicle_type, city, hired_at, status)`

These are the slow-moving dimensions. `restaurants.zone_id` is how you get from an event
to a zone.

Trino exposes these lookups through `postgres.public`. After a successful dbt build,
join delivery detail to current restaurant/zone values with:

```bash
docker compose -f harness/docker-compose.yml exec -T trino trino --file /lakehouse/delivery_lookup.sql
```

This exploratory query scans raw `order_placed` events for the restaurant mapping;
missing, invalid or conflicting mappings leave lookup columns null. It preserves delivery
rows but does not establish historical attribution or validated order facts.

## Two things to know

**The clock runs at 60x.** One real second is one simulated minute, so a few minutes of
running gives you a few hours of order history and you don't have to wait 40 real minutes
to see a delivery complete. The `occurred_at` timestamps are internally consistent —
treat them as normal timestamps.

**The stream is not clean.** On purpose, and in the ways a real one isn't: at-least-once
delivery means some events are published more than once, events interleave and arrive out
of order, a courier app periodically drops offline for an hour and then flushes everything
it buffered at one instant, roughly 1% of payloads are malformed, and about 2% of orders never reach a
terminal state. You do not have to handle all of it in the slice you build — but we'd
like to read what you'd do about it.

## Starting over

Use `docker compose stop` and `docker compose up -d --wait` to retain the current
environment. The original Postgres volume is anonymous: `down`/`up` can detach it,
leaving MinIO objects without their Iceberg catalog. Both stores must be retained together.
`docker compose down -v` intentionally destroys attached data volumes.

## Raw lakehouse ingestion

From the repository root:

```bash
uv run --locked harness/ingestion/consume.py --seconds 30
```

The dedicated `lakehouse.raw_order_events` queue receives all new event types, leaving
`orders` untouched. Raw bytes and broker metadata land through Trino in Iceberg v2/Parquet,
partitioned by UTC ingestion day in MinIO. Acknowledgements follow the completed batch
INSERT; duplicate deliveries and malformed messages remain in raw for later transformation.
The consumer supports continuous runs (omit `--seconds`) and does not depend on dbt.

Batching defaults to 500 messages or five seconds. Use `--batch-size`, `--flush-seconds`,
`--seconds` and `--limit` to adjust it. Override `AMQP_URL`, `TRINO_HOST`, `TRINO_PORT`
and `TRINO_USER` for other connections. Python scripts carry PEP 723 dependency metadata
and adjacent `.py.lock` files; `uv run --locked` needs no `pyproject.toml` or manual setup.

The raw table stores `payload` bytes, `ingested_at`, `exchange`, `routing_key`,
`redelivered` and `batch_id`. Partition dates use ingestion time, not event occurrence time.
Postgres stores Iceberg catalog pointers in `iceberg_catalog`; event data lives in MinIO.
The harness producer publishes non-persistent messages, so durable queues alone do not
guarantee source durability across broker failures. Failed or uncertain writes remain
unacknowledged; restart the consumer and expect possible duplicates after lost acknowledgements.

## Inspect the landing layer

From the repository root, `just check` checks recent receipts and an actual Parquet object.
For SQL inspection:

```bash
docker compose -f harness/docker-compose.yml exec trino trino
```

```sql
SELECT routing_key, count(*) AS messages,
       min(ingested_at) AS first_receipt, max(ingested_at) AS last_receipt
FROM lakehouse.bronze.raw_order_events
GROUP BY routing_key ORDER BY routing_key;

SELECT * FROM lakehouse.bronze."raw_order_events$partitions";
SELECT file_format, file_path, record_count
FROM lakehouse.bronze."raw_order_events$files";

-- Diagnostic extraction; malformed data remains in the raw table.
SELECT routing_key, try(json_parse(from_utf8(payload))) AS event
FROM lakehouse.bronze.raw_order_events LIMIT 10;
```

## Verification

```bash
uv run --locked harness/ingestion/verify.py
uv run --locked harness/ingestion/verify_startup.py
```

The first check uses a disposable queue/table for exact-byte preservation, all event types,
malformed input, duplicates, receipt-day partitions, failed writes, interrupted acknowledgements
and heartbeats during slow writes. The second uses isolated containers and volumes to check
fresh startup, invalid SQL, incompatible schemas, restarts and the `just nuke` recipe.

Startup hooks have bounded waits and reject incompatible table definitions. Inspect failures:

```bash
docker compose -f harness/docker-compose.yml logs postgres minio trino
```

The root `just stop` preserves containers and data. `just nuke` deletes this Compose project's
containers and attached volumes, including Postgres, RabbitMQ and MinIO data. Stop the foreground
consumer first. It does not prune unrelated Docker resources or remove previously detached volumes.

## Delivery-event counts with dbt

Run `just dbt` from the repository root after landing some events. It installs the locked
Python 3.12 dbt environment and runs `dbt build` against Trino; ingestion can continue.
Run only one dbt build at a time. Each build appends new delivery-candidate batches to
the Iceberg table `lakehouse.analytics.stg_nomly__delivery_inputs`. The view
`stg_nomly__deliveries` validates/deduplicates captured detail; `delivery_counts` rebuilds fully.
Candidates have routing key or JSON event type `order_delivered`; unrelated events stay in Bronze.

Valid events require a UTF-8 JSON object, UUID event/order IDs, `order_delivered`, and an
RFC 3339 occurrence timestamp with explicit timezone and at most six fractional digits.
Higher timestamp precision is rejected rather than rounded across a window boundary.
Bad candidates retain their original bytes and rejection reason in the input table.
Identical bytes deduplicate by event ID, preserving the earliest receipt. Different bytes for
the same parseable ID—including formatting changes and invalid variants—fail the upstream test.

Use **`dbt build`, not `dbt run`**: the conflict test must pass before replacing counts.
A failure leaves the previous count table stale and queryable; this prototype has no atomic
multi-table publication gate. Inspect the command result before using its report. Additional
fields stay in raw bytes; no lateness, unique-order or zone/algorithm metric is inferred.

```sql
SELECT window_start, delivered_events
FROM lakehouse.analytics.delivery_counts
ORDER BY window_start;

SELECT rejection_reason, count(*)
FROM lakehouse.analytics.stg_nomly__delivery_inputs
WHERE rejection_reason IS NOT NULL
GROUP BY 1;

SELECT event_id, count(DISTINCT payload) AS variants
FROM lakehouse.analytics.stg_nomly__delivery_inputs
WHERE event_id IS NOT NULL
GROUP BY 1 HAVING count(DISTINCT payload) > 1;
```

`dbt_project/analyses/chiara_delivery_counts.sql` provides a bounded report with `window_start`
and `window_end` variables. Windows are half-open, based on UTC occurrence time; missing windows
mean zero deliveries. Input capture uses incremental `append`, scanning only the **current and
previous UTC ingestion-day partitions** by default, in both Bronze and captured inputs.
`delivery_input_lookback_days` controls the number of days; `delivery_input_as_of_date` is the last
included UTC day and defaults to the dbt run date. Bounds are inclusive midnight at the start and
exclusive midnight after the last day. The input table is partitioned by `day(ingested_at)`.

```bash
just dbt '{"delivery_input_lookback_days":1}'  # Current UTC partition only.
just dbt '{"delivery_input_as_of_date":"2026-01-01","delivery_input_lookback_days":7}'  # Historical backfill.
```

Capture excludes `(batch_id, UTC ingestion day)` pairs already present in the input table.
The day is part of the checkpoint so one batch crossing midnight can be captured over two runs.
Each consumer batch has a fresh ID and commits all its messages in one INSERT;
**batch IDs must never be reused or extended later**. Capturing rows and their batch IDs is one
Iceberg commit, so retries do not append the same batch again. New replay batches retain duplicate
messages and rejected candidates; the detail view deduplicates valid event IDs and finds first receipts.
Event time never filters discovery: an old delivery received today is included. Receipt dates outside
the window are deferred until an explicit backfill or wider lookback. After a dbt outage longer than
the window, backfill the missed dates before treating counts as complete. Replayed rows with their
original old receipt timestamps need the same backfill. Timestamp maxima are not used as checkpoints.

Within selected partitions, batches with no candidates may be rescanned because they have no captured
rows. Conflict tests still scan accumulated inputs and counts still rebuild fully. Use one dbt writer.
The existing input table is reused; delivery detail automatically converts back to a view if necessary.
Temporary incremental relations use tables rather than views. Source corrections/deletions and
validation/SQL changes require a full refresh with the complete intended raw history. First creation
and full refresh intentionally read all retained history; window bounds apply only to incremental runs.
Existing unpartitioned input tables need this one-time full refresh to rewrite their physical layout:

```bash
uv run --locked --project dbt_project dbt build --full-refresh --project-dir dbt_project --profiles-dir dbt_project
```
This counts unique delivery event IDs, not unique orders. The final Postgres sink is deferred.

Connection overrides: `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG` (default `lakehouse`),
`NOMLY_RAW_SCHEMA` (default `bronze`) and `NOMLY_ANALYTICS_SCHEMA` (default `analytics`).
No dbt declarations control the all-events RabbitMQ consumer.

```bash
just dbt-test    # Existing models only; does not ingest or rebuild.
just dbt-verify  # Build/test live models, then run the full isolated verification.
```

The full verification requires a running local Docker harness with some valid delivery events;
its recipe builds live models first and the script checks that live data reconciles before creating fixtures.
Verification creates disposable schemas, checks duplicates,
replay, rejected payloads, delayed/boundary events, conflicts, stable input capture and invalid
SQL/YAML/contracts, then independently reconciles the live captured bytes with staged rows and
window counts. The fixture script does not stop ingestion or alter live models; avoid concurrent dbt builds.
Evidence is written to `dbt_project/target/lakehouse_evidence.json` after cleanup succeeds.
It also checks delivery detail's table-to-view migration, actual incremental INSERT execution,
unchanged input files on no-op/failed-build retries, preservation of duplicates across replay batches,
window bounds, explicit old backfills, midnight-spanning batches, earlier receipt corrections and
full-refresh equivalence. Trino's query plan must show date constraints on both raw and input scans.

`verify_<id>_raw/` and `verify_<id>_analytics/` are temporary object prefixes inside the
single `nomly-lakehouse` bucket, not additional buckets. Dropping an Iceberg view leaves
metadata objects behind, so verification drops its schemas and then deletes only its own
exact prefixes. Cleanup verifies that no objects remain. An interrupted run can still leave
artifacts; check that its catalog schemas are gone before removing that run's prefixes.
Never delete `bronze/` or `analytics/` directly: those contain the live Iceberg tables.

If something here doesn't work, tell us — that's our bug, not yours.
