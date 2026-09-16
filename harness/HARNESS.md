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

If something here doesn't work, tell us — that's our bug, not yours.
