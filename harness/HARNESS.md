# The harness

Everything you need is here. One command.

```bash
docker compose up
```

That gives you RabbitMQ with a live stream of Nomly order events, and Postgres
pre-seeded with the dimension tables. Nothing else to install or configure.

Both are here because they start in one command — not because they're the architecture
we're proposing. What you'd actually build is the design document's question.

## Connect

| | |
|---|---|
| RabbitMQ (AMQP) | `amqp://guest:guest@localhost:5672/` |
| Postgres | `postgresql://nomly:nomly@localhost:5432/nomly` |

## Poke at it in a browser

You don't need to install a client to see what's going on.

| | |
|---|---|
| **RabbitMQ UI** | http://localhost:15672 — `guest` / `guest` |
| **Adminer** (Postgres UI) | http://localhost:8081 — user `nomly`, password `nomly`, database `nomly` |

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

```bash
docker compose down -v && docker compose up
```

If something here doesn't work, tell us — that's our bug, not yours.
