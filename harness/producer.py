"""Nomly order-event producer. Publishes to the `nomly.events` topic exchange.

The simulated clock runs 60x wall clock: one real second is one simulated minute,
so a few minutes of running produces a few hours of order history.

Deliberately imperfect, in the same ways a real queue is:
  - at-least-once delivery, so some events are published twice
  - events interleave and arrive out of order
  - couriers go offline and their buffered events all land at once
  - a small number of payloads are malformed
  - some orders never reach a terminal state
"""
import heapq, json, os, random, time, uuid
from datetime import datetime, timedelta, timezone

import pika

SPEED = 60                  # simulated minutes per real second
ORDERS_PER_SECOND = 2
EXCHANGE = "nomly.events"
QUEUE = "orders"            # pre-bound to #, so you can consume immediately

ZONE_CITY = {1: "Warsaw", 2: "Warsaw", 3: "Warsaw",
             4: "Krakow", 5: "Krakow", 6: "Gdansk"}
CANCEL_REASONS = ["restaurant_closed", "no_courier", "customer_changed_mind", "address_invalid"]

# Same formulas as seed.sql, so joins line up.
COURIERS_BY_CITY = {}
for cid in range(1, 61):
    city = ["Warsaw", "Warsaw", "Warsaw", "Krakow", "Krakow", "Gdansk"][cid % 6]
    COURIERS_BY_CITY.setdefault(city, []).append(cid)

_sim_epoch = datetime.now(timezone.utc).replace(microsecond=0)
_real_epoch = time.monotonic()


def sim_now():
    return _sim_epoch + timedelta(seconds=(time.monotonic() - _real_epoch) * SPEED)


def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def connect():
    """Retry until RabbitMQ accepts us, so `docker compose up` just works."""
    url = os.environ.get("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/")
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(url))
            ch = conn.channel()
            ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
            ch.queue_declare(QUEUE, durable=True)
            ch.queue_bind(QUEUE, EXCHANGE, routing_key="#")
            return conn, ch
        except pika.exceptions.AMQPConnectionError:
            print("waiting for rabbitmq...", flush=True)
            time.sleep(2)


class Scheduler:
    """Events are queued to publish at a wall-clock deadline, which is what makes
    them interleave and arrive out of order."""

    def __init__(self):
        self._heap = []
        self._seq = 0
        self.hold_release = None   # set during a courier-offline window

    def add(self, delay_real, event):
        # A courier app that is offline buffers everything and flushes it in one go.
        if self.hold_release and event["event_type"] in ("order_picked_up", "order_delivered"):
            delay_real = max(delay_real, self.hold_release - time.monotonic())
        self._seq += 1
        heapq.heappush(self._heap, (time.monotonic() + delay_real, self._seq, event))
        if random.random() < 0.03:   # at-least-once: same event_id, published twice
            self._seq += 1
            heapq.heappush(self._heap, (time.monotonic() + delay_real + random.uniform(0.5, 4), self._seq, event))

    def due(self):
        now = time.monotonic()
        while self._heap and self._heap[0][0] <= now:
            yield heapq.heappop(self._heap)[2]


def corrupt(event):
    """1% of payloads are broken. Your pipeline should survive all three shapes."""
    broken = dict(event)
    choice = random.choice(["no_order_id", "bad_timestamp", "unknown_type"])
    if choice == "no_order_id":
        broken["order_id"] = None
    elif choice == "bad_timestamp":
        broken["occurred_at"] = "N/A"
    else:
        broken["event_type"] = "order_teleported"
    return broken


def schedule_order(sched):
    order_id = str(uuid.uuid4())
    restaurant_id = random.randint(1, 40)
    zone_id = 1 + (restaurant_id % 6)
    city = ZONE_CITY[zone_id]
    courier_id = random.choice(COURIERS_BY_CITY[city])

    # Dispatch v2 handles 20% of Warsaw orders and nothing elsewhere.
    algo_version = "v2" if city == "Warsaw" and random.random() < 0.20 else "v1"

    minutes_to_deliver = max(12.0, random.gauss(32, 7))
    if algo_version == "v2":
        minutes_to_deliver += 14 if zone_id == 3 else -6

    placed_at = sim_now()

    def emit(event_type, offset_minutes, **fields):
        occurred_at = placed_at + timedelta(minutes=offset_minutes)
        event = {"event_id": str(uuid.uuid4()), "event_type": event_type,
                 "order_id": order_id, "occurred_at": iso(occurred_at), **fields}
        if random.random() < 0.01:
            event = corrupt(event)
        # jitter, so publish order does not match occurred_at order
        sched.add(offset_minutes / SPEED * 60 + random.uniform(0, 1.5), event)

    emit("order_placed", 0, restaurant_id=restaurant_id, city=city,
         items_count=random.randint(1, 6), total_amount=round(random.uniform(18, 95), 2))

    if random.random() < 0.06:     # cancelled instead of delivered
        emit("order_cancelled", round(random.uniform(1, minutes_to_deliver), 1),
             reason=random.choice(CANCEL_REASONS),
             cancelled_by=random.choice(["customer", "restaurant", "system"]))
        return

    emit("courier_assigned", round(minutes_to_deliver * 0.08, 1),
         courier_id=courier_id, algo_version=algo_version)
    emit("order_picked_up", round(minutes_to_deliver * 0.45, 1), courier_id=courier_id)

    if random.random() < 0.02:     # 2% of orders never reach a terminal state
        return
    emit("order_delivered", round(minutes_to_deliver, 1))


def main():
    random.seed()
    conn, ch = connect()
    print(f"publishing to exchange '{EXCHANGE}', pre-bound queue '{QUEUE}'", flush=True)

    sched = Scheduler()
    published = 0
    next_order = time.monotonic()
    next_outage = time.monotonic() + random.uniform(120, 180)

    while True:
        now = time.monotonic()

        # Periodically the courier app drops offline for an hour of simulated time,
        # then flushes everything it buffered at one instant.
        if sched.hold_release and now >= sched.hold_release:
            print("courier app back online, flushing buffered events", flush=True)
            sched.hold_release = None
            next_outage = now + random.uniform(240, 420)
        elif not sched.hold_release and now >= next_outage:
            sched.hold_release = now + random.uniform(60, 90)
            print("courier app offline, buffering events", flush=True)

        for event in sched.due():
            ch.basic_publish(EXCHANGE, event.get("event_type") or "unknown",
                             json.dumps(event),
                             pika.BasicProperties(content_type="application/json"))
            published += 1
            if published % 500 == 0:
                print(f"{published} events published (simulated time {iso(sim_now())})", flush=True)

        while now >= next_order:
            schedule_order(sched)
            next_order += 1 / ORDERS_PER_SECOND

        conn.process_data_events(0)
        time.sleep(0.05)


if __name__ == "__main__":
    main()
