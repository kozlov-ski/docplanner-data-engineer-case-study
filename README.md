# Data Engineering case study

Hello 👋 — everything you need for the exercise is in this repo.

| | |
|---|---|
| `CASE_STUDY.md` | The brief. Start here. |
| `harness/` | The environment you build against. `docker compose up` and you're running. |

## Quick start

```bash
cd harness && docker compose up
```

That gives you RabbitMQ with a live stream of order events and Postgres pre-seeded with
the dimension tables, in about six seconds. Both have a browser UI:

| | |
|---|---|
| RabbitMQ | http://localhost:15672 — `guest` / `guest` |
| Adminer (Postgres) | http://localhost:8081 — `nomly` / `nomly` / `nomly` |

`harness/HARNESS.md` has the connection strings, the event schema, the table definitions
and a four-line consumer to get you started.

## A note on the harness code

`harness/producer.py` is not a black box — read it if you like. It will show you how the
events are generated, but it will not answer any of the questions in the brief, because
none of them ask you to guess a property of the data. We're asking what you would build,
and whether the numbers your pipeline produces can be trusted.

If anything here doesn't work, tell us — that's our bug, not yours.
