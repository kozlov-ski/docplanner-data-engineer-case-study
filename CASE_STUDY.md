# Case study - Senior Data Engineering

Hello 👋

We're super happy you got here!

At this stage of our recruitment process we'd like to put in front of you the kind of
problem our Data Platform team actually spends its week on: a product team wants data
they can act on, the request arrives not fully refined, and it's very
likely that whatever you build will be needed by other product teams with slight
differences.

**This is mostly a design exercise.** We're far more interested in how you think about
the problem, the trade-offs you pick, and how you'd explain them to an analyst or a PM
than in how much code you produce. There is a small piece we'd like you to actually
build, and we've done the boring setup for you so you can spend your time on the
interesting part.

**Aim for 2 - 4 hours.** If you find yourself past four, stop and write down what you would
have done instead. We'd rather read a small design you can defend than a large one you
can't.

**On AI:** use it freely — we do, every day. What really matters is how you use AI to
empower your decisions, your design and your choices. And how you will use AI when things
break first at 100x volume (or how to prevent it!).

---

# The setup

You've just joined the Data Platform team at **Nomly**, a food delivery marketplace.

Your team's mandate is to make the data platform genuinely self-service: product teams
and analysts should be able to get the data they need without a data engineer in the
loop for every request. You're not being asked to build one pipeline — you're being
asked to design **the platform and framework that build pipelines**. The Dispatch team is your first
customer.

## What Dispatch is doing

Dispatch is rolling out a new courier-assignment algorithm, **Dispatch v2**, gradually:
it currently handles 20% of orders in Warsaw, and they want to widen it. Every order
event carries the `algo_version` that handled it.

They want to watch the rollout closely enough to stop it if it's making things worse.

## What they actually asked for

### Jira ticket

> **DATA-412 — Real-time dispatch dashboard**
> *Reporter: Bea (PM, Dispatch) · Priority: Highest*
>
> We need a real-time dashboard for the Dispatch v2 rollout. Same numbers as the daily
> report, but live. Can you just dump it into a Google Sheet every few minutes? I'll
> build the chart myself, I already have the template.
>
> Needed before we go to 50% next Tuesday.

### Slack thread in #dispatch-rollout

> **Bea** (PM): the daily report is too slow — by the time we see a problem we've burned
> a whole day of orders 😞 can we get something live?
>
> **Chiara** (Ops lead): for me the thing that matters is late deliveries. if v2 makes
> deliveries later in *any* zone I want to know within minutes, not tomorrow.
>
> **Tomek** (Analyst, Dispatch): honestly if the events just land somewhere I can query,
> I'll write the SQL myself. I'd rather not open a ticket with the platform team every
> time I need one more field 🙏
>
> **Bea**: also — Payments asked me for something similar last month for their refunds
> flow. did that ever get built?
>
> **Chiara**: one more thing. last week the courier app was offline for about an hour and
> then every event it had queued up arrived at once. the daily report showed a huge spike
> at 14:00 that never actually happened. that can't happen on the live one.
>
> **Tomek**: and what even counts as a late delivery? >45 min from `order_placed`? from
> `courier_assigned`? I don't think anyone ever agreed on this.

That's everything we have. It is not a complete set of requirements, and that's on
purpose.

---

# What we're giving you

A `docker compose up` that gets you:

- **RabbitMQ**, with a producer emitting a continuous stream of realistic order events:
  `order_placed`, `courier_assigned`, `order_picked_up`, `order_delivered`,
  `order_cancelled`. It behaves like a real queue — at-least-once delivery, events that
  arrive out of order, a periodic burst of events that were buffered while a courier app
  was offline, the occasional malformed payload, and some orders that never reach a
  terminal state.
- **Postgres**, pre-seeded with the slow-moving dimensions: `restaurants` (zone, cuisine,
  tier, commission), `couriers` (vehicle type, city, status) and `zones` (city, name).
- A README with connection details, the event schema and the table definitions.

**Getting the setup:** either clone the repo

<https://github.com/DocPlanner/data-engineer-case-study-app>

or download exactly the same thing as a zip

<https://drive.google.com/file/d/1A9ncgT02YzxgZVljTfDLxsoYN1HcfqAj/view>

Then run `docker compose up` inside the `harness` folder. Everything you need to connect
is in `HARNESS.md`, and both RabbitMQ and Postgres come with a browser UI so you can
inspect the queue and the tables without installing a client. If anything doesn't work,
tell us — that's our bug, not yours.

One thing up front: RabbitMQ and Postgres are in the harness because they start in one
command, not because they are the right answer. Treat them as a stand-in, not as a
proposed architecture.

---

# What we'd like back

## 1. A design document

This is the main thing we'll read. Short is good; bullet points beat essays. Please
cover:

**a. What you're actually solving.** The requirements you derived from the inputs above,
the gaps you found, and who you'd have asked what. Where you had to assume something to
keep moving, say so — we want to clearly see your assumptions and reasoning.

**b. Architecture.** How events get from the queue to something a person can query:
ingestion, storage, transformation, serving. A diagram is very welcome. Tell us what
you'd store, in what format, and why.

Don't let the harness constrain you. Postgres is in it because it starts in one command —
we want to know whether you'd reach for a warehouse, a lakehouse, object storage with a
query engine on top, something streaming-native, or something else entirely, and what that
choice buys and costs you.

And one decision we'd especially like you to take a position on: what do you actually
land? A raw event table, one row per event, pivoted at query time? An order-level fact
table, one row per order, updated as later events arrive? Both, as separate layers?
Something else? Each option makes something else hard — an order-level row has to be
mutated every time a late event turns up, while an event-level table pushes that cost onto
everyone who queries it. Tell us which you'd choose, and why.

**c. The self-service interface.** This is the part we care most about. Tomek has a data
background and writes good SQL, but is not a data engineer and is not going to write a
consumer. How does Tomek declare *"I want these events, these fields, this
transformation, this table"* and get it, without you?

> One way to do this is a declarative config that an analyst commits to a repo. If you
> think a different interface is better — a UI, SQL-only, dbt models, something we
> haven't thought of — make the case for it.

And the other half of that: when Tomek opens a PR with a broken SQL transform or a
malformed config, what catches it before it reaches production? Walk us through it.
Where would an AI reviewer genuinely help here, and where would you not trust one?

**d. How this becomes a service rather than a project.** Payments will ask next week,
and Search the week after. What does onboarding team #2 through #10 look like? What do
you own, what do they own, and what stops the whole thing collapsing into ten bespoke
pipelines with your name on them?

**e. Correctness, and how you'd prove it.** Duplicates, out-of-order events, the
courier-app outage that flushed an hour of events at once and created a fake spike,
orders that never complete, replays and backfills. What guarantees do you offer your
users, and what do you explicitly *not* offer?

Then the harder half: how would you convince Chiara — and yourself — that the number on
her dashboard is actually right? What would you reconcile against, and what check would
fail loudly if it stopped being right?

**f. Scale and cost.** Today Nomly does a couple of hundred orders a minute. What breaks first
at 100x? Be concrete about storage format, query engine, and what it costs.

**g. Serving.** How do Bea and Chiara actually see the number? Note that Bea asked for a
Google Sheet — tell us what you did with that request and why.

Then, at the end: what you'd do next, and what you deliberately cut.

## 2. One thin working slice

We'd like to see a small piece of it actually run. **One** event type, end to end:

- through the interface you designed in (c) — so a config file, or whatever you chose
- consumed from the RabbitMQ we gave you
- written **idempotently** into the harness Postgres — whatever you'd use for real, use
  what's in front of you here: run it twice, get the same table
- plus the query you would hand Chiara, run against your own table. Tell us what it
  returned and over what window — and, more importantly, why you believe the number.

**Please don't build the whole platform.** One event type is the entire build. Everything
else lives in the design document.

The harness source is in the repo and you're welcome to read it — it will show you how
the events are generated. It will not tell you whether your pipeline counted them
correctly, and that is the part we're reading.

## 3. A short README

How to run it, and what would break first if we ran it unattended for a week.

---

# Nice to haves

Only if you have time and want to — genuinely optional:

- **Metric definition:** pick a definition of "late delivery" and say who should own it
- **Observability:** what you'd monitor, and what would page someone at 9am
- **Schema evolution:** Dispatch adds a field to `courier_assigned` next sprint. What happens?
- **Access control:** `courier_id` is in the event stream and Tomek wants to slice by courier
- **Tests:** a couple that would catch a real regression in your transformation logic

---

# Run

Please provide clear instructions on how to run your slice, and a note on the failures
you'd expect in practice and what someone on call should do about them.

---

# What happens next

A 60-minute design review with two engineers from the team. We'll dig into your
trade-offs, push on the parts you flagged as uncertain, and ask what you'd change now
that you've slept on it. Come ready to disagree with us — the JD says "strong opinions,
loosely held" and we meant it.

# Questions?

Anything at all, just ask:

- [Giuseppe Russo](mailto:giuseppe.russo@docplanner.com)

Good luck — we're looking forward to reading it 🚀
