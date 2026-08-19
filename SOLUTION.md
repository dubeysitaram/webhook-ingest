# SOLUTION

## What was broken, and why

| # | Defect | Symptom it caused |
|---|---|---|
| 1 | `Ingest` ran `EventExists` and then inserted. Two concurrent redeliveries both passed the check before either wrote, so both inserted and both counted. Migration 001 backed `event_id` with a plain index, which speeds up lookups but permits duplicate values, so nothing beneath the application stopped it. | Duplicate call records; counts drifting high |
| 2 | The aggregate was incremented once per **event**, but `UpsertCall` is an upsert on `call_id`, so several events can legitimately describe one call. Two events about one call left one row in `calls` and a `call_count` of two. | Counts higher than the real number of calls |
| 3 | The recording goroutine was handed the **request's** context. `net/http` cancels that the instant the handler returns, and the handler returns immediately by design, so every `MarkRecordingProcessed` failed with `context canceled`. The error was assigned in a branch whose body was `// TODO: handle`. | Recordings never processed, and silence in the logs |
| 4 | That goroutine was untracked. `srv.Shutdown` drains in-flight HTTP handlers, but this work deliberately outlives its handler, so the process exited while it was still running. | In-flight work lost on every deploy |
| 5 | The stats cache was created empty at boot and nothing loaded `account_stats` into it, while `GET /accounts/{id}/stats` serves that cache. | Established accounts reported zero after a deploy |
| 6 | `Cache.Record` mutated the map holding no lock, though `Cache` carries a mutex and `Get` takes it. Increments were lost, and concurrent Go map writes abort the whole process. | Undercounting, and a latent crash under load |

The three durable writes were also independent statements. A failure after the
event row committed left the aggregates un-updated, and the provider's retry
was then dismissed as a duplicate — so the count stayed wrong permanently.

Defects 1, 3, 5 and 6 each have a test that fails before the fix and passes
after. Defect 4's test is committed with its fix, because the assertion needs
the `Wait` method that is itself the fix.

## Why this deduplication strategy

**Chosen: a unique index on `events.event_id`, with the insert and both
aggregate updates in one transaction.** The insert uses `ON CONFLICT
(event_id) DO NOTHING`; zero rows affected means redelivery, and the
transaction returns without touching `calls` or `account_stats`.

The point is that correctness does not depend on application timing. Two
concurrent deliveries cannot both win because the second **blocks on the
unique index** until the first commits and then sees the conflict. The same
transaction that decides "this event is new" performs the counting, so the
decision and its consequences cannot be separated by a crash.

*Redis `SETNX` with a TTL* was the alternative. It is faster and takes load off
Postgres, but Redis is a cache: keys are evictable under memory pressure and a
restart without persistence loses them. Either failure silently permits a
double-count, with nothing to detect it afterwards. It is also a second system
that can disagree with the ledger, and it cannot make the dedup decision atomic
with the write it guards — the process can die between `SETNX` and the commit,
and the event is then permanently lost instead of merely retried.

I would still put Redis in front at high volume, but strictly as an
optimisation absorbing obvious repeats, never as the authority. That is the
change in constraints, not the design: Postgres stays the arbiter.

*A `UNIQUE` constraint alone without the transaction* would stop duplicate
rows but not fix defect 1's counting, since the increment was a separate
statement.

## At 10,000 webhooks/second

The current shape — synchronous transaction per delivery, one goroutine per
recording — would not hold. What I would change, in order:

1. **Make the handler a durable enqueue.** Write the raw delivery to an append-only
   log (Kafka/Redis Streams) keyed by `event_id`, acknowledge, and move counting
   into consumers. The provider needs a fast 2xx, not a completed write.
2. **Batch the aggregate updates.** 10k individual `UPDATE account_stats` per second
   will serialise on per-account row locks. Consumers should fold events over a
   short window and apply one update per account per flush.
3. **Redis as the dedup fast path**, as above, with the unique index still underneath.
4. **Partition `events` by time** and keep the dedup window bounded; a unique index
   over an unbounded table becomes the write bottleneck.
5. **A worker pool with bounded concurrency** for recordings instead of an unbounded
   goroutine per event, plus a reconciler (below) for durability.

## Out of scope, deliberately

Per the brief, not built: authentication, webhook signature verification, rate
limiting, dashboards. Of these, **signature verification** is the one I would
raise first — the endpoint currently trusts any caller who can reach it, and
`event_id` is attacker-controlled, so a forged event can poison the dedup key
and suppress a real delivery. Also unhandled: no request body size limit, and
no `ReadHeaderTimeout` on the server.

## What I would do next

- **A reconciler for recordings.** `Wait` bounds the loss on a graceful shutdown but
  does not eliminate it: work past the deadline is still lost, and a hard kill loses
  it regardless. `calls WHERE recording_processed = false` is already a durable work
  queue; a sweep at startup and on a ticker would make the work crash-safe. A
  redelivery currently does not retry a failed recording, since it is correctly
  treated as a duplicate — the reconciler is what closes that gap.
- **Out-of-order deliveries.** `UpsertCall` applies whatever arrives last. A delayed
  older event can overwrite a newer status. `occurred_at` is on the payload but not
  in `calls`; storing it and refusing to apply an older event would fix it.
- **Cache and database drift.** The two are updated in the same call but not
  atomically — a crash between commit and `cache.Apply` leaves the cache stale until
  the next restart warms it. Serving stats from Postgres with a short TTL, or
  periodically re-warming, would remove the divergence.
