# webhook-ingest

A Go service that receives call-completion webhooks from a telephony provider,
stores them, and maintains per-account call statistics.

This repository is a submission for the Convin backend take-home. The service
shipped with a set of runtime defects that the test suite did not catch; they
are diagnosed and fixed here. The reasoning behind each change is in
[SOLUTION.md](SOLUTION.md).

## What was fixed

| Defect | Fix |
|---|---|
| Concurrent redeliveries were both stored and both counted | Unique index on `events.event_id`, and one transaction per delivery using `ON CONFLICT DO NOTHING` |
| Counts drifted above the real number of calls | The aggregate now counts calls, not events; a revising event applies only a duration delta |
| Recordings were never marked processed, silently | Background work detached from the request context via `context.WithoutCancel`, with its own timeout, and failures are logged |
| In-flight work was lost on every deploy | Background work is tracked in a `WaitGroup` and drained after `srv.Shutdown` |
| Stats read as zero after a restart | The in-memory cache is warmed from `account_stats` before the server accepts traffic |
| The stats cache lost increments and could crash the process | Every mutation now holds the cache's mutex |

## Running it

```bash
docker compose up -d --build   # Postgres, Redis, and the service
curl localhost:8080/healthz    # -> ok
go test ./...                  # the test suite
```

`make reset` tears everything down, wipes the volumes, and starts fresh.
Migrations are plain SQL in `migrations/`, applied by Postgres on the first
start of an empty volume — **run `make reset` after pulling, so that
`002_event_id_unique.sql` is applied.**

Tests run against the Postgres started by Compose, so bring the stack up first.
They clean up after themselves and are safe to run repeatedly.

### Running the tests

```bash
go test ./...          # full suite
go test ./... -race    # recommended: several tests target concurrency defects
```

The concurrency tests warm the HTTP and pgx connection pools before their
burst. Without that, connections are established lazily one at a time, which
staggers the requests enough that they stop overlapping — and the test then
passes for the wrong reason.

### Already running Postgres or Redis locally?

The published host ports are overridable. Copy `.env.example` to `.env` and
change `APP_PORT`, `POSTGRES_PORT`, `REDIS_PORT`.

Note that `.env` is read by Docker Compose, not by Go. To point `go test ./...`
at the same database, export the connection strings in your shell as well:

```bash
export DATABASE_URL="postgres://webhook:webhook@localhost:5433/webhook?sslmode=disable"
export REDIS_ADDR="localhost:6379"
```

## The API

**`POST /webhooks/calls`**

```json
{
  "event_id":      "evt_01H8XK2M9P",
  "call_id":       "call_9f2ab31c",
  "account_id":    "acc_123",
  "status":        "completed",
  "duration_sec":  143,
  "recording_url": "https://recordings.example.com/9f2ab31c.wav",
  "occurred_at":   "2026-08-13T09:12:00Z"
}
```

`status` is one of `completed`, `failed`, `no_answer`.

Delivery is idempotent on `event_id`. A redelivery is acknowledged with `200`
and changes nothing, including when several copies arrive simultaneously.

**`GET /accounts/{account_id}/stats`** — returns the in-memory aggregate. The
durable copy of the same numbers lives in the `account_stats` table, and the
cache is warmed from it at startup.

**`GET /healthz`**

## Layout

```
cmd/server/           entrypoint and wiring
internal/config/      environment configuration
internal/store/       Postgres repository
internal/stats/       in-memory per-account totals
internal/ingest/      webhook ingestion and processing
internal/httpapi/     routes and handlers
internal/redisclient/ Redis connection
internal/testutil/    shared test setup
migrations/           schema
```

## Notes on the approach

The public API of every package is unchanged: existing exported functions keep
their signatures and behaviour, and new functionality was added alongside them
rather than replacing them. The entrypoint stays at `./cmd/server` and the
Dockerfile's `BUILD_FLAGS` argument is untouched.
