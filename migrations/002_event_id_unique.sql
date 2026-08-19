-- Enforce at-most-once storage of a webhook delivery.
--
-- Migration 001 created a plain index on events.event_id. An index makes
-- lookups fast; it does not forbid duplicate values. Ingestion relied on a
-- read ("does this event_id exist?") followed by a write, and concurrent
-- redeliveries could both pass the read before either wrote. With no
-- constraint underneath, both inserts succeeded.
--
-- The unique index makes the database the arbiter: the second concurrent
-- insert of an event_id now loses, whatever the application does.

-- Collapse any duplicates an earlier deployment already stored, keeping the
-- earliest row per event_id. Migrations run on an empty volume in the normal
-- case, so this is usually a no-op; it exists so the constraint can also be
-- applied to a database that has been running with the defect.
DELETE FROM events a
      USING events b
      WHERE a.event_id = b.event_id
        AND a.id > b.id;

DROP INDEX IF EXISTS idx_events_event_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_unique
    ON events (event_id);

-- Rebuild the aggregates from the calls they describe.
--
-- Collapsing the duplicate event rows above does not undo what they already
-- did to account_stats. A database that has been running with the defect has
-- a call_count inflated by every duplicate delivery and by every additional
-- event describing a call already counted -- which is the drift operations
-- reported. Dropping the duplicates leaves those numbers exactly as wrong as
-- they were.
--
-- calls is the authority: one row per call, holding that call's latest
-- duration. Recomputing from it restores the invariant the application now
-- maintains incrementally, that for every account call_count equals the
-- number of rows in calls and total_duration_sec the sum of their durations.
--
-- Accounts that have rows in account_stats but none in calls cannot be
-- corrected from calls, so they are zeroed rather than left inflated.
UPDATE account_stats SET call_count = 0, total_duration_sec = 0;

INSERT INTO account_stats (account_id, call_count, total_duration_sec)
SELECT account_id, count(*), coalesce(sum(duration_sec), 0)
  FROM calls
 GROUP BY account_id
ON CONFLICT (account_id) DO UPDATE SET
    call_count         = EXCLUDED.call_count,
    total_duration_sec = EXCLUDED.total_duration_sec;
