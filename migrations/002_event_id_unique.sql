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
