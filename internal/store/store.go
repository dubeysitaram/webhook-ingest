// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// EventExists reports whether an event with this ID has already been stored.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// InsertEvent stores the raw delivery.
func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	return err
}

// UpsertCall creates or refreshes the call record for this event.
func (s *Store) UpsertCall(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE, updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

// IncrementAccountStats folds one completed call into the durable aggregate.
func (s *Store) IncrementAccountStats(ctx context.Context, accountID string, durationSec int) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID, durationSec)
	return err
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}

// IngestResult reports what one delivery actually changed.
type IngestResult struct {
	// Duplicate is true when this event_id was already stored, in which case
	// nothing else was written and both deltas are zero.
	Duplicate bool
	// CallDelta is 1 when this event created a call that did not exist
	// before, and 0 when it revised a call already counted.
	CallDelta int64
	// DurationDelta is the change this event made to the account's total
	// duration: the full duration for a new call, or the difference from the
	// previous value when a known call is revised.
	DurationDelta int64
}

// IngestEvent stores one delivery and folds it into the aggregates, all
// inside a single transaction.
//
// Idempotency is enforced by the database rather than by the application.
// The insert into events relies on the unique index on event_id added in
// migration 002: a redelivery hits ON CONFLICT DO NOTHING, affects no rows,
// and returns Duplicate without touching calls or account_stats. Two
// concurrent deliveries of one event cannot both win, because the second
// blocks on the unique index until the first commits and then sees the
// conflict. That is the guarantee the previous EventExists-then-insert
// sequence could not make, since both requests could pass the check before
// either wrote.
//
// Running the three writes in one transaction also removes a second failure
// mode. Previously they were three independent statements, so a failure after
// the event row was committed left the aggregates un-updated, and the
// provider's retry was then dismissed as a duplicate -- the count stayed
// wrong permanently. Now either all three land or none do, and a retry of a
// failed delivery finds no event row and is processed properly.
//
// The aggregate counts calls, not events. account_stats.call_count is
// maintained so that it equals the number of rows in calls for the account,
// and total_duration_sec the sum of their durations.
func (s *Store) IngestEvent(ctx context.Context, e Event) (IngestResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestResult{}, err
	}
	// Rollback is a no-op once the transaction has committed.
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	if err != nil {
		return IngestResult{}, err
	}
	if tag.RowsAffected() == 0 {
		// Already stored by an earlier or concurrent delivery. Commit the
		// empty transaction so the connection returns to the pool cleanly.
		if err := tx.Commit(ctx); err != nil {
			return IngestResult{}, err
		}
		return IngestResult{Duplicate: true}, nil
	}

	// Take the call row's lock before reading its duration, so a concurrent
	// event for the same call cannot compute its delta from the same stale
	// value. A call that does not exist yet returns no rows.
	var prevDuration int
	hadCall := true
	err = tx.QueryRow(ctx,
		`SELECT duration_sec FROM calls WHERE call_id = $1 FOR UPDATE`,
		e.CallID).Scan(&prevDuration)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		hadCall = false
	case err != nil:
		return IngestResult{}, err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL); err != nil {
		return IngestResult{}, err
	}

	res := IngestResult{}
	if hadCall {
		res.DurationDelta = int64(e.DurationSec - prevDuration)
	} else {
		res.CallDelta = 1
		res.DurationDelta = int64(e.DurationSec)
	}

	if res.CallDelta != 0 || res.DurationDelta != 0 {
		if _, err := tx.Exec(ctx,
			`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (account_id) DO UPDATE SET
			     call_count         = account_stats.call_count + EXCLUDED.call_count,
			     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
			e.AccountID, res.CallDelta, res.DurationDelta); err != nil {
			return IngestResult{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return IngestResult{}, err
	}
	return res, nil
}
