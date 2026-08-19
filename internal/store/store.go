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
//
// Deprecated: superseded by IngestEvent. Answering this question before
// writing is what made ingestion racy: two concurrent deliveries of one event
// both saw "no", and both went on to insert and count it. The answer is stale
// the moment it is returned, so it cannot be the basis of a dedup decision.
// Kept for API compatibility and used by tests; do not build on it.
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
//
// Deprecated: superseded by IngestEvent. This writes the event on its own,
// outside any transaction, so a failure in the aggregate updates that should
// accompany it leaves the event stored and the totals un-updated -- and the
// provider's retry is then dismissed as a duplicate, making the loss
// permanent. Kept for API compatibility; do not build on it.
func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	return err
}

// UpsertCall creates or refreshes the call record for this event.
//
// Deprecated: superseded by IngestEvent, which performs this upsert inside
// the same transaction as the event insert and the aggregate update, and
// serialises concurrent events for one call. Used alone it takes no advisory
// lock, so callers cannot compute a correct aggregate delta around it.
// Kept for API compatibility; do not build on it.
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
//
// Deprecated: superseded by IngestEvent. This adds one per *event*, but
// several events can describe one call -- a later delivery correcting a
// duration or a status -- so the count drifts above the real number of calls.
// That is the defect the ops report described, and calling this reintroduces
// it. IngestEvent applies a delta instead: a new call adds one, a revision
// adds none. Kept for API compatibility; do not build on it.
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

	// Serialise every transaction that touches this call_id.
	//
	// The FOR UPDATE below can only lock a row that already exists. When two
	// different events for the same *new* call arrive together, both read "no
	// such call", both conclude they created it, and both add one to
	// call_count -- leaving one row in calls and a count of two. A row lock
	// cannot close that window because there is no row yet to lock.
	//
	// An advisory lock is keyed on a number rather than a row, so it works
	// before the row exists. It is released automatically when the
	// transaction ends. hashtext can collide, which would serialise two
	// unrelated calls unnecessarily; that costs a little throughput and
	// changes no result.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtext($1))`, e.CallID); err != nil {
		return IngestResult{}, err
	}

	// Now read the existing duration, if any, to work out the delta.
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

	// recording_url is kept when the incoming event does not carry one. An
	// event that omits the recording is not asserting that the recording is
	// gone -- a correction to a duration or a status simply has nothing to say
	// about it -- and overwriting the stored URL with the empty string loses
	// it for good while recording_processed still claims it was handled.
	//
	// recording_processed is cleared only when the URL actually changes to a
	// different one, because the flag describes the recording that was
	// processed. Leaving it set would mark audio nobody has fetched as done.
	if _, err := tx.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, NULLIF($5, ''), now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = COALESCE(EXCLUDED.recording_url, calls.recording_url),
		     recording_processed = CASE
		         WHEN EXCLUDED.recording_url IS NOT NULL
		          AND EXCLUDED.recording_url IS DISTINCT FROM calls.recording_url
		         THEN FALSE
		         ELSE calls.recording_processed
		     END,
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

// AllAccountStats reads every account's durable totals, for warming the
// in-memory cache at startup.
func (s *Store) AllAccountStats(ctx context.Context) (map[string]Stats, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT account_id, call_count, total_duration_sec FROM account_stats`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]Stats)
	for rows.Next() {
		var accountID string
		var st Stats
		if err := rows.Scan(&accountID, &st.CallCount, &st.TotalDurationSec); err != nil {
			return nil, err
		}
		out[accountID] = st
	}
	return out, rows.Err()
}
