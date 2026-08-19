package ingest_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestAggregateCountsCallsNotEvents pins down what call_count means.
//
// UpsertCall has always been an upsert on call_id, so more than one event can
// legitimately describe the same call: a later delivery can correct a
// duration or a status. The aggregate, however, was incremented once per
// event. Two events about one call therefore left one row in calls and a
// call_count of two -- counts drifting above the real number of calls.
//
// The invariant this test protects is that for any account,
// account_stats.call_count equals the number of rows in calls, and
// total_duration_sec equals the sum of their durations. A revising event
// contributes no new call and only the difference in duration.
func TestAggregateCountsCallsNotEvents(t *testing.T) {
	svc, st := newService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	first := ingest.Event{
		EventID:     eventID,
		CallID:      callID,
		AccountID:   accountID,
		Status:      "completed",
		DurationSec: 100,
		OccurredAt:  time.Now().UTC(),
	}
	if err := svc.Ingest(ctx, first); err != nil {
		t.Fatalf("first Ingest: %v", err)
	}

	// A second, genuinely different event describing the same call, with a
	// corrected duration.
	second := first
	second.EventID = eventID + "_corrected"
	second.DurationSec = 143
	if err := svc.Ingest(ctx, second); err != nil {
		t.Fatalf("second Ingest: %v", err)
	}

	var calls int
	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM calls WHERE account_id = $1`, accountID).
		Scan(&calls); err != nil {
		t.Fatalf("count calls: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls holds %d rows, want 1", calls)
	}

	durable, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if durable.CallCount != int64(calls) {
		t.Errorf("call_count is %d but calls holds %d rows: the aggregate is "+
			"counting events rather than calls", durable.CallCount, calls)
	}
	if durable.TotalDurationSec != 143 {
		t.Errorf("total_duration_sec is %d, want 143 (the corrected duration, "+
			"not 100+143)", durable.TotalDurationSec)
	}

	// The in-memory aggregate must agree with the durable one.
	cached := svc.Stats(accountID)
	if cached.CallCount != durable.CallCount ||
		cached.TotalDurationSec != durable.TotalDurationSec {
		t.Errorf("cache reports %+v, durable reports %+v: the two have drifted",
			cached, durable)
	}
}

// TestRedeliveryDoesNotChangeAggregates is the plain idempotency case at the
// service level: the exact same event twice must leave the totals alone.
func TestRedeliveryDoesNotChangeAggregates(t *testing.T) {
	svc, st := newService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	evt := ingest.Event{
		EventID:     eventID,
		CallID:      callID,
		AccountID:   accountID,
		Status:      "completed",
		DurationSec: 143,
		OccurredAt:  time.Now().UTC(),
	}

	for i := 0; i < 3; i++ {
		if err := svc.Ingest(ctx, evt); err != nil {
			t.Fatalf("Ingest %d: %v", i, err)
		}
	}

	durable, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if durable.CallCount != 1 || durable.TotalDurationSec != 143 {
		t.Errorf("after 3 deliveries of one event the aggregate is %+v, "+
			"want CallCount=1 TotalDurationSec=143", durable)
	}

	cached := svc.Stats(accountID)
	if cached.CallCount != 1 || cached.TotalDurationSec != 143 {
		t.Errorf("in-memory aggregate is %+v, want CallCount=1 "+
			"TotalDurationSec=143", cached)
	}
}

// TestTwoEventsForOneNewCallArrivingTogether is the concurrent version of
// TestAggregateCountsCallsNotEvents.
//
// Two different events describing the same call is the case the sequential
// test covers. When both arrive at the same instant and the call does not yet
// exist, the row lock taken while computing the delta cannot help: SELECT ...
// FOR UPDATE only locks rows that already exist, so both transactions read
// "no such call", both conclude they created it, and both add one to
// call_count. The result is one row in calls and a count of two -- the same
// drift the sequential fix was meant to remove, reappearing under
// concurrency. IngestEvent takes an advisory lock on the call_id to close it.
func TestTwoEventsForOneNewCallArrivingTogether(t *testing.T) {
	svc, st := newService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Open pool connections first, so the two deliveries genuinely overlap
	// instead of being staggered by lazy connection setup.
	var warm sync.WaitGroup
	for i := 0; i < 4; i++ {
		warm.Add(1)
		go func() {
			defer warm.Done()
			var one int
			_ = st.Pool().QueryRow(ctx, `SELECT 1`).Scan(&one)
		}()
	}
	warm.Wait()

	event := func(suffix string, duration int) ingest.Event {
		return ingest.Event{
			EventID:     eventID + suffix,
			CallID:      callID,
			AccountID:   accountID,
			Status:      "completed",
			DurationSec: duration,
			OccurredAt:  time.Now().UTC(),
		}
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, evt := range []ingest.Event{event("_a", 100), event("_b", 143)} {
		wg.Add(1)
		go func(evt ingest.Event) {
			defer wg.Done()
			<-start
			if err := svc.Ingest(ctx, evt); err != nil {
				t.Errorf("Ingest %s: %v", evt.EventID, err)
			}
		}(evt)
	}
	close(start)
	wg.Wait()

	var calls int
	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM calls WHERE account_id = $1`, accountID).
		Scan(&calls); err != nil {
		t.Fatalf("count calls: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls holds %d rows, want 1", calls)
	}

	durable, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if durable.CallCount != int64(calls) {
		t.Errorf("call_count is %d but calls holds %d rows: both deliveries "+
			"counted the same new call", durable.CallCount, calls)
	}

	// Whichever delivery lands last sets the duration, so either value is
	// correct -- but the total must equal that call's duration, never the sum.
	if durable.TotalDurationSec != 100 && durable.TotalDurationSec != 143 {
		t.Errorf("total_duration_sec is %d, want 100 or 143 (never 243)",
			durable.TotalDurationSec)
	}

	cached := svc.Stats(accountID)
	if cached.CallCount != durable.CallCount ||
		cached.TotalDurationSec != durable.TotalDurationSec {
		t.Errorf("cache reports %+v, durable reports %+v: the two have drifted",
			cached, durable)
	}
}
