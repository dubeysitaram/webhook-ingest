package ingest_test

import (
	"context"
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
