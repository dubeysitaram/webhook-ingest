package ingest_test

import (
	"context"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// bootService builds a Service the way cmd/server does at startup, so a test
// can reproduce what the stats endpoint reports on a freshly deployed
// process.
func bootService(t *testing.T) (*ingest.Service, *store.Store) {
	t.Helper()
	svc, st := newService(t)
	return svc, st
}

// TestStatsSurviveARestart covers the totals that read as zero after every
// deploy.
//
// The durable aggregate lives in account_stats and is intact across a
// restart, but the in-memory cache is created empty by cmd/server and nothing
// loads the durable numbers into it. GET /accounts/{id}/stats serves that
// cache, so every deploy makes established accounts report zero calls until
// enough new webhooks arrive to rebuild the numbers from scratch. To
// operations this looked like data disappearing on deploy.
//
// The test ingests through one service, then boots a second one over the same
// database -- which is what a redeploy is -- and asks the new process for the
// totals.
func TestStatsSurviveARestart(t *testing.T) {
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
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Sanity check: the durable copy is correct before the restart.
	durable, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if durable.CallCount != 1 || durable.TotalDurationSec != 143 {
		t.Fatalf("durable stats are %+v, want CallCount=1 TotalDurationSec=143", durable)
	}

	// Restart: a new process over the same database.
	restarted, _ := bootService(t)

	got := restarted.Stats(accountID)
	if got.CallCount != durable.CallCount || got.TotalDurationSec != durable.TotalDurationSec {
		t.Errorf("after restart the service reports %+v, want %+v -- "+
			"the in-memory cache was not populated from account_stats",
			got, durable)
	}
}
