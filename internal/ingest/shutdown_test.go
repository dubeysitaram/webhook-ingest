package ingest_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// newService builds a Service directly, without the HTTP layer, so a test can
// reach methods the router does not expose.
func newService(t *testing.T) (*ingest.Service, *store.Store) {
	t.Helper()
	cfg := config.Load()
	st := testutil.NewStore(t)

	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis (is `docker compose up` running?): %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return ingest.New(st, stats.NewCache(), rdb, log), st
}

// TestWaitDrainsInFlightRecordingWork covers the work lost on every deploy.
//
// srv.Shutdown drains in-flight HTTP handlers, but the recording goroutine
// deliberately outlives its handler and was untracked, so nothing knew it
// existed. Shutdown returned, main returned, and the process exited while the
// work was still running.
//
// The assertion is that immediately after Wait returns -- with no polling at
// all -- the work is already complete.
func TestWaitDrainsInFlightRecordingWork(t *testing.T) {
	svc, st := newService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  143,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now().UTC(),
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := svc.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	var processed bool
	if err := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`,
		callID).Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("Wait returned before the recording work had finished")
	}
}

// TestWaitReturnsWhenThereIsNothingInFlight keeps shutdown from blocking on
// an idle service.
func TestWaitReturnsWhenThereIsNothingInFlight(t *testing.T) {
	svc, _ := newService(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := svc.Wait(ctx); err != nil {
		t.Fatalf("Wait on an idle service: %v", err)
	}
}
