package ingest_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// waitForRecordingProcessed polls the calls row until recording_processed is
// true, or gives up. The work is asynchronous by design, so polling is the
// honest way to observe it; the stand-in work takes 50ms.
func waitForRecordingProcessed(t *testing.T, st *store.Store, callID string, within time.Duration) bool {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(within)

	for time.Now().Before(deadline) {
		var processed bool
		err := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`,
			callID).Scan(&processed)
		if err == nil && processed {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

// TestRecordingIsMarkedProcessed is the regression test for the recording
// work that silently never completed.
//
// Ingest handed the request's context to a goroutine that outlives the
// request. net/http cancels a request context as soon as its handler returns,
// and this handler returns immediately so the provider gets a fast
// acknowledgement. By the time the goroutine woke up, its context had been
// cancelled, so pgx refused to run the UPDATE and returned "context
// canceled". The error was assigned in a branch whose body was an empty
// "TODO: handle", so nothing was logged either -- which is exactly what
// operations reported: recordings never marked processed, and nothing in the
// logs about it.
//
// No existing test covered the recording path end to end. The store-level
// test calls MarkRecordingProcessed directly, bypassing the goroutine, so it
// passes whether or not ingestion ever reaches that code.
func TestRecordingIsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := eventJSON(eventID, callID, accountID) // includes a recording_url
	resp, err := http.Post(srv.URL+"/webhooks/calls",
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	if !waitForRecordingProcessed(t, st, callID, 5*time.Second) {
		t.Fatal("recording_processed is still false: the background work " +
			"never completed, and no error was surfaced")
	}
}

// TestRecordingIsSkippedWhenThereIsNoURL guards the other branch: an event
// with no recording must not be marked processed.
func TestRecordingIsSkippedWhenThereIsNoURL(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := `{"event_id":"` + eventID + `","call_id":"` + callID +
		`","account_id":"` + accountID + `","status":"no_answer",` +
		`"duration_sec":0,"occurred_at":"2026-08-13T09:12:00Z"}`
	resp, err := http.Post(srv.URL+"/webhooks/calls",
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if waitForRecordingProcessed(t, st, callID, 500*time.Millisecond) {
		t.Fatal("a call with no recording_url was marked processed")
	}
}
