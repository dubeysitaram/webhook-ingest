package store_test

import (
	"context"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// recordingState reads back the two recording columns for a call.
func recordingState(t *testing.T, s *store.Store, callID string) (url string, processed bool) {
	t.Helper()
	if err := s.Pool().QueryRow(context.Background(),
		`SELECT recording_url, recording_processed FROM calls WHERE call_id = $1`,
		callID).Scan(&url, &processed); err != nil {
		t.Fatalf("read call %s: %v", callID, err)
	}
	return url, processed
}

// TestRevisingEventKeepsTheRecordingURL covers a correction that carries no
// recording.
//
// More than one event can describe a call: a later delivery can correct a
// duration or a status. The upsert applied EXCLUDED.recording_url
// unconditionally, so a correction that omitted the recording overwrote a
// good URL with the empty string -- while recording_processed stayed true.
// The row then claimed a recording had been processed for a call with no
// recording to point at, and the URL was gone with no way to recover it.
func TestRevisingEventKeepsTheRecordingURL(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	const url = "https://recordings.example.com/original.wav"
	first := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 100,
		RecordingURL: url, Payload: []byte(`{}`),
	}
	if _, err := s.IngestEvent(ctx, first); err != nil {
		t.Fatalf("first IngestEvent: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	// A correction to the duration only. The provider did not repeat the
	// recording URL, which does not mean the recording ceased to exist.
	second := first
	second.EventID = eventID + "_corrected"
	second.DurationSec = 143
	second.RecordingURL = ""
	if _, err := s.IngestEvent(ctx, second); err != nil {
		t.Fatalf("second IngestEvent: %v", err)
	}

	gotURL, processed := recordingState(t, s, callID)
	if gotURL != url {
		t.Errorf("recording_url is %q, want %q: a correction that carried no "+
			"recording erased the one already stored", gotURL, url)
	}
	if !processed {
		t.Errorf("recording_processed is false, want true: the recording was " +
			"already processed and nothing about it changed")
	}
}

// TestChangedRecordingURLIsReprocessed covers the opposite case: the
// recording itself is replaced.
//
// recording_processed described the old URL. Leaving it true marks a
// recording nobody has fetched as done, and the new audio is never processed.
func TestChangedRecordingURLIsReprocessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	first := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 100,
		RecordingURL: "https://recordings.example.com/original.wav",
		Payload:      []byte(`{}`),
	}
	if _, err := s.IngestEvent(ctx, first); err != nil {
		t.Fatalf("first IngestEvent: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	const replaced = "https://recordings.example.com/replaced.wav"
	second := first
	second.EventID = eventID + "_replaced"
	second.RecordingURL = replaced
	if _, err := s.IngestEvent(ctx, second); err != nil {
		t.Fatalf("second IngestEvent: %v", err)
	}

	gotURL, processed := recordingState(t, s, callID)
	if gotURL != replaced {
		t.Errorf("recording_url is %q, want %q", gotURL, replaced)
	}
	if processed {
		t.Errorf("recording_processed is true, but the recording was replaced: " +
			"the flag still describes the URL that is no longer stored")
	}
}
