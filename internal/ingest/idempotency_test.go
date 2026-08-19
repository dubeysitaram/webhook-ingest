package ingest_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// deliveries is the number of copies of one event we deliver at once.
const deliveries = 32

// statsResponse mirrors the JSON shape of GET /accounts/{id}/stats.
type statsResponse struct {
	CallCount        int64 `json:"call_count"`
	TotalDurationSec int64 `json:"total_duration_sec"`
}

// getStats reads the in-memory aggregate over HTTP.
func getStats(t *testing.T, srvURL, accountID string) statsResponse {
	t.Helper()
	resp, err := http.Get(srvURL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var got statsResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	return got
}

// warmUp opens n HTTP connections and n pooled Postgres connections up front.
//
// Without this the test proves nothing. On a cold process Go establishes TCP
// connections and pgx opens pool connections lazily, one at a time, which
// staggers the "concurrent" requests enough that they no longer overlap. With
// the connections already warm the burst below is genuinely simultaneous.
func warmUp(t *testing.T, srvURL string, st *store.Store, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if resp, err := http.Get(srvURL + "/healthz"); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body) // drain so the conn is reused
				_ = resp.Body.Close()
			}
			var one int
			_ = st.Pool().QueryRow(context.Background(), `SELECT 1`).Scan(&one)
		}()
	}
	wg.Wait()
}

// TestConcurrentDuplicateDeliveryCountsOnce is the regression test for the
// ingestion race.
//
// The provider delivers at least once and does not serialise its retries, so
// several copies of one event can be in flight at the same moment. The
// original code asked "does this event_id exist?" and inserted if it did not.
// Between those two steps another request ran the same check and got the same
// answer, so both inserted. The schema did not stop it either: migration 001
// created a plain index on events.event_id, which makes lookups fast but
// permits duplicate values.
//
// The pre-existing TestDuplicateDeliveryIsIgnored posts three times in
// sequence, so it never opens that window.
func TestConcurrentDuplicateDeliveryCountsOnce(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	warmUp(t, srv.URL, st, deliveries)

	body := eventJSON(eventID, callID, accountID)
	codes := make([]int, deliveries)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release every goroutine at the same instant
			resp, err := http.Post(srv.URL+"/webhooks/calls",
				"application/json", strings.NewReader(body))
			if err != nil {
				t.Errorf("delivery %d: %v", i, err)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(io.Discard, resp.Body)
			codes[i] = resp.StatusCode
		}(i)
	}
	close(start)
	wg.Wait()

	// Every delivery must be acknowledged. A non-2xx makes the provider retry
	// an event we have in fact already stored.
	for i, code := range codes {
		if code != http.StatusOK {
			t.Errorf("delivery %d: got %d, want 200", i, code)
		}
	}

	var stored int
	row := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&stored); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if stored != 1 {
		t.Errorf("stored %d copies of the event, want 1", stored)
	}

	durable, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if durable.CallCount != 1 {
		t.Errorf("account_stats.call_count is %d, want 1", durable.CallCount)
	}
	if durable.TotalDurationSec != 143 {
		t.Errorf("account_stats.total_duration_sec is %d, want 143", durable.TotalDurationSec)
	}

	cached := getStats(t, srv.URL, accountID)
	if cached.CallCount != 1 {
		t.Errorf("in-memory call_count is %d, want 1", cached.CallCount)
	}
	if cached.TotalDurationSec != 143 {
		t.Errorf("in-memory total_duration_sec is %d, want 143", cached.TotalDurationSec)
	}
}
