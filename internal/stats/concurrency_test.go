package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

// TestCacheRecordIsSafeUnderConcurrency is the regression test for the
// unguarded map write in Cache.Record.
//
// Cache carries a sync.RWMutex and Get takes it, but Record read and mutated
// the map without holding it at all. Two consequences follow. Two webhooks for
// one account can read the same CallCount and write back the same incremented
// value, so one call is silently lost. Worse, concurrent writes to a Go map
// are detected by the runtime and abort the whole process with "fatal error:
// concurrent map writes" -- not a recoverable error.
//
// Run with -race to see the data race reported directly.
func TestCacheRecordIsSafeUnderConcurrency(t *testing.T) {
	c := stats.NewCache()

	const (
		accounts       = 4
		perAccount     = 250
		durationPerAdd = 2
	)

	var wg sync.WaitGroup
	for a := 0; a < accounts; a++ {
		for i := 0; i < perAccount; i++ {
			wg.Add(1)
			go func(a int) {
				defer wg.Done()
				c.Record(accountID(a), durationPerAdd)
			}(a)
		}
	}
	wg.Wait()

	for a := 0; a < accounts; a++ {
		got := c.Get(accountID(a))
		if got.CallCount != perAccount {
			t.Errorf("%s: CallCount is %d, want %d (increments were lost)",
				accountID(a), got.CallCount, perAccount)
		}
		if want := int64(perAccount * durationPerAdd); got.TotalDurationSec != want {
			t.Errorf("%s: TotalDurationSec is %d, want %d",
				accountID(a), got.TotalDurationSec, want)
		}
	}
}

// TestCacheReadsAndWritesInterleave exercises Get against Record, which is
// what happens when the stats endpoint is polled while webhooks arrive.
func TestCacheReadsAndWritesInterleave(t *testing.T) {
	c := stats.NewCache()
	const n = 500

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.Record("acc_interleave", 1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			_ = c.Get("acc_interleave")
		}
	}()
	wg.Wait()

	if got := c.Get("acc_interleave"); got.CallCount != n {
		t.Errorf("CallCount is %d, want %d", got.CallCount, n)
	}
}

func accountID(i int) string {
	return [...]string{"acc_a", "acc_b", "acc_c", "acc_d"}[i]
}
