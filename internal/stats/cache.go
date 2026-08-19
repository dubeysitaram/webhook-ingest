// Package stats keeps a hot-path, in-memory view of per-account call totals.
//
// The durable copy of these numbers lives in Postgres; this cache exists so
// the stats endpoint does not hit the database on every read.
package stats

import "sync"

// AccountStats is a point-in-time view of one account's totals.
type AccountStats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Cache holds per-account running totals.
//
// Every method that touches m or the structs it points at must hold mu.
// Callers reach the cache from many concurrent request goroutines, and a Go
// map that is written by two goroutines at once aborts the process.
type Cache struct {
	mu sync.RWMutex
	m  map[string]*AccountStats
}

// NewCache returns an empty cache.
func NewCache() *Cache {
	return &Cache{m: make(map[string]*AccountStats)}
}

// Get returns a snapshot of an account's totals. Unknown accounts read as zero.
func (c *Cache) Get(accountID string) AccountStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.m[accountID]
	if !ok {
		return AccountStats{}
	}
	return *s
}

// Record folds one completed call into an account's running totals.
func (c *Cache) Record(accountID string, durationSec int) {
	c.Apply(accountID, 1, int64(durationSec))
}

// Apply adds the given deltas to an account's totals.
//
// Ingestion needs deltas rather than a plain increment because a redelivered
// or corrected event can revise the duration of a call that has already been
// counted. Such an event contributes a callDelta of zero and a durationDelta
// of the difference, which keeps the cache equal to the durable aggregate.
func (c *Cache) Apply(accountID string, callDelta, durationDelta int64) {
	if callDelta == 0 && durationDelta == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	s, ok := c.m[accountID]
	if !ok {
		s = &AccountStats{}
		c.m[accountID] = s
	}
	s.CallCount += callDelta
	s.TotalDurationSec += durationDelta
}

// Load replaces the cache contents with the given totals.
//
// Used at startup to warm the cache from the durable aggregate in Postgres.
// Without it the cache begins every deployment empty and the stats endpoint
// reports zero for accounts whose totals are intact in the database.
func (c *Cache) Load(totals map[string]AccountStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.m = make(map[string]*AccountStats, len(totals))
	for accountID, st := range totals {
		c.m[accountID] = &AccountStats{
			CallCount:        st.CallCount,
			TotalDurationSec: st.TotalDurationSec,
		}
	}
}
