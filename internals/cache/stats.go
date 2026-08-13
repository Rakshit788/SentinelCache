package cache

import "sync/atomic"

// Stats is a point-in-time snapshot of cache-wide counters, summed across
// all shards.
type Stats struct {
	Hits        int64
	Misses      int64
	Evictions   int64
	Expirations int64
}

// Stats returns the current cache-wide metric counters. Safe to call
// concurrently with any other Cache method.
func (c *Cache) Stats() Stats {
	var s Stats
	for _, sh := range c.shards {
		s.Hits += atomic.LoadInt64(&sh.hits)
		s.Misses += atomic.LoadInt64(&sh.misses)
		s.Evictions += atomic.LoadInt64(&sh.evictions)
		s.Expirations += atomic.LoadInt64(&sh.expirations)
	}
	return s
}
