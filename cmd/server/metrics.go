package main

import (
	"net/http"
	"time"

	"go-cache/internals/cache"
	"go-cache/internals/metrics"
)

// serverMetrics holds request-level counters/histograms for this node, one
// per HTTP method the node serves. Cache-level counters (hits, misses,
// evictions, expirations) live on the cache itself and are read directly
// from cache.Stats() when scraped, rather than duplicated here.
type serverMetrics struct {
	requestCount   map[string]*metrics.Counter
	requestLatency map[string]*metrics.Histogram
}

func newServerMetrics() *serverMetrics {
	methods := []string{"set", "get", "delete"}
	m := &serverMetrics{
		requestCount:   make(map[string]*metrics.Counter, len(methods)),
		requestLatency: make(map[string]*metrics.Histogram, len(methods)),
	}
	for _, method := range methods {
		m.requestCount[method] = &metrics.Counter{}
		m.requestLatency[method] = metrics.NewHistogram(metrics.DefaultBuckets)
	}
	return m
}

// withMetrics wraps a handler so every request to it is counted and timed
// under the given method label, without the handler itself needing to
// know about metrics.
func withMetrics(method string, m *serverMetrics, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h(w, r)
		m.requestCount[method].Inc()
		m.requestLatency[method].Observe(time.Since(start))
	}
}

func handleMetrics(c *cache.Cache, m *serverMetrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		stats := c.Stats()
		metrics.WriteCounter(w, "cache_hits_total", "Total cache Get calls that found a live key.", "", stats.Hits)
		metrics.WriteCounter(w, "cache_misses_total", "Total cache Get calls that found no live key.", "", stats.Misses)
		metrics.WriteCounter(w, "cache_evictions_total", "Total keys evicted by the LRU policy.", "", stats.Evictions)
		metrics.WriteCounter(w, "cache_expirations_total", "Total keys removed for being past their TTL.", "", stats.Expirations)

		for _, method := range []string{"set", "get", "delete"} {
			labels := `method="` + method + `"`
			metrics.WriteCounter(w, "request_count", "Total requests handled, by method.", labels, m.requestCount[method].Value())
			metrics.WriteHistogram(w, "request_latency_seconds", "Request handling latency in seconds, by method.", labels, m.requestLatency[method].Snapshot())
		}
	}
}
