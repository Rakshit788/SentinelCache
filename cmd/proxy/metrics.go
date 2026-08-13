package main

import (
	"net/http"
	"time"

	"go-cache/internals/metrics"
)

// proxyMetrics holds counters/histograms for the proxy. Per-node health
// state (healthy_nodes, ring_nodes) isn't duplicated here - it's read
// live from ProxyServer.health when scraped, since that map is already
// the source of truth.
type proxyMetrics struct {
	requestCount   map[string]*metrics.Counter
	requestLatency map[string]*metrics.Histogram

	replicaReadFailures  metrics.Counter
	replicaWriteFailures metrics.Counter
	quorumSuccess        metrics.Counter
	quorumFailure        metrics.Counter
	readRepairsTotal     metrics.Counter
}

func newProxyMetrics() *proxyMetrics {
	methods := []string{"set", "get", "delete"}
	m := &proxyMetrics{
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
// under the given method label.
func withMetrics(method string, m *proxyMetrics, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h(w, r)
		m.requestCount[method].Inc()
		m.requestLatency[method].Observe(time.Since(start))
	}
}

func (p *ProxyServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	for _, method := range []string{"set", "get", "delete"} {
		labels := `method="` + method + `"`
		metrics.WriteCounter(w, "request_count", "Total requests handled by the proxy, by method.", labels, p.metrics.requestCount[method].Value())
		metrics.WriteHistogram(w, "request_latency_seconds", "Proxy request handling latency in seconds, by method.", labels, p.metrics.requestLatency[method].Snapshot())
	}

	metrics.WriteCounter(w, "replica_read_failures_total", "Total failed GET requests to individual replica nodes.", "", p.metrics.replicaReadFailures.Value())
	metrics.WriteCounter(w, "replica_write_failures_total", "Total failed SET/DELETE requests to individual replica nodes.", "", p.metrics.replicaWriteFailures.Value())
	metrics.WriteCounter(w, "quorum_success_total", "Total client requests that met their required read/write quorum.", "", p.metrics.quorumSuccess.Value())
	metrics.WriteCounter(w, "quorum_failure_total", "Total client requests that failed to meet their required read/write quorum.", "", p.metrics.quorumFailure.Value())
	metrics.WriteCounter(w, "read_repairs_total", "Total background read-repair writes sent to stale or missing replicas.", "", p.metrics.readRepairsTotal.Value())

	healthy, inRing := p.ringCounts()
	metrics.WriteGauge(w, "healthy_nodes", "Number of backend nodes currently considered healthy.", "", int64(healthy))
	metrics.WriteGauge(w, "ring_nodes", "Number of backend nodes currently present in the consistent-hash ring.", "", int64(inRing))
}

// ringCounts returns the current (healthy node count, in-ring node count)
// from the proxy's live health view.
func (p *ProxyServer) ringCounts() (healthy, inRing int) {
	for _, node := range p.allNodes {
		h := p.health[node]
		h.mu.Lock()
		if h.healthy {
			healthy++
		}
		if h.inRing {
			inRing++
		}
		h.mu.Unlock()
	}
	return healthy, inRing
}
