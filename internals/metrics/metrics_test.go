package metrics

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCounter(t *testing.T) {
	c := &Counter{}
	c.Inc()
	c.Add(4)
	if got := c.Value(); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestCounterConcurrent(t *testing.T) {
	c := &Counter{}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()
	if got := c.Value(); got != 10000 {
		t.Fatalf("expected 10000, got %d", got)
	}
}

func TestGauge(t *testing.T) {
	g := &Gauge{}
	g.Set(10)
	g.Inc()
	g.Inc()
	g.Dec()
	if got := g.Value(); got != 11 {
		t.Fatalf("expected 11, got %d", got)
	}
}

func TestHistogramObserveAndSnapshot(t *testing.T) {
	h := NewHistogram([]float64{0.001, 0.01, 0.1})

	h.Observe(500 * time.Microsecond)  // falls in the 0.001 bucket
	h.Observe(5 * time.Millisecond)    // falls in the 0.01 bucket
	h.Observe(50 * time.Millisecond)   // falls in the 0.1 bucket
	h.Observe(500 * time.Millisecond)  // falls in +Inf

	snap := h.Snapshot()
	if snap.Count != 4 {
		t.Fatalf("expected count 4, got %d", snap.Count)
	}
	// Buckets are cumulative: the 0.001 bucket only contains the first
	// observation, but the +Inf bucket contains all of them.
	if snap.CumulativeCounts[0] != 1 {
		t.Fatalf("expected cumulative count 1 in first bucket, got %d", snap.CumulativeCounts[0])
	}
	last := len(snap.CumulativeCounts) - 1
	if snap.CumulativeCounts[last] != 4 {
		t.Fatalf("expected cumulative count 4 in +Inf bucket, got %d", snap.CumulativeCounts[last])
	}
}

func TestWriteCounterFormat(t *testing.T) {
	var buf bytes.Buffer
	WriteCounter(&buf, "cache_hits_total", "Total cache hits.", "", 42)
	out := buf.String()

	for _, want := range []string{
		"# HELP cache_hits_total Total cache hits.",
		"# TYPE cache_hits_total counter",
		"cache_hits_total 42",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestWriteHistogramFormat(t *testing.T) {
	h := NewHistogram([]float64{0.01, 0.1})
	h.Observe(5 * time.Millisecond)

	var buf bytes.Buffer
	WriteHistogram(&buf, "request_latency_seconds", "Request latency.", `method="GET"`, h.Snapshot())
	out := buf.String()

	for _, want := range []string{
		`request_latency_seconds_bucket{method="GET",le="0.01"} 1`,
		`request_latency_seconds_bucket{method="GET",le="+Inf"} 1`,
		"request_latency_seconds_sum",
		"request_latency_seconds_count",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}
