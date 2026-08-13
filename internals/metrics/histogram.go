package metrics

import (
	"math"
	"sort"
	"sync/atomic"
	"time"
)

// DefaultBuckets are latency bucket upper bounds in seconds, spanning
// sub-millisecond cache operations up to multi-second worst cases - wide
// enough to be reused for both cache-op and HTTP-request latencies.
var DefaultBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5,
}

// Histogram tracks the distribution of observed durations in fixed
// buckets, matching Prometheus histogram semantics. Safe for concurrent
// use.
type Histogram struct {
	boundsNs []int64
	counts   []int64 // per-bucket, non-cumulative; counts[len(boundsNs)] is the +Inf overflow bucket
	sumNs    int64
	count    int64
}

func NewHistogram(bucketsSeconds []float64) *Histogram {
	bounds := make([]int64, len(bucketsSeconds))
	for i, s := range bucketsSeconds {
		bounds[i] = int64(s * float64(time.Second))
	}
	return &Histogram{
		boundsNs: bounds,
		counts:   make([]int64, len(bounds)+1),
	}
}

func (h *Histogram) Observe(d time.Duration) {
	ns := int64(d)
	idx := sort.Search(len(h.boundsNs), func(i int) bool { return h.boundsNs[i] >= ns })
	atomic.AddInt64(&h.counts[idx], 1)
	atomic.AddInt64(&h.sumNs, ns)
	atomic.AddInt64(&h.count, 1)
}

// HistogramSnapshot is a point-in-time, cumulative view of a Histogram,
// suitable for Prometheus text exposition.
type HistogramSnapshot struct {
	// BucketUpperBoundsSeconds[i] is the "le" value for CumulativeCounts[i].
	// The last entry is always +Inf.
	BucketUpperBoundsSeconds []float64
	CumulativeCounts         []int64
	SumSeconds                float64
	Count                     int64
}

func (h *Histogram) Snapshot() HistogramSnapshot {
	n := len(h.boundsNs)
	bounds := make([]float64, n+1)
	for i, ns := range h.boundsNs {
		bounds[i] = float64(ns) / float64(time.Second)
	}
	bounds[n] = math.Inf(1)

	cum := make([]int64, n+1)
	var running int64
	for i := 0; i <= n; i++ {
		running += atomic.LoadInt64(&h.counts[i])
		cum[i] = running
	}

	return HistogramSnapshot{
		BucketUpperBoundsSeconds: bounds,
		CumulativeCounts:         cum,
		SumSeconds:               float64(atomic.LoadInt64(&h.sumNs)) / float64(time.Second),
		Count:                    atomic.LoadInt64(&h.count),
	}
}
