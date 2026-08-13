// Package metrics provides minimal, dependency-free Counter, Gauge, and
// Histogram primitives plus a Prometheus text-exposition-format writer.
// It intentionally doesn't pull in the official Prometheus client library:
// the wire format is simple enough to hand-write, and doing so keeps the
// binaries free of an extra dependency.
package metrics

import "sync/atomic"

// Counter is a monotonically increasing value, safe for concurrent use.
type Counter struct {
	v int64
}

func (c *Counter) Inc() {
	atomic.AddInt64(&c.v, 1)
}

func (c *Counter) Add(n int64) {
	atomic.AddInt64(&c.v, n)
}

func (c *Counter) Value() int64 {
	return atomic.LoadInt64(&c.v)
}

// Gauge is a value that can move up or down, safe for concurrent use.
type Gauge struct {
	v int64
}

func (g *Gauge) Set(n int64) {
	atomic.StoreInt64(&g.v, n)
}

func (g *Gauge) Inc() {
	atomic.AddInt64(&g.v, 1)
}

func (g *Gauge) Dec() {
	atomic.AddInt64(&g.v, -1)
}

func (g *Gauge) Value() int64 {
	return atomic.LoadInt64(&g.v)
}
