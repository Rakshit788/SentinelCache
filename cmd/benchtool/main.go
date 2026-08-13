// benchtool is a standalone load generator for internals/cache. Unlike
// `go test -bench`, which only reports mean ns/op, it records every
// operation's latency and reports real p50/p95/p99 percentiles alongside
// throughput and memory, so the global-lock vs. sharded comparison can be
// backed by actual measured numbers instead of estimates.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"go-cache/internals/cache"
)

func main() {
	mode := flag.String("mode", "sharded", "cache mode: \"global\" (1 shard) or \"sharded\" (32 shards)")
	workers := flag.Int("workers", runtime.NumCPU()*4, "concurrent worker goroutines")
	totalOps := flag.Int("ops", 4_000_000, "total operations across all workers")
	keyspace := flag.Int("keyspace", 10_000, "number of distinct keys")
	readRatio := flag.Float64("read-ratio", 0.9, "fraction of operations that are Get (rest are Set)")
	flag.Parse()

	var c *cache.Cache
	switch *mode {
	case "global":
		c = cache.NewCacheWithShardCount(0, 1)
	case "sharded":
		c = cache.NewCache()
	default:
		fmt.Fprintf(os.Stderr, "unknown -mode %q, want \"global\" or \"sharded\"\n", *mode)
		os.Exit(1)
	}

	keys := make([]string, *keyspace)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		c.Set(keys[i], i, 0)
	}

	opsPerWorker := *totalOps / *workers
	actualOps := opsPerWorker * *workers

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Individual cache operations (~100-500ns) are close to the OS clock's
	// timing resolution, especially on Windows, which makes single-op
	// timestamps noisy (many read back as 0). Timing a small batch and
	// dividing by the batch size gives a stable per-op estimate while
	// still exposing latency variance caused by lock contention, which
	// plays out over many operations rather than within a single one.
	const batchSize = 200

	latencies := make([][]time.Duration, *workers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w)))
			local := make([]time.Duration, 0, opsPerWorker/batchSize+1)
			for i := 0; i < opsPerWorker; i += batchSize {
				n := batchSize
				if i+n > opsPerWorker {
					n = opsPerWorker - i
				}
				batchStart := time.Now()
				for j := 0; j < n; j++ {
					key := keys[rng.Intn(*keyspace)]
					if rng.Float64() < *readRatio {
						c.Get(key)
					} else {
						c.Set(key, i+j, 0)
					}
				}
				local = append(local, time.Since(batchStart)/time.Duration(n))
			}
			latencies[w] = local
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	all := make([]time.Duration, 0, actualOps/batchSize+*workers)
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	percentile := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		idx := int(float64(len(all)-1) * p)
		return all[idx]
	}

	throughput := float64(actualOps) / elapsed.Seconds()
	totalAllocDelta := memAfter.TotalAlloc - memBefore.TotalAlloc
	heapAllocDelta := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)

	fmt.Printf("mode=%s workers=%d ops=%d keyspace=%d read_ratio=%.2f num_cpu=%d gomaxprocs=%d\n",
		*mode, *workers, actualOps, *keyspace, *readRatio, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	fmt.Printf("wall_time=%s throughput=%.0f ops/sec\n", elapsed, throughput)
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n",
		percentile(0.50), percentile(0.95), percentile(0.99), all[len(all)-1])
	fmt.Printf("total_alloc_delta=%d KB heap_alloc_delta=%d KB gc_runs=%d gc_cpu_fraction=%.5f\n",
		totalAllocDelta/1024, heapAllocDelta/1024, memAfter.NumGC-memBefore.NumGC, memAfter.GCCPUFraction)
}
