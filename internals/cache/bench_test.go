package cache

import (
	"fmt"
	"math/rand"
	"testing"
)

// benchKeySpace is the number of distinct keys touched by the benchmarks.
// Large enough that concurrent goroutines land in different shards most of
// the time under the sharded cache, which is exactly the scenario sharding
// is meant to help with.
const benchKeySpace = 10000

func benchKeys() []string {
	keys := make([]string, benchKeySpace)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

func populateBenchCache(c *Cache, keys []string) {
	for i, k := range keys {
		c.Set(k, i, 0)
	}
}

// runParallelOp drives b.N operations across GOMAXPROCS goroutines. Each
// goroutine gets its own RNG (seeded once, outside the timed loop) so
// benchmark contention measures the cache's locking, not the stdlib
// global rand mutex.
func runParallelOp(b *testing.B, op func(rng *rand.Rand, keys []string)) {
	keys := benchKeys()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			op(rng, keys)
		}
	})
}

func benchmarkGet(b *testing.B, c *Cache) {
	keys := benchKeys()
	populateBenchCache(c, keys)
	runParallelOp(b, func(rng *rand.Rand, keys []string) {
		c.Get(keys[rng.Intn(benchKeySpace)])
	})
}

func benchmarkSet(b *testing.B, c *Cache) {
	runParallelOp(b, func(rng *rand.Rand, keys []string) {
		c.Set(keys[rng.Intn(benchKeySpace)], 1, 0)
	})
}

// benchmarkMixed models a typical read-heavy cache workload: 90% Get, 10%
// Set against a shared, already-populated key space.
func benchmarkMixed(b *testing.B, c *Cache) {
	keys := benchKeys()
	populateBenchCache(c, keys)
	runParallelOp(b, func(rng *rand.Rand, keys []string) {
		key := keys[rng.Intn(benchKeySpace)]
		if rng.Intn(100) < 90 {
			c.Get(key)
		} else {
			c.Set(key, 1, 0)
		}
	})
}

// "Global lock" cache: a single shard, i.e. every Get/Set/Delete contends
// on one sync.RWMutex for the whole cache - this is the pre-sharding
// design. Compared against the default 32-shard Cache to quantify the
// effect of sharding under concurrent load.
func BenchmarkGlobalLockCache_Get(b *testing.B) { benchmarkGet(b, newCacheWithShards(0, 1)) }
func BenchmarkGlobalLockCache_Set(b *testing.B) { benchmarkSet(b, newCacheWithShards(0, 1)) }
func BenchmarkGlobalLockCache_Mixed(b *testing.B) {
	benchmarkMixed(b, newCacheWithShards(0, 1))
}

func BenchmarkShardedCache_Get(b *testing.B)   { benchmarkGet(b, NewCache()) }
func BenchmarkShardedCache_Set(b *testing.B)   { benchmarkSet(b, NewCache()) }
func BenchmarkShardedCache_Mixed(b *testing.B) { benchmarkMixed(b, NewCache()) }
