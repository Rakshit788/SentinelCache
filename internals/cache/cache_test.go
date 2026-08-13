package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCacheSetGet(t *testing.T) {
	c := NewCache()

	c.Set("name", "rakshit", 0)

	got, _, _, ok := c.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if got != "rakshit" {
		t.Fatalf("expected rakshit, got %v", got)
	}
}

func TestCacheDelete(t *testing.T) {
	c := NewCache()

	c.Set("name", "rakshit", 0)
	c.Delete("name")

	if _, _, _, ok := c.Get("name"); ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestCacheTTLExpires(t *testing.T) {
	c := NewCache()

	c.Set("short", "value", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if _, _, _, ok := c.Get("short"); ok {
		t.Fatal("expected key to expire")
	}
}

func TestCacheDeleteExpired(t *testing.T) {
	c := NewCache()

	c.Set("expired", "value", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	c.DeleteExpired()

	if c.Len() != 0 {
		t.Fatalf("expected expired key to be removed, got len %d", c.Len())
	}
}

func TestCacheLRUEvictsOldest(t *testing.T) {
	// Pinned to a single shard: LRU order is only guaranteed within a
	// shard, so a multi-shard cache can't make whole-cache ordering
	// promises. This test targets that per-shard guarantee directly.
	c := newCacheWithShards(2, 1)

	c.Set("a", 1, 0)
	c.Set("b", 2, 0)
	c.Set("c", 3, 0)

	if _, _, _, ok := c.Get("a"); ok {
		t.Fatal("expected oldest key a to be evicted")
	}
	if c.Len() != 2 {
		t.Fatalf("expected len 2, got %d", c.Len())
	}
}

func TestCacheLRUTouchMovesKeyToFront(t *testing.T) {
	c := newCacheWithShards(2, 1)

	c.Set("a", 1, 0)
	c.Set("b", 2, 0)

	if _, _, _, ok := c.Get("a"); !ok {
		t.Fatal("expected key a to exist")
	}

	c.Set("c", 3, 0)

	if _, _, _, ok := c.Get("b"); ok {
		t.Fatal("expected key b to be evicted after a was touched")
	}
	if _, _, _, ok := c.Get("a"); !ok {
		t.Fatal("expected key a to remain")
	}
}

func TestCacheGetPurgesExpiredEntry(t *testing.T) {
	c := newCacheWithShards(0, 1)

	c.Set("short", "value", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if _, _, _, ok := c.Get("short"); ok {
		t.Fatal("expected key to expire")
	}

	// A stale Get must not just report a miss - it must actually remove
	// the entry, otherwise expired-but-unread keys leak forever.
	if c.Len() != 0 {
		t.Fatalf("expected Get on expired key to purge it, got len %d", c.Len())
	}
}

func TestCacheDeleteExpiredSweepsEveryShard(t *testing.T) {
	c := NewCache()

	const n = 500
	for i := 0; i < n; i++ {
		c.Set(fmt.Sprintf("key-%d", i), i, 5*time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)

	c.DeleteExpired()

	if c.Len() != 0 {
		t.Fatalf("expected all %d expired keys across all shards to be swept, got len %d", n, c.Len())
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := NewCacheWithMaxSize(100)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			for j := 0; j < 100; j++ {
				c.Set(key, j, 0)
				c.Get(key)
			}
			c.Delete(key)
		}(i)
	}
	wg.Wait()
}

func TestCacheSetReturnsIncreasingVersion(t *testing.T) {
	c := NewCache()

	v1 := c.Set("k", "v1", 0)
	v2 := c.Set("k", "v2", 0)

	if v2 <= v1 {
		t.Fatalf("expected later write to have a newer version, got v1=%d v2=%d", v1, v2)
	}

	_, gotVersion, _, ok := c.Get("k")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if gotVersion != v2 {
		t.Fatalf("expected stored version %d, got %d", v2, gotVersion)
	}
}

func TestSetWithVersionRejectsStaleWrite(t *testing.T) {
	c := NewCache()

	newer := time.Now().UnixNano()
	older := newer - int64(time.Second)

	if !c.SetWithVersion("k", "new", 0, newer) {
		t.Fatal("expected first write to apply")
	}

	if c.SetWithVersion("k", "stale", 0, older) {
		t.Fatal("expected stale write to be rejected")
	}

	val, version, _, ok := c.Get("k")
	if !ok || val != "new" || version != newer {
		t.Fatalf("expected value to remain 'new' at version %d, got %v at %d (found=%v)", newer, val, version, ok)
	}
}

func TestSetWithVersionAppliesNewerWrite(t *testing.T) {
	c := NewCache()

	older := time.Now().UnixNano()
	newer := older + int64(time.Second)

	c.SetWithVersion("k", "old", 0, older)

	if !c.SetWithVersion("k", "new", 0, newer) {
		t.Fatal("expected newer write to apply")
	}

	val, version, _, ok := c.Get("k")
	if !ok || val != "new" || version != newer {
		t.Fatalf("expected value 'new' at version %d, got %v at %d (found=%v)", newer, val, version, ok)
	}
}

func TestSetWithVersionPreservesExpiration(t *testing.T) {
	c := NewCache()

	version := time.Now().UnixNano()
	c.SetWithVersion("k", "v", 5*time.Second, version)

	_, _, expiresAt, ok := c.Get("k")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if expiresAt == 0 {
		t.Fatal("expected a non-zero expiration to be stored")
	}
}

func TestCacheStats(t *testing.T) {
	c := newCacheWithShards(0, 1)

	c.Set("a", 1, 0)
	c.Get("a")       // hit
	c.Get("missing") // miss

	c.Set("short", "v", 5*time.Millisecond)
	time.Sleep(15 * time.Millisecond)
	c.DeleteExpired() // expiration

	stats := c.Stats()
	if stats.Hits != 1 {
		t.Fatalf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Fatalf("expected 1 miss, got %d", stats.Misses)
	}
	if stats.Expirations != 1 {
		t.Fatalf("expected 1 expiration, got %d", stats.Expirations)
	}

	evictC := newCacheWithShards(2, 1)
	evictC.Set("a", 1, 0)
	evictC.Set("b", 2, 0)
	evictC.Set("c", 3, 0) // evicts "a"
	if got := evictC.Stats().Evictions; got != 1 {
		t.Fatalf("expected 1 eviction, got %d", got)
	}
}
