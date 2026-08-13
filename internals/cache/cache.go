package cache

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// defaultShardCount is the number of independent shards a Cache is split
// into. Splitting the keyspace across shards means Set/Get/Delete on
// different keys can proceed concurrently without contending on a single
// global lock.
const defaultShardCount = 32

type CacheItem struct {
	Value      interface{}
	Expiration int64
	// Version is a last-write-wins timestamp (UnixNano). Replicas compare
	// versions to resolve conflicts during quorum reads and read repair.
	Version int64
}

type shard struct {
	mu      sync.RWMutex
	store   map[string]CacheItem
	ll      *list.List
	keys    map[string]*list.Element
	maxSize int

	// Metric counters, kept per-shard and summed on read (see stats.go) so
	// that reporting hits/misses doesn't itself reintroduce cross-shard
	// contention on every Get.
	hits        int64
	misses      int64
	evictions   int64
	expirations int64
}

func newShard(maxSize int) *shard {
	return &shard{
		store:   make(map[string]CacheItem),
		ll:      list.New(),
		keys:    make(map[string]*list.Element),
		maxSize: maxSize,
	}
}

type Cache struct {
	shards   []*shard
	stopChan chan struct{}
	stopOnce sync.Once
}

func NewCache() *Cache {
	return newCacheWithShards(0, defaultShardCount)
}

func NewCacheWithMaxSize(maxSize int) *Cache {
	return newCacheWithShards(maxSize, defaultShardCount)
}

// NewCacheWithShardCount builds a Cache with an explicit shard count.
// Exposed for benchmarking/comparison tools (e.g. cmd/benchtool) that need
// to construct a single-shard "global lock" cache to measure against the
// default sharded configuration; application code should use NewCache or
// NewCacheWithMaxSize instead.
func NewCacheWithShardCount(maxSize, shardCount int) *Cache {
	return newCacheWithShards(maxSize, shardCount)
}

// newCacheWithShards builds a Cache with an explicit shard count so tests
// can pin shard count to 1 for deterministic, whole-cache LRU ordering.
func newCacheWithShards(maxSize, shardCount int) *Cache {
	if shardCount < 1 {
		shardCount = 1
	}

	perShardMax := 0
	if maxSize > 0 {
		perShardMax = maxSize / shardCount
		if perShardMax < 1 {
			perShardMax = 1
		}
	}

	shards := make([]*shard, shardCount)
	for i := range shards {
		shards[i] = newShard(perShardMax)
	}

	return &Cache{
		shards:   shards,
		stopChan: make(chan struct{}),
	}
}

func (c *Cache) shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return c.shards[h.Sum32()%uint32(len(c.shards))]
}

// Set stores value under key with the given ttl, assigning it a fresh
// version, and returns that version. The version is derived from the
// current time but bumped past whatever version is already stored for
// this key, so repeated local writes always produce a strictly increasing
// sequence even if the clock's resolution doesn't (observed on Windows).
func (c *Cache) Set(key string, value interface{}, ttl time.Duration) int64 {
	var expiration int64
	if ttl > 0 {
		expiration = time.Now().Add(ttl).UnixNano()
	}

	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	version := time.Now().UnixNano()
	if existing, ok := s.store[key]; ok && existing.Version >= version {
		version = existing.Version + 1
	}

	s.applyLocked(key, value, expiration, version)
	return version
}

// SetWithVersion stores value under key only if version is not older than
// whatever is currently stored (last-write-wins). It reports whether the
// write was applied. This lets callers replay writes (replication, read
// repair) without a late-arriving stale write clobbering newer data.
func (c *Cache) SetWithVersion(key string, value interface{}, ttl time.Duration, version int64) bool {
	var expiration int64
	if ttl > 0 {
		expiration = time.Now().Add(ttl).UnixNano()
	}

	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.store[key]; ok && existing.Version > version {
		return false
	}

	s.applyLocked(key, value, expiration, version)
	return true
}

// applyLocked writes the item and updates LRU/eviction state. Callers must
// hold s.mu.
func (s *shard) applyLocked(key string, value interface{}, expiration, version int64) {
	s.store[key] = CacheItem{
		Value:      value,
		Expiration: expiration,
		Version:    version,
	}

	if elem, ok := s.keys[key]; ok {
		s.ll.MoveToFront(elem)
	} else {
		s.keys[key] = s.ll.PushFront(key)
	}

	if s.maxSize > 0 && s.ll.Len() > s.maxSize {
		s.evictOldest()
	}
}

// Get returns the value, version, and expiration (UnixNano, 0 meaning no
// expiry) stored for key. The last return value reports whether the key
// was found (and not expired). Expiration is surfaced so callers doing
// replication or read repair can preserve a key's remaining TTL instead
// of accidentally making it permanent.
func (c *Cache) Get(key string) (interface{}, int64, int64, bool) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.store[key]
	if !ok {
		atomic.AddInt64(&s.misses, 1)
		return nil, 0, 0, false
	}

	if item.Expiration > 0 && item.Expiration < time.Now().UnixNano() {
		s.deleteKey(key)
		atomic.AddInt64(&s.expirations, 1)
		atomic.AddInt64(&s.misses, 1)
		return nil, 0, 0, false
	}

	if elem, ok := s.keys[key]; ok {
		s.ll.MoveToFront(elem)
	}

	atomic.AddInt64(&s.hits, 1)
	return item.Value, item.Version, item.Expiration, true
}

func (c *Cache) Delete(key string) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteKey(key)
}

// deleteKey removes key from the shard. Callers must hold s.mu.
func (s *shard) deleteKey(key string) {
	if elem, ok := s.keys[key]; ok {
		s.ll.Remove(elem)
		delete(s.keys, key)
	}
	delete(s.store, key)
}
