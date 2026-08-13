package cache

import "sync/atomic"

// evictOldest removes the least-recently-used entry from the shard.
// Callers must hold s.mu.
func (s *shard) evictOldest() {
	elem := s.ll.Back()
	if elem == nil {
		return
	}
	key := elem.Value.(string)
	s.ll.Remove(elem)
	delete(s.keys, key)
	delete(s.store, key)
	atomic.AddInt64(&s.evictions, 1)
}

func (c *Cache) Len() int {
	total := 0
	for _, s := range c.shards {
		s.mu.RLock()
		total += s.ll.Len()
		s.mu.RUnlock()
	}
	return total
}
