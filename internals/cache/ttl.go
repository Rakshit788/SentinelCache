package cache

import (
	"sync/atomic"
	"time"
)

func (c *Cache) StartJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.DeleteExpired()
			case <-c.stopChan:
				return
			}
		}
	}()
}

func (c *Cache) StopJanitor() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
	})
}

// DeleteExpired sweeps every shard for expired keys. Each shard is locked
// independently and only for the duration of its own sweep, so a large
// cache doesn't block reads/writes on unrelated shards while cleaning up.
func (c *Cache) DeleteExpired() {
	now := time.Now().UnixNano()

	for _, s := range c.shards {
		s.mu.Lock()
		for key, item := range s.store {
			if item.Expiration > 0 && item.Expiration < now {
				s.deleteKey(key)
				atomic.AddInt64(&s.expirations, 1)
			}
		}
		s.mu.Unlock()
	}
}
