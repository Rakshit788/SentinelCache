package cache

import (
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

func (c *Cache) DeleteExpired() {
	const maxSampleSize = 20

	now := time.Now().UnixNano()

	c.mu.Lock()
	defer c.mu.Unlock()

	checked := 0
	for key, item := range c.store {
		if item.Expiration > 0 && item.Expiration < now {
			if elem, ok := c.keys[key]; ok {
				c.ll.Remove(elem)
				delete(c.keys, key)
			}
			delete(c.store, key)
		}

		checked++
		if checked >= maxSampleSize {
			return
		}
	}
}