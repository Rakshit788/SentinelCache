package cache

func NewCacheWithMaxSize(maxSize int) *Cache {
	c := NewCache()
	c.maxSize = maxSize
	return c
}

func (c *Cache) evictOldest() {
	elem := c.ll.Back()
	if elem == nil {
		return
	}
	key := elem.Value.(string)
	c.ll.Remove(elem)
	delete(c.keys, key)
	delete(c.store, key)
}

func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ll.Len()
}
