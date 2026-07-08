package cache

import (
	"container/list"
	"sync"
	"time"
)

type CacheItem struct {
	Value      interface{}
	Expiration int64
}

type Cache struct {
	mu       sync.RWMutex
	store    map[string]CacheItem
	ll       *list.List
	keys     map[string]*list.Element
	maxSize  int
	stopChan chan struct{}
	stopOnce sync.Once
}

func NewCache() *Cache {
	return &Cache{
		store:    make(map[string]CacheItem),
		ll:       list.New(),
		keys:     make(map[string]*list.Element),
		stopChan: make(chan struct{}),
	}
}

func (c *Cache) Set(key string, value interface{}, ttl time.Duration) {
	var expiration int64
	if ttl > 0 {
		expiration = time.Now().Add(ttl).UnixNano()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store[key] = CacheItem{
		Value:      value,
		Expiration: expiration,
	}

	if elem, ok := c.keys[key]; ok {
		c.ll.MoveToFront(elem)
	} else {
		c.keys[key] = c.ll.PushFront(key)
	}

	if c.maxSize > 0 && c.ll.Len() > c.maxSize {
		c.evictOldest()
	}
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.store[key]
	if !ok {
		return nil, false
	}

	if item.Expiration > 0 && item.Expiration < time.Now().UnixNano() {
		return nil, false
	}

	if elem, ok := c.keys[key]; ok {
		c.ll.MoveToFront(elem)
	}

	return item.Value, true
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.keys[key]; ok {
		c.ll.Remove(elem)
		delete(c.keys, key)
	}
	delete(c.store, key)
}
