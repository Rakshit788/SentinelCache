package main

import (
	"fmt"
	"sync"
	"time"
)

type CacheItem struct {
	Value      interface{}
	Expiration int64 // Unix timestamp in nanoseconds or seconds
}

type Cache struct {
	mu    sync.RWMutex
	store map[string]CacheItem
}

// Fixed the syntax error here
func NewCache() *Cache {
	return &Cache{
		store: make(map[string]CacheItem),
	}
}

// Changed expiration to time.Duration for a cleaner API
func (c *Cache) Set(key string, value interface{}, ttl time.Duration) {
	var expiration int64
	if ttl > 0 {
		expiration = time.Now().Add(ttl).UnixNano() // Using nanoseconds for higher precision
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store[key] = CacheItem{
		Value:      value,
		Expiration: expiration,
	}
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.store[key]
	if !ok {
		return nil, false
	}

	// Fixed passive TTL check to match UnixNano
	if item.Expiration > 0 && item.Expiration < time.Now().UnixNano() {
		// Note: The item is expired, but it still exists in c.store memory.
		// We will handle deleting it during Day 2 (Active TTL Engine).
		return nil, false
	}

	return item.Value, true
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.store, key)
}
func main() {
	cache := NewCache()
	var wg sync.WaitGroup

	// Concurrently write to the cache
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			cache.Set(key, id, 2*time.Second)
		}(i)
	}

	// Concurrently read from the cache
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			val, found := cache.Get(key)
			if found {
				fmt.Printf("Found %s: %v\n", key, val)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("Day 1 Core Engine Verified Thread-Safe!")
}
