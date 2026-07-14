package cache

import (
	"testing"
	"time"
)

func TestCacheSetGet(t *testing.T) {
	c := NewCache()

	c.Set("name", "rakshit", 0)

	got, ok := c.Get("name")
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

	if _, ok := c.Get("name"); ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestCacheTTLExpires(t *testing.T) {
	c := NewCache()

	c.Set("short", "value", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if _, ok := c.Get("short"); ok {
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
	c := NewCacheWithMaxSize(2)

	c.Set("a", 1, 0)
	c.Set("b", 2, 0)
	c.Set("c", 3, 0)

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected oldest key a to be evicted")
	}
	if c.Len() != 2 {
		t.Fatalf("expected len 2, got %d", c.Len())
	}
}

func TestCacheLRUTouchMovesKeyToFront(t *testing.T) {
	c := NewCacheWithMaxSize(2)

	c.Set("a", 1, 0)
	c.Set("b", 2, 0)

	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected key a to exist")
	}

	c.Set("c", 3, 0)

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected key b to be evicted after a was touched")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected key a to remain")
	}
}
