package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type HashRing struct {
	vnodes int
	ring   []uint32
	nodes  map[uint32]string
	mu     sync.RWMutex
}

// NewHashRing creates a new HashRing with the specified number of virtual nodes per physical node.
func NewHashRing(vnodes int) *HashRing {
	if vnodes <= 0 {
		vnodes = 50 // Default fallback
	}
	return &HashRing{
		vnodes: vnodes,
		nodes:  make(map[uint32]string),
	}
}

// hash calculates the CRC32 checksum of the key.
func (h *HashRing) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// AddNode adds a new physical node to the ring, creating vnodes virtual nodes for it.
func (h *HashRing) AddNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.vnodes; i++ {
		vnodeKey := node + "#" + strconv.Itoa(i)
		vnodeHash := h.hash(vnodeKey)

		h.nodes[vnodeHash] = node
		h.ring = append(h.ring, vnodeHash)
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

// RemoveNode removes a physical node and its virtual nodes from the ring.
func (h *HashRing) RemoveNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	newRing := make([]uint32, 0, len(h.ring))
	for _, vnodeHash := range h.ring {
		if mappedNode, ok := h.nodes[vnodeHash]; ok && mappedNode == node {
			delete(h.nodes, vnodeHash)
		} else {
			newRing = append(newRing, vnodeHash)
		}
	}
	h.ring = newRing
}

// GetNode retrieves the closest physical node on the ring for the given key.
func (h *HashRing) GetNode(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return ""
	}

	keyHash := h.hash(key)

	// Binary search to find the first virtual node hash >= keyHash
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= keyHash
	})

	// Wrap around to 0 if the key hash is greater than all hashes on the ring
	if idx == len(h.ring) {
		idx = 0
	}

	return h.nodes[h.ring[idx]]
}
