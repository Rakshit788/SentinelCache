package consistenthash

import (
	"strconv"
	"testing"
)

func TestHashRing_AddAndGetNode(t *testing.T) {
	hr := NewHashRing(3) // 3 vnodes per physical node

	hr.AddNode("node1")
	hr.AddNode("node2")
	hr.AddNode("node3")

	// Ensure there are 9 vnodes on the ring
	if len(hr.ring) != 9 {
		t.Errorf("Expected 9 vnodes, got %d", len(hr.ring))
	}

	// Route some keys
	nodeA := hr.GetNode("mykey-1")
	nodeB := hr.GetNode("mykey-2")
	nodeC := hr.GetNode("mykey-3")

	if nodeA == "" || nodeB == "" || nodeC == "" {
		t.Errorf("Expected mapped node names, got empty string: %q, %q, %q", nodeA, nodeB, nodeC)
	}

	// Verify that getting the same key repeatedly is consistent
	for i := 0; i < 10; i++ {
		if hr.GetNode("mykey-1") != nodeA {
			t.Errorf("Routing is not consistent for key 'mykey-1'")
		}
	}
}

func TestHashRing_RemoveNode(t *testing.T) {
	hr := NewHashRing(5)

	hr.AddNode("node1")
	hr.AddNode("node2")

	// Map key to a node
	key := "test-key"
	firstNode := hr.GetNode(key)

	// Remove the other node
	var nodeToRemove string
	if firstNode == "node1" {
		nodeToRemove = "node2"
	} else {
		nodeToRemove = "node1"
	}

	hr.RemoveNode(nodeToRemove)

	// Key should still map to firstNode
	if hr.GetNode(key) != firstNode {
		t.Errorf("Expected key to still map to %s, got %s", firstNode, hr.GetNode(key))
	}

	// Remove firstNode
	hr.RemoveNode(firstNode)

	// Ring is empty now
	if hr.GetNode(key) != "" {
		t.Errorf("Expected empty string on empty ring, got %s", hr.GetNode(key))
	}
}

func TestHashRing_EmptyRing(t *testing.T) {
	hr := NewHashRing(5)
	if hr.GetNode("any-key") != "" {
		t.Errorf("Expected empty string for empty ring, got %s", hr.GetNode("any-key"))
	}
}

func TestHashRing_Distribution(t *testing.T) {
	// A higher number of vnodes should yield a more balanced distribution
	hr := NewHashRing(100)
	hr.AddNode("node-a")
	hr.AddNode("node-b")
	hr.AddNode("node-c")

	counts := make(map[string]int)
	totalKeys := 1000

	for i := 0; i < totalKeys; i++ {
		node := hr.GetNode("key-" + strconv.Itoa(i))
		counts[node]++
	}

	// Ensure all nodes received some share of keys
	if counts["node-a"] == 0 || counts["node-b"] == 0 || counts["node-c"] == 0 {
		t.Errorf("Distribution failed: at least one node has 0 keys mapped. Distribution: %+v", counts)
	}
}

func TestHashRing_GetNodes(t *testing.T) {
	hr := NewHashRing(3)
	hr.AddNode("node1")
	hr.AddNode("node2")
	hr.AddNode("node3")

	// Get 2 nodes
	nodes := hr.GetNodes("some-test-key", 2)
	if len(nodes) != 2 {
		t.Fatalf("Expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Errorf("Expected distinct nodes, got duplicate: %v", nodes)
	}

	// Requesting more nodes than exist in the ring should return all unique physical nodes
	nodes = hr.GetNodes("some-test-key", 5)
	if len(nodes) != 3 {
		t.Fatalf("Expected 3 nodes (maximum available), got %d", len(nodes))
	}
}
