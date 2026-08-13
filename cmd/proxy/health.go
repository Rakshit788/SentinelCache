package main

import (
	"log"
	"net/http"
	"sync"
	"time"
)

// startHealthChecker runs a background loop that probes every known node's
// /health endpoint, tracks consecutive successes/failures per node, and
// drives automatic ring membership: a node is dropped from the ring after
// failThreshold consecutive failures and rejoins after recoverThreshold
// consecutive successes.
func (p *ProxyServer) startHealthChecker(interval time.Duration, failThreshold, recoverThreshold int) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			p.checkAllNodes(failThreshold, recoverThreshold)
		}
	}()
}

func (p *ProxyServer) checkAllNodes(failThreshold, recoverThreshold int) {
	var wg sync.WaitGroup
	for _, node := range p.allNodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			p.checkNode(node, failThreshold, recoverThreshold)
		}(node)
	}
	wg.Wait()
}

func (p *ProxyServer) checkNode(node string, failThreshold, recoverThreshold int) {
	resp, err := p.healthClient.Get(node + "/health")
	ok := err == nil && resp.StatusCode == http.StatusOK
	if resp != nil {
		resp.Body.Close()
	}

	h := p.health[node]
	h.mu.Lock()
	defer h.mu.Unlock()

	if ok {
		h.consecutiveFails = 0
		h.consecutiveOK++
		if !h.inRing && h.consecutiveOK >= recoverThreshold {
			p.ring.AddNode(node)
			h.inRing = true
			h.healthy = true
			log.Printf("[HEALTH] Node %s recovered after %d consecutive successful checks - added back to ring", node, h.consecutiveOK)
		}
		return
	}

	h.consecutiveOK = 0
	h.consecutiveFails++
	if h.inRing && h.consecutiveFails >= failThreshold {
		p.ring.RemoveNode(node)
		h.inRing = false
		h.healthy = false
		log.Printf("[HEALTH] Node %s marked unhealthy after %d consecutive failed checks - removed from ring", node, h.consecutiveFails)
	}
}
