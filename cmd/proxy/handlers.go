package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func (p *ProxyServer) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error": "Failed to read request body"}`, http.StatusBadRequest)
		return
	}

	var req SetRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		http.Error(w, `{"error": "Invalid JSON format"}`, http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
		return
	}

	targetNodes := p.ring.GetNodes(req.Key, p.replicationFactor)
	if len(targetNodes) < p.writeQuorum {
		p.metrics.quorumFailure.Inc()
		http.Error(w, fmt.Sprintf(`{"error": "not enough healthy replicas: have %d, need write quorum %d"}`, len(targetNodes), p.writeQuorum), http.StatusServiceUnavailable)
		return
	}

	// The proxy assigns one version for this write and sends it to every
	// replica, so replicas agree on ordering instead of each stamping
	// their own local time.
	req.Version = time.Now().UnixNano()
	replicatedBody, err := json.Marshal(req)
	if err != nil {
		http.Error(w, `{"error": "Failed to encode replicated write"}`, http.StatusInternalServerError)
		return
	}

	successCount := p.replicateSet(targetNodes, replicatedBody)

	if successCount < p.writeQuorum {
		p.metrics.quorumFailure.Inc()
		http.Error(w, fmt.Sprintf(`{"error": "write quorum not met: %d/%d succeeded, need %d"}`, successCount, len(targetNodes), p.writeQuorum), http.StatusInternalServerError)
		return
	}
	p.metrics.quorumSuccess.Inc()

	log.Printf("[PROXY SET] Key: %s version %d replicated to %d/%d nodes (quorum %d)", req.Key, req.Version, successCount, len(targetNodes), p.writeQuorum)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":           req.Key,
		"success":       true,
		"version":       req.Version,
		"write_targets": targetNodes,
		"success_count": successCount,
		"write_quorum":  p.writeQuorum,
	})
}

// replicateSet fans a single already-encoded SetRequest body out to every
// target node in parallel and returns how many acknowledged with 200 OK.
func (p *ProxyServer) replicateSet(targetNodes []string, body []byte) int {
	var wg sync.WaitGroup
	successChan := make(chan bool, len(targetNodes))

	for _, nodeAddr := range targetNodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			resp, err := p.client.Post(addr+"/set", "application/json", bytes.NewReader(body))
			if err != nil {
				log.Printf("Replicated write to %s failed: %v", addr, err)
				p.metrics.replicaWriteFailures.Inc()
				successChan <- false
				return
			}
			defer resp.Body.Close()
			ok := resp.StatusCode == http.StatusOK
			if !ok {
				p.metrics.replicaWriteFailures.Inc()
			}
			successChan <- ok
		}(nodeAddr)
	}

	wg.Wait()
	close(successChan)

	successCount := 0
	for success := range successChan {
		if success {
			successCount++
		}
	}
	return successCount
}

type replicaGetResult struct {
	addr      string
	ok        bool // got a valid HTTP response, hit or miss
	found     bool
	value     interface{}
	version   int64
	expiresAt int64
}

func (p *ProxyServer) getFromReplica(addr, key string) replicaGetResult {
	res := replicaGetResult{addr: addr}

	urlStr := fmt.Sprintf("%s/get?key=%s", addr, url.QueryEscape(key))
	resp, err := p.client.Get(urlStr)
	if err != nil {
		log.Printf("[QUORUM GET] Request to %s failed: %v", addr, err)
		p.metrics.replicaReadFailures.Inc()
		return res
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		log.Printf("[QUORUM GET] Node %s returned status %d", addr, resp.StatusCode)
		p.metrics.replicaReadFailures.Inc()
		return res
	}

	res.ok = true
	if resp.StatusCode == http.StatusOK {
		var body GetResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err == nil {
			res.found = body.Found
			res.value = body.Value
			res.version = body.Version
			res.expiresAt = body.ExpiresAt
		}
	}
	return res
}

func (p *ProxyServer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
		return
	}

	targetNodes := p.ring.GetNodes(key, p.replicationFactor)
	if len(targetNodes) < p.readQuorum {
		p.metrics.quorumFailure.Inc()
		http.Error(w, fmt.Sprintf(`{"error": "not enough healthy replicas: have %d, need read quorum %d"}`, len(targetNodes), p.readQuorum), http.StatusServiceUnavailable)
		return
	}

	results := make([]replicaGetResult, len(targetNodes))
	var wg sync.WaitGroup
	for i, nodeAddr := range targetNodes {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			results[i] = p.getFromReplica(addr, key)
		}(i, nodeAddr)
	}
	wg.Wait()

	okCount := 0
	var winner *replicaGetResult
	for i := range results {
		if results[i].ok {
			okCount++
		}
		if results[i].found && (winner == nil || results[i].version > winner.version) {
			winner = &results[i]
		}
	}

	if okCount < p.readQuorum {
		p.metrics.quorumFailure.Inc()
		http.Error(w, fmt.Sprintf(`{"error": "read quorum not met: %d/%d replicas responded, need %d"}`, okCount, len(targetNodes), p.readQuorum), http.StatusServiceUnavailable)
		return
	}
	p.metrics.quorumSuccess.Inc()

	w.Header().Set("Content-Type", "application/json")

	if winner == nil {
		log.Printf("[PROXY GET] Key: %s not found on any replica (quorum %d/%d met)", key, okCount, p.readQuorum)
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":   key,
			"found": false,
		})
		return
	}

	log.Printf("[PROXY GET] Key: %s served from %s at version %d (quorum %d/%d)", key, winner.addr, winner.version, okCount, p.readQuorum)
	json.NewEncoder(w).Encode(GetResponse{
		Key:       key,
		Value:     winner.value,
		Found:     true,
		Version:   winner.version,
		ExpiresAt: winner.expiresAt,
	})

	// Read repair runs after the response is sent so it never adds
	// latency to the client's request.
	go p.readRepair(key, *winner, results)
}

// readRepair pushes the winning (highest-version) value to any replica
// that responded with a stale or missing copy. It preserves the winner's
// remaining TTL so a repaired key doesn't accidentally become permanent.
func (p *ProxyServer) readRepair(key string, winner replicaGetResult, results []replicaGetResult) {
	var ttlMs int
	if winner.expiresAt > 0 {
		remaining := time.Until(time.Unix(0, winner.expiresAt))
		if remaining <= 0 {
			// Already expired since the read - nothing to repair with.
			return
		}
		ttlMs = int(remaining.Milliseconds())
		if ttlMs < 1 {
			ttlMs = 1
		}
	}

	repairReq := SetRequest{
		Key:     key,
		Value:   winner.value,
		TTLMs:   ttlMs,
		Version: winner.version,
	}
	body, err := json.Marshal(repairReq)
	if err != nil {
		return
	}

	for _, res := range results {
		if res.addr == winner.addr {
			continue
		}
		if res.found && res.version >= winner.version {
			continue
		}

		p.metrics.readRepairsTotal.Inc()
		go func(addr string) {
			resp, err := p.client.Post(addr+"/set", "application/json", bytes.NewReader(body))
			if err != nil {
				log.Printf("[READ REPAIR] key=%s failed to repair %s: %v", key, addr, err)
				return
			}
			defer resp.Body.Close()
			log.Printf("[READ REPAIR] key=%s repaired %s to version %d (status %d)", key, addr, winner.version, resp.StatusCode)
		}(res.addr)
	}
}

func (p *ProxyServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, `{"error": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
		return
	}

	targetNodes := p.ring.GetNodes(key, p.replicationFactor)
	if len(targetNodes) < p.writeQuorum {
		p.metrics.quorumFailure.Inc()
		http.Error(w, fmt.Sprintf(`{"error": "not enough healthy replicas: have %d, need write quorum %d"}`, len(targetNodes), p.writeQuorum), http.StatusServiceUnavailable)
		return
	}

	var wg sync.WaitGroup
	successChan := make(chan bool, len(targetNodes))

	for _, nodeAddr := range targetNodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			reqUrl := fmt.Sprintf("%s/delete?key=%s", addr, url.QueryEscape(key))
			req, err := http.NewRequest(http.MethodDelete, reqUrl, nil)
			if err != nil {
				successChan <- false
				return
			}
			resp, err := p.client.Do(req)
			if err != nil {
				log.Printf("Replicated delete from %s failed: %v", addr, err)
				p.metrics.replicaWriteFailures.Inc()
				successChan <- false
				return
			}
			defer resp.Body.Close()
			ok := resp.StatusCode == http.StatusOK
			if !ok {
				p.metrics.replicaWriteFailures.Inc()
			}
			successChan <- ok
		}(nodeAddr)
	}

	wg.Wait()
	close(successChan)

	successCount := 0
	for success := range successChan {
		if success {
			successCount++
		}
	}

	if successCount < p.writeQuorum {
		p.metrics.quorumFailure.Inc()
		http.Error(w, fmt.Sprintf(`{"error": "delete quorum not met: %d/%d succeeded, need %d"}`, successCount, len(targetNodes), p.writeQuorum), http.StatusInternalServerError)
		return
	}
	p.metrics.quorumSuccess.Inc()

	log.Printf("[PROXY DELETE] Key: %s deleted from %d/%d nodes (quorum %d)", key, successCount, len(targetNodes), p.writeQuorum)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":           key,
		"success":       true,
		"delete_count":  successCount,
		"total_targets": len(targetNodes),
		"write_quorum":  p.writeQuorum,
	})
}
