package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-cache/internals/consistent_hashing"
)

type SetRequest struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	TTLMs int         `json:"ttl_ms"`
}

type ProxyServer struct {
	ring              *consistenthash.HashRing
	client            *http.Client
	replicationFactor int
}

func main() {
	portFlag := flag.String("port", getEnv("PORT", "8080"), "Port to listen on")
	nodesFlag := flag.String("nodes", getEnv("NODES", "http://cache-node-1:8080,http://cache-node-2:8080,http://cache-node-3:8080"), "Comma-separated list of backend node URLs")
	vnodesFlag := flag.Int("vnodes", getEnvInt("VNODES", 50), "Number of virtual nodes per physical node")
	replFlag := flag.Int("replication", getEnvInt("REPLICATION", 2), "Replication factor N")
	flag.Parse()

	log.Printf("Starting SentinelCache Proxy Router...")
	log.Printf("Configuration: Port=%s, Nodes=%s, Vnodes=%d, Replication=%d", *portFlag, *nodesFlag, *vnodesFlag, *replFlag)

	// Create and populate the Hash Ring
	ring := consistenthash.NewHashRing(*vnodesFlag)
	nodeList := strings.Split(*nodesFlag, ",")
	for _, n := range nodeList {
		trimmed := strings.TrimSpace(n)
		if trimmed != "" {
			ring.AddNode(trimmed)
			log.Printf("Added physical node to ring: %s", trimmed)
		}
	}

	proxy := &ProxyServer{
		ring:              ring,
		replicationFactor: *replFlag,
		client: &http.Client{
			Timeout: 500 * time.Millisecond,
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/set", proxy.handleSet)
	mux.HandleFunc("/get", proxy.handleGet)
	mux.HandleFunc("/delete", proxy.handleDelete)
	mux.HandleFunc("/health", proxy.handleHealth)

	serverAddr := ":" + *portFlag
	log.Printf("Listening and serving HTTP Proxy on %s", serverAddr)
	if err := http.ListenAndServe(serverAddr, mux); err != nil {
		log.Fatalf("Proxy failed to start: %v", err)
	}
}

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

	// Find the N replica nodes for this key
	targetNodes := p.ring.GetNodes(req.Key, p.replicationFactor)
	if len(targetNodes) == 0 {
		http.Error(w, `{"error": "No backend nodes available"}`, http.StatusInternalServerError)
		return
	}

	var wg sync.WaitGroup
	successChan := make(chan bool, len(targetNodes))

	// Send write request to all replica nodes in parallel
	for _, nodeAddr := range targetNodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			url := fmt.Sprintf("%s/set", addr)
			resp, err := p.client.Post(url, "application/json", bytes.NewBuffer(bodyBytes))
			if err != nil {
				log.Printf("Replicated write to %s failed: %v", addr, err)
				successChan <- false
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				successChan <- true
			} else {
				log.Printf("Replicated write to %s returned status %d", addr, resp.StatusCode)
				successChan <- false
			}
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

	if successCount == 0 {
		http.Error(w, `{"error": "All replicated write attempts failed"}`, http.StatusInternalServerError)
		return
	}

	log.Printf("[PROXY SET] Key: %s replicated successfully to %d/%d nodes", req.Key, successCount, len(targetNodes))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":           req.Key,
		"success":       true,
		"write_targets": targetNodes,
		"success_count": successCount,
	})
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
	if len(targetNodes) == 0 {
		http.Error(w, `{"error": "No backend nodes available"}`, http.StatusInternalServerError)
		return
	}

	// Try reading from the target nodes sequentially (Failover logic)
	for i, nodeAddr := range targetNodes {
		urlStr := fmt.Sprintf("%s/get?key=%s", nodeAddr, url.QueryEscape(key))
		resp, err := p.client.Get(urlStr)
		if err != nil {
			log.Printf("[FAILOVER GET] Attempt %d to %s failed: %v. Trying next replica...", i+1, nodeAddr, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			// Found the key, return the response directly
			w.Header().Set("Content-Type", "application/json")
			io.Copy(w, resp.Body)
			log.Printf("[PROXY GET] Key: %s fetched from %s", key, nodeAddr)
			return
		} else if resp.StatusCode == http.StatusNotFound {
			log.Printf("[PROXY GET] Key: %s not found on %s. Checking replicas...", key, nodeAddr)
		} else {
			log.Printf("[FAILOVER GET] Node %s returned status %d. Trying next replica...", nodeAddr, resp.StatusCode)
		}
	}

	// If we checked all nodes and none returned the key successfully, return 404 NotFound
	log.Printf("[PROXY GET] Key: %s not found on any replica", key)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":   key,
		"found": false,
		"error": "Key not found on any replica nodes",
	})
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
	if len(targetNodes) == 0 {
		http.Error(w, `{"error": "No backend nodes available"}`, http.StatusInternalServerError)
		return
	}

	var wg sync.WaitGroup
	successChan := make(chan bool, len(targetNodes))

	// Replicate deletion in parallel
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
				successChan <- false
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				successChan <- true
			} else {
				successChan <- false
			}
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

	log.Printf("[PROXY DELETE] Key: %s deleted from %d/%d nodes", key, successCount, len(targetNodes))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":           key,
		"success":       true,
		"delete_count":  successCount,
		"total_targets": len(targetNodes),
	})
}

func (p *ProxyServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if valStr, exists := os.LookupEnv(key); exists {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return fallback
}
