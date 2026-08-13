package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go-cache/internals/consistent_hashing"
)

type SetRequest struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	TTLMs int         `json:"ttl_ms"`
	// Version is set by the proxy once per logical write and sent
	// unchanged to every replica, so all replicas agree on a single
	// version for the same write instead of each generating their own.
	Version int64 `json:"version,omitempty"`
}

type GetResponse struct {
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	Found     bool        `json:"found"`
	Version   int64       `json:"version"`
	ExpiresAt int64       `json:"expires_at"`
}

type nodeHealth struct {
	mu               sync.Mutex
	healthy          bool
	inRing           bool
	consecutiveOK    int
	consecutiveFails int
}

type ProxyServer struct {
	ring              *consistenthash.HashRing
	client            *http.Client
	healthClient      *http.Client
	replicationFactor int
	writeQuorum       int
	readQuorum        int

	// allNodes is the full static node list, independent of ring
	// membership, so the health checker keeps probing nodes that have
	// been removed from the ring and can add them back once they recover.
	allNodes []string
	health   map[string]*nodeHealth

	metrics *proxyMetrics
}

func main() {
	portFlag := flag.String("port", getEnv("PORT", "8080"), "Port to listen on")
	nodesFlag := flag.String("nodes", getEnv("NODES", "http://cache-node-1:8080,http://cache-node-2:8080,http://cache-node-3:8080"), "Comma-separated list of backend node URLs")
	vnodesFlag := flag.Int("vnodes", getEnvInt("VNODES", 50), "Number of virtual nodes per physical node")
	replFlag := flag.Int("replication", getEnvInt("REPLICATION", 2), "Replication factor N")
	writeQuorumFlag := flag.Int("write-quorum", getEnvInt("WRITE_QUORUM", 1), "Number of replica acks required for a write to succeed (W)")
	readQuorumFlag := flag.Int("read-quorum", getEnvInt("READ_QUORUM", 1), "Number of replica responses required for a read to succeed (R)")
	healthIntervalFlag := flag.Duration("health-interval", getEnvDuration("HEALTH_INTERVAL", 2*time.Second), "Interval between node health checks")
	healthFailThresholdFlag := flag.Int("health-fail-threshold", getEnvInt("HEALTH_FAIL_THRESHOLD", 3), "Consecutive failed health checks before a node is removed from the ring")
	healthRecoverThresholdFlag := flag.Int("health-recover-threshold", getEnvInt("HEALTH_RECOVER_THRESHOLD", 2), "Consecutive successful health checks before a removed node rejoins the ring")
	flag.Parse()

	replicationFactor := *replFlag
	writeQuorum := clamp(*writeQuorumFlag, 1, replicationFactor)
	readQuorum := clamp(*readQuorumFlag, 1, replicationFactor)

	log.Printf("Starting SentinelCache Proxy Router...")
	log.Printf("Configuration: Port=%s, Nodes=%s, Vnodes=%d, Replication=%d, WriteQuorum=%d, ReadQuorum=%d",
		*portFlag, *nodesFlag, *vnodesFlag, replicationFactor, writeQuorum, readQuorum)

	// Create and populate the Hash Ring
	ring := consistenthash.NewHashRing(*vnodesFlag)
	var allNodes []string
	health := make(map[string]*nodeHealth)
	for _, n := range strings.Split(*nodesFlag, ",") {
		trimmed := strings.TrimSpace(n)
		if trimmed == "" {
			continue
		}
		ring.AddNode(trimmed)
		allNodes = append(allNodes, trimmed)
		health[trimmed] = &nodeHealth{healthy: true, inRing: true}
		log.Printf("Added physical node to ring: %s", trimmed)
	}

	proxy := &ProxyServer{
		ring:              ring,
		replicationFactor: replicationFactor,
		writeQuorum:       writeQuorum,
		readQuorum:        readQuorum,
		allNodes:          allNodes,
		health:            health,
		metrics:           newProxyMetrics(),
		client: &http.Client{
			Timeout: 500 * time.Millisecond,
		},
		healthClient: &http.Client{
			Timeout: 300 * time.Millisecond,
		},
	}

	proxy.startHealthChecker(*healthIntervalFlag, *healthFailThresholdFlag, *healthRecoverThresholdFlag)

	mux := http.NewServeMux()
	mux.HandleFunc("/set", withMetrics("set", proxy.metrics, proxy.handleSet))
	mux.HandleFunc("/get", withMetrics("get", proxy.metrics, proxy.handleGet))
	mux.HandleFunc("/delete", withMetrics("delete", proxy.metrics, proxy.handleDelete))
	mux.HandleFunc("/health", proxy.handleHealth)
	mux.HandleFunc("/members", proxy.handleMembers)
	mux.HandleFunc("/metrics", proxy.handleMetrics)

	srv := &http.Server{
		Addr:    ":" + *portFlag,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("Listening and serving HTTP Proxy on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Proxy failed to start: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("Shutdown signal received, draining connections...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Graceful shutdown failed: %v", err)
	}

	log.Printf("Proxy stopped cleanly")
}

func (p *ProxyServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

// handleMembers reports the proxy's current view of each backend node's
// health, useful for debugging automatic ring membership.
func (p *ProxyServer) handleMembers(w http.ResponseWriter, r *http.Request) {
	type memberView struct {
		Node             string `json:"node"`
		Healthy          bool   `json:"healthy"`
		InRing           bool   `json:"in_ring"`
		ConsecutiveOK    int    `json:"consecutive_ok"`
		ConsecutiveFails int    `json:"consecutive_fails"`
	}

	members := make([]memberView, 0, len(p.allNodes))
	for _, node := range p.allNodes {
		h := p.health[node]
		h.mu.Lock()
		members = append(members, memberView{
			Node:             node,
			Healthy:          h.healthy,
			InRing:           h.inRing,
			ConsecutiveOK:    h.consecutiveOK,
			ConsecutiveFails: h.consecutiveFails,
		})
		h.mu.Unlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(members)
}

func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
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

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if valStr, exists := os.LookupEnv(key); exists {
		if val, err := time.ParseDuration(valStr); err == nil {
			return val
		}
	}
	return fallback
}
