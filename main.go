package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"go-cache/internals/cache"
)

type SetRequest struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	TTLMs int         `json:"ttl_ms"`
}

func main() {
	// Parse CLI flags or fallback to environment variables
	portFlag := flag.String("port", getEnv("PORT", "8080"), "Port to listen on")
	maxSizeFlag := flag.Int("max-size", getEnvInt("MAX_SIZE", 1000), "Maximum cache size before eviction")
	cleanupIntervalFlag := flag.Duration("cleanup-interval", getEnvDuration("CLEANUP_INTERVAL", 10*time.Second), "Interval for the active TTL cleanup janitor")
	flag.Parse()

	log.Printf("Starting SentinelCache Node...")
	log.Printf("Configuration: Port=%s, MaxSize=%d, CleanupInterval=%v", *portFlag, *maxSizeFlag, *cleanupIntervalFlag)

	// Initialize the LRU cache
	c := cache.NewCacheWithMaxSize(*maxSizeFlag)

	// Start the active TTL janitor
	c.StartJanitor(*cleanupIntervalFlag)
	defer c.StopJanitor()

	// Register HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/set", handleSet(c))
	mux.HandleFunc("/get", handleGet(c))
	mux.HandleFunc("/delete", handleDelete(c))
	mux.HandleFunc("/health", handleHealth())

	// Start server
	serverAddr := ":" + *portFlag
	log.Printf("Listening and serving HTTP on %s", serverAddr)
	if err := http.ListenAndServe(serverAddr, mux); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

func handleSet(c *cache.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error": "Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		var req SetRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error": "Invalid request body"}`, http.StatusBadRequest)
			return
		}

		if req.Key == "" {
			http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
			return
		}

		var ttl time.Duration
		if req.TTLMs > 0 {
			ttl = time.Duration(req.TTLMs) * time.Millisecond
		}

		c.Set(req.Key, req.Value, ttl)
		log.Printf("[SET] Key: %s, TTL: %v", req.Key, ttl)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":     req.Key,
			"success": true,
		})
	}
}

func handleGet(c *cache.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error": "Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
			return
		}

		val, found := c.Get(key)
		w.Header().Set("Content-Type", "application/json")

		if !found {
			log.Printf("[GET] Key: %s - MISS", key)
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"key":   key,
				"found": false,
			})
			return
		}

		log.Printf("[GET] Key: %s - HIT", key)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":   key,
			"value": val,
			"found": true,
		})
	}
}

func handleDelete(c *cache.Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, `{"error": "Method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
			return
		}

		c.Delete(key)
		log.Printf("[DELETE] Key: %s", key)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":     key,
			"success": true,
		})
	}
}

func handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "healthy",
		})
	}
}

// Helper functions to fetch and parse environment variables
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
