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
	"syscall"
	"time"

	"go-cache/internals/cache"
)

type SetRequest struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	TTLMs int         `json:"ttl_ms"`
	// Version is optional. When set (by the proxy, coordinating a single
	// logical write across replicas, or by read repair) it is applied via
	// last-write-wins instead of generating a fresh version locally, so
	// every replica agrees on the same version for the same write.
	Version int64 `json:"version,omitempty"`
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

	m := newServerMetrics()

	// Register HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/set", withMetrics("set", m, handleSet(c)))
	mux.HandleFunc("/get", withMetrics("get", m, handleGet(c)))
	mux.HandleFunc("/delete", withMetrics("delete", m, handleDelete(c)))
	mux.HandleFunc("/health", handleHealth())
	mux.HandleFunc("/metrics", handleMetrics(c, m))

	// Start server
	srv := &http.Server{
		Addr:    ":" + *portFlag,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("Listening and serving HTTP on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("Shutdown signal received, draining connections...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Graceful shutdown failed: %v", err)
	}

	c.StopJanitor()
	log.Printf("Server stopped cleanly")
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

		var version int64
		applied := true
		if req.Version > 0 {
			version = req.Version
			applied = c.SetWithVersion(req.Key, req.Value, ttl, req.Version)
		} else {
			version = c.Set(req.Key, req.Value, ttl)
		}
		log.Printf("[SET] Key: %s, TTL: %v, Version: %d, Applied: %v", req.Key, ttl, version, applied)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":     req.Key,
			"success": true,
			"applied": applied,
			"version": version,
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

		val, version, expiresAt, found := c.Get(key)
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

		log.Printf("[GET] Key: %s - HIT (version %d)", key, version)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"key":        key,
			"value":      val,
			"found":      true,
			"version":    version,
			"expires_at": expiresAt,
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
