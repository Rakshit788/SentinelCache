# SentinelCache

SentinelCache is a distributed in-memory cache written in Go. The goal is a self-healing cache cluster with TTL expiration, LRU eviction, consistent hashing, quorum-based replication, node failure detection, and automatic recovery behavior.

The project is now at the self-healing cluster stage: cache nodes work independently, a proxy routes requests using consistent hashing, writes/reads use configurable quorum (W/R), and the proxy actively health-checks every node, removing failed ones from the ring and re-adding them (with read-repaired data) once they recover.

## Current Architecture

```text
Client
  |
  v
Proxy Router :8080
  |
  |-- consistent hashing ring (auto add/remove on health change)
  |-- replication factor N, write quorum W, read quorum R
  |-- background health checker (per-node consecutive fail/ok tracking)
  |-- quorum reads pick the highest-version replica response
  |-- async read repair backfills stale/missing replicas
  |
  |--> cache-node-1 :8081
  |--> cache-node-2 :8082
  |--> cache-node-3 :8083
```

## What Is Done

### Phase 1: Single-Node Engine

- In-memory key-value cache, sharded into 32 independent segments (own mutex, map, and LRU list per shard) so unrelated keys don't contend on a single lock.
- Thread-safe cache access using per-shard `sync.RWMutex`.
- `Set`, `Get`, and `Delete` operations.
- TTL support per key.
- Background TTL janitor using `StartJanitor`, sweeping every shard fully on each tick.
- `Get` immediately purges expired entries instead of just reporting a miss.
- LRU eviction when max cache size is reached (per shard).
- Graceful shutdown: `SIGINT`/`SIGTERM` drains in-flight requests via `http.Server.Shutdown` before the process exits.
- Every value carries a `Version` (last-write-wins timestamp). `Set` assigns a fresh, strictly-increasing version per key; `SetWithVersion` applies a caller-supplied version and rejects writes older than what's stored, which is what makes quorum writes and read repair safe to replay.
- HTTP cache node API:
  - `POST /set` (accepts optional `version`; returns `version` and `applied`)
  - `GET /get?key=...` (returns `version` and `expires_at`)
  - `DELETE /delete?key=...`
  - `GET /health`
- Config through flags/env vars:
  - `PORT`
  - `MAX_SIZE`
  - `CLEANUP_INTERVAL`
- Cache unit tests for:
  - set/get
  - delete
  - TTL expiry
  - expired cleanup (single key and full-cache sweep across all shards)
  - LRU eviction
  - LRU touch behavior
  - Get purging an expired entry on read
  - concurrent access under the race detector
  - monotonically increasing versions on repeated writes
  - `SetWithVersion` rejecting stale writes / applying newer ones
  - expiration surviving a versioned write

### Phase 2: Ring Router

- Consistent hashing ring.
- Virtual nodes.
- Add node support.
- Remove node support.
- Primary node lookup for a key.
- Multiple unique replica lookup using `GetNodes`.
- Unit tests for:
  - add/get behavior
  - remove behavior
  - empty ring
  - distribution sanity
  - replica selection

### Phase 3: Static Cluster

- Proxy service in `cmd/proxy` (split into `main.go` setup, `health.go` health checking, `handlers.go` request handling).
- Static backend node list through `NODES`.
- Configurable virtual node count through `VNODES`.
- Configurable replication factor through `REPLICATION`.
- Docker image builds both:
  - `cache-server`
  - `cache-proxy`
- Docker Compose defines:
  - `cache-node-1`
  - `cache-node-2`
  - `cache-node-3`
  - `proxy`
- `.gitignore` excludes local Go cache/build artifacts.

### Phase 4: Self-Healing Cluster

- **Quorum reads/writes.** Configurable write quorum `W` and read quorum `R` (env `WRITE_QUORUM` / `READ_QUORUM`, clamped to `[1, REPLICATION]`). A write succeeds only once at least `W` replicas ack; a read succeeds once at least `R` replicas respond, and the client gets the highest-version value among them (last-write-wins) instead of the first replica that happens to answer.
- **Versioned writes across replicas.** The proxy assigns one version per logical write and sends it to every replica, so all replicas agree on ordering for that write instead of drifting on local clocks.
- **Health detection.** A background goroutine (`cmd/proxy/health.go`) pings every configured node's `/health` on an interval (`HEALTH_INTERVAL`), tracking consecutive successes/failures per node.
- **Automatic ring membership.** A node is removed from the consistent-hash ring after `HEALTH_FAIL_THRESHOLD` consecutive failures, and added back after `HEALTH_RECOVER_THRESHOLD` consecutive successes — the proxy keeps probing removed nodes so it notices recovery. `GET /members` on the proxy exposes live health/ring state per node for debugging.
- **Read repair.** After a quorum read, any replica that returned a stale or missing value is asynchronously patched with the winning value and version in the background (never blocks the client response), preserving the winner's remaining TTL so a repaired key doesn't become permanent by accident.
- Verified manually end-to-end: killing a node drops it from the ring within `HEALTH_FAIL_THRESHOLD` ticks while quorum ops keep succeeding on the remaining replicas; restarting it triggers automatic rejoin, and the next quorum `GET` for a key it missed backfills it via read repair.

### Phase 5: Performance & Observability

- **Benchmarks with real, measured numbers** (not estimates) comparing the pre-sharding design (a single global `sync.RWMutex` over the whole cache, reproduced via `newCacheWithShards(0, 1)`) against the default 32-shard cache — see [Benchmarks](#benchmarks) below.
- **`go test -bench` suite** (`internals/cache/bench_test.go`): `BenchmarkGlobalLockCache_{Get,Set,Mixed}` vs `BenchmarkShardedCache_{Get,Set,Mixed}`, run with `b.RunParallel` across `GOMAXPROCS` goroutines and `-benchmem` for per-op allocations.
- **`cmd/benchtool`**, a standalone load generator that records every operation's latency (not just the mean) and reports real p50/p95/p99 percentiles, throughput, and memory/GC stats — `go test -bench` alone can't produce percentiles.
- **Cache-level metrics**: `Cache.Stats()` returns hits/misses/evictions/expirations, tracked with per-shard atomic counters (summed on read) so scraping stats doesn't itself reintroduce cross-shard lock contention.
- **Hand-rolled Prometheus `/metrics` endpoint** (`internals/metrics`: `Counter`, `Gauge`, `Histogram` + a text-exposition writer, no external dependency) on both the cache node and the proxy — see [Observability](#observability) below.

## What Is Partially Done

- Docker Compose includes the proxy, but service health checks are not defined yet (the proxy has its own internal health checker independent of Compose's).
- Deletes are not versioned (no tombstones), so a delete is best-effort across quorum — a replica that misses a delete can still be "read-repaired" back to life by a stale replica's stale-but-versioned value. This needs a tombstone + garbage-collection design (see Self-Healing Features below).
- Automatic ring membership reacts to health, but there's no rebalancing/data migration when a node's ring position changes — keys simply hash to whatever nodes are currently in the ring.

## What Is Left

### High Priority

- Add a `.dockerignore` file.
- Add `/stats` endpoint on cache nodes.
- Add proxy integration tests using `httptest` (including quorum, health-driven ring changes, and read repair).
- Add Docker Compose health checks for cache nodes and proxy.

### Self-Healing Features

- Add tombstoned deletes so delete is last-write-wins consistent under read repair, with a GC pass to reclaim tombstones after they've propagated.
- Add hinted handoff:
  - if a replica is down during write, store a pending write and replay it when the node returns (currently the write quorum just proceeds without the down replica, and it catches up only via read repair on a subsequent read).
- Add rebalancing when nodes join or rejoin (currently ring membership changes but data doesn't proactively move).
- Add a periodic anti-entropy repair loop instead of relying only on read-triggered repair.

### Gossip Roadmap

- Add membership state per node:
  - node ID
  - address
  - heartbeat counter
  - status: alive, suspect, dead
  - last seen time
- Add node endpoints:
  - `POST /gossip`
  - `GET /members`
  - optional `POST /join`
- Periodically gossip membership information to peers.
- Mark nodes suspect/dead when heartbeats stop.
- Connect gossip membership changes to the proxy/ring membership.

### Production Polish

- Add request logging middleware (structured, beyond the current `log.Printf` lines).
- Add benchmarks for hash ring lookup and end-to-end proxy routing (cache Get/Set are covered; the ring and proxy aren't yet).
- Add CI workflow.
- Add full API documentation.
- Add architecture and system design docs.
- Deduplicate the `getEnv*` config helpers currently copy-pasted between `cmd/server` and `cmd/proxy` into a shared `internals/config` package.

## Recommended Next Steps

1. Add tombstoned deletes (fixes the resurrection-via-read-repair gap noted above).
2. Add proxy integration tests (`httptest`) covering quorum thresholds, ring add/remove on health change, and read repair.
3. Add hinted handoff so a replica that's down during a write catches up as soon as it's healthy again, not just lazily on the next read.
4. Add `.dockerignore` and Docker Compose health checks.
5. Add `/stats` to cache nodes.
6. Wire Prometheus + Grafana into `docker-compose.yml` to actually visualize the `/metrics` output instead of just curling it.
7. Start gossip membership once hinted handoff and tombstones are in place.

## Run With Docker Compose

Start the full stack:

```powershell
docker compose up --build -d
```

Services:

| Service | Host Port | Container Port |
| --- | ---: | ---: |
| proxy | 8080 | 8080 |
| cache-node-1 | 8081 | 8080 |
| cache-node-2 | 8082 | 8080 |
| cache-node-3 | 8083 | 8080 |

Check proxy health:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/health"
```

Check per-node health/ring membership as the proxy sees it:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/members"
```

Try self-healing: stop a node (`docker compose stop cache-node-1`), watch `/members` show it as `in_ring: false` within a few health-check intervals, confirm `/get`/`/set` still work via quorum on the remaining two nodes, then `docker compose start cache-node-1` and watch it rejoin automatically.

Set a value through the proxy:

```powershell
$body = @{key='smoke'; value='ok'; ttl_ms=0} | ConvertTo-Json -Compress
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/set" -ContentType "application/json" -Body $body
```

Get a value through the proxy:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/get?key=smoke"
```

Delete a value through the proxy:

```powershell
Invoke-RestMethod -Method Delete -Uri "http://localhost:8080/delete?key=smoke"
```

Stop the stack:

```powershell
docker compose down
```

## Tests

Run tests locally:

```powershell
go test ./...
```

Run tests with the race detector (recommended — the cache is sharded and accessed concurrently):

```powershell
go test -race ./...
```

If local Go execution is blocked by Windows Application Control, run tests inside Docker:

```powershell
docker run --rm -v "${PWD}:/app" -w /app golang:1.26.1-alpine go test -race ./...
```

The same Application Control policy can also intermittently block *running a pre-built `.exe`* directly (a fresh binary hash gets flagged) even though it allows `go test`. If `./gocache-server.exe` refuses to run, `go run ./cmd/server` reliably works around it — `go run` was used to produce every manually-verified result in this README.

## Benchmarks

Sharding the cache (`internals/cache`, [Phase 1](#phase-1-single-node-engine)) replaced a single `sync.RWMutex` over the whole keyspace with 32 independently-locked shards. Two ways to measure the effect are included:

```powershell
# Standard Go benchmarks: mean ns/op and allocations, global-lock vs sharded
go test -bench=. -benchmem -run=^$ ./internals/cache/...

# Standalone load generator: real throughput and p50/p95/p99 latency
# (go test -bench only reports the mean, not percentiles)
go run ./cmd/benchtool -mode=global -ops=4000000 -read-ratio=0.9
go run ./cmd/benchtool -mode=sharded -ops=4000000 -read-ratio=0.9
```

**Measured results** (Intel i5-10200H, 8 logical cores, Windows, Go 1.26.1; `-mode=global` reproduces the pre-sharding design via a single shard, `-mode=sharded` is the current default of 32 shards; workload: 10,000-key space, 90% Get / 10% Set, 32 concurrent workers):

| Metric | Global lock (1 shard) | Sharded (32 shards) | Improvement |
| --- | ---: | ---: | ---: |
| Throughput | ~2.08M ops/sec | ~10.28M ops/sec | **~4.9x** |
| p95 latency | ~51.7µs | ~16.5µs | **~3.1x lower** |
| p99 latency | ~62.8µs | ~28.1µs | **~2.2x lower** |

`go test -bench` on the same machine shows the same story at the single-operation level (`-benchmem`, `Get`): global lock **422.7 ns/op**, sharded **109.6 ns/op** (~3.9x), with 0 B/op for both — sharding buys concurrency, not extra allocations.

These are the actual numbers from this machine; run the commands above to reproduce them on yours before quoting a number anywhere (a resume, an interview) — hardware and `GOMAXPROCS` change the result.

## Observability

Both the cache node and the proxy expose `GET /metrics` in Prometheus text exposition format, written by a small dependency-free package (`internals/metrics`) rather than the official client library.

Cache node metrics:

| Metric | Type | Meaning |
| --- | --- | --- |
| `cache_hits_total` | counter | `Get` calls that found a live key |
| `cache_misses_total` | counter | `Get` calls that found no live key |
| `cache_evictions_total` | counter | Keys evicted by the LRU policy |
| `cache_expirations_total` | counter | Keys removed for being past their TTL |
| `request_count{method}` | counter | Requests handled, by `set`/`get`/`delete` |
| `request_latency_seconds{method}` | histogram | Request handling latency |

Proxy metrics (in addition to `request_count`/`request_latency_seconds`):

| Metric | Type | Meaning |
| --- | --- | --- |
| `replica_read_failures_total` | counter | Failed GETs to individual replica nodes |
| `replica_write_failures_total` | counter | Failed SET/DELETEs to individual replica nodes |
| `quorum_success_total` | counter | Client requests that met their required quorum |
| `quorum_failure_total` | counter | Client requests that failed to meet their required quorum |
| `read_repairs_total` | counter | Background read-repair writes sent to stale/missing replicas |
| `healthy_nodes` | gauge | Backend nodes currently considered healthy |
| `ring_nodes` | gauge | Backend nodes currently present in the consistent-hash ring |

Try it against a running node or proxy:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/metrics"
```

To scrape it with Prometheus, add a job like this to `prometheus.yml` and point it at the proxy and each node:

```yaml
scrape_configs:
  - job_name: sentinelcache
    static_configs:
      - targets: ["localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083"]
```

Not yet wired up: an actual `prometheus`/`grafana` service in `docker-compose.yml` to scrape and visualize this (see [Recommended Next Steps](#recommended-next-steps)) — today it's `/metrics` you can curl or point an external Prometheus at.

## Project Pitch

SentinelCache is a distributed cache in Go. Each node provides TTL and LRU-based in-memory storage, sharded across 32 independently-locked segments so concurrent access to unrelated keys doesn't contend, with lazy and active TTL expiry, versioned values, and graceful shutdown. A proxy uses consistent hashing with virtual nodes to route keys, replicating writes with configurable quorum (W) and serving reads with configurable quorum (R), resolving conflicts by picking the highest-version replica response. The proxy continuously health-checks every node, automatically removing failed nodes from the ring and re-adding them once they recover, and asynchronously read-repairs any replica that fell behind. Sharding the cache is backed by measured numbers, not a guess: on the reference machine it moved concurrent GET throughput from ~2.08M to ~10.28M ops/sec (~4.9x) with p99 latency dropping ~2.2x (see [Benchmarks](#benchmarks)), and every node/proxy exposes a hand-rolled Prometheus `/metrics` endpoint for live cache, request, quorum, and replication-health stats (see [Observability](#observability)). The next milestones are tombstoned deletes, hinted handoff, and a gossip-based membership protocol to replace the current static node list.
