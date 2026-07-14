# SentinelCache

SentinelCache is a distributed in-memory cache written in Go. The goal is to build a self-healing cache cluster with TTL expiration, LRU eviction, consistent hashing, replication, node failure detection, and automatic recovery behavior.

The project is currently at the static distributed-cache stage: cache nodes work independently, a proxy routes requests using consistent hashing, and writes/deletes are replicated to multiple nodes.

## Current Architecture

```text
Client
  |
  v
Proxy Router :8080
  |
  |-- consistent hashing ring
  |-- replication factor N=2
  |-- read failover across replicas
  |
  |--> cache-node-1 :8081
  |--> cache-node-2 :8082
  |--> cache-node-3 :8083
```

## What Is Done

### Phase 1: Single-Node Engine

- In-memory key-value cache.
- Thread-safe cache access using `sync.RWMutex`.
- `Set`, `Get`, and `Delete` operations.
- TTL support per key.
- Background TTL janitor using `StartJanitor`.
- LRU eviction when max cache size is reached.
- HTTP cache node API:
  - `POST /set`
  - `GET /get?key=...`
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
  - expired cleanup
  - LRU eviction
  - LRU touch behavior

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

- Proxy service in `cmd/proxy`.
- Static backend node list through `NODES`.
- Configurable virtual node count through `VNODES`.
- Configurable replication factor through `REPLICATION`.
- `SET` requests are forwarded to replica nodes in parallel.
- `GET` requests try replica nodes with failover.
- `DELETE` requests are forwarded to replica nodes in parallel.
- Docker image builds both:
  - `cache-server`
  - `cache-proxy`
- Docker Compose defines:
  - `cache-node-1`
  - `cache-node-2`
  - `cache-node-3`
  - `proxy`
- `.gitignore` excludes local Go cache/build artifacts.

## What Is Partially Done

- The proxy has basic read failover, but it does not yet track node health.
- The hash ring supports removing nodes, but the proxy does not yet remove failed nodes automatically.
- Docker Compose includes the proxy, but service health checks are not defined yet.
- TTL expiry works, but `Get` currently returns a miss for expired keys without immediately removing them from the cache.
- Replication exists, but there is no quorum logic yet.

## What Is Left

### High Priority

- Add a `.dockerignore` file.
- Add `/stats` endpoint on cache nodes.
- Delete expired keys immediately inside `Get`.
- Add proxy integration tests using `httptest`.
- Add Docker Compose health checks for cache nodes and proxy.
- Add a proxy health checker that periodically calls each node's `/health`.
- Track node state in the proxy:
  - healthy
  - unhealthy
  - last seen time
  - failed health check count
- Remove unhealthy nodes from the active hash ring.
- Add recovered nodes back into the active hash ring.

### Self-Healing Features

- Add read repair:
  - if one replica has a value and another replica misses it, repair the missing replica in the background.
- Add value metadata:
  - version
  - updated timestamp
  - optional expiry timestamp in API responses
- Add hinted handoff:
  - if a replica is down during write, store a pending write and replay it when the node returns.
- Add rebalancing when nodes join or rejoin.
- Add anti-entropy repair loop to periodically compare and repair replicas.

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

- Add request logging middleware.
- Add graceful shutdown.
- Add Prometheus-style `/metrics`.
- Add benchmarks for:
  - cache set/get
  - hash ring lookup
  - proxy routing
- Add race detector verification.
- Add CI workflow.
- Add full API documentation.
- Add architecture and system design docs.

## Recommended Next Steps

1. Add `.dockerignore`.
2. Fix expired-key cleanup inside `Get`.
3. Add `/stats` to cache nodes.
4. Add proxy integration tests.
5. Add proxy health checker.
6. Connect health checker to the hash ring.
7. Add read repair.
8. Start gossip after basic health-based self-healing works.

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

If local Go execution is blocked by Windows Application Control, run tests inside Docker:

```powershell
docker run --rm -v "${PWD}:/app" -w /app golang:1.26.1-alpine go test ./...
```

## Project Pitch

SentinelCache is a distributed cache in Go. Each node provides TTL and LRU-based in-memory storage. A proxy uses consistent hashing with virtual nodes to route keys and replicate writes across multiple cache nodes. The next milestone is self-healing: detecting failed nodes, removing them from the active ring, routing traffic to healthy replicas, and repairing data when nodes recover.
