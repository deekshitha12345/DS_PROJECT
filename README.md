# Distributed Cache System

This project implements a Redis-inspired distributed cache in Python.

## Design

The system is split into four layers:

- `distributed_cache/cache`: in-memory storage, TTL, and LRU eviction.
- `distributed_cache/cluster`: consistent hashing, replicas, membership, and heartbeats.
- `distributed_cache/api`: FastAPI request handlers for client and node-to-node traffic.
- `tests`: unit and integration tests for store, routing, failure handling, and API behavior.

### Folder Structure

```text
distributed_cache/
  cache/
  cluster/
  api/
tests/
scripts/
docker/
```

### Design Choices

- FastAPI is used for a lightweight HTTP API and easy local multi-node simulation.
- A shared hash-ring module handles sharding deterministically.
- Each node keeps an in-memory store and can replicate writes to a configured replica.
- A cluster manager tracks node health with heartbeat timestamps and routes around failed primaries.

## Run Locally

Install dependencies with `pip install -e .[dev]`, then start the demo gateway with `python -m distributed_cache.entrypoint`.

For a multi-container run, use `docker compose up --build`. The compose file starts the gateway and three node services.

## Testing

Run `python -m pytest -q`.

The suite covers cache CRUD, TTL expiration, LRU eviction, consistent hashing, cluster routing, failover, and the HTTP API.

## Demo Flow

1. Start the service.
2. `PUT /cache/{key}` with a JSON body containing `value` and optional `ttl_seconds`.
3. `GET /cache/{key}` to confirm sharding and retrieval.
4. Stop or stale a node heartbeat to exercise replica failover.

## Performance Check

Run `python scripts/benchmark.py` to measure average PUT/GET latency and approximate throughput in the in-process cluster.

