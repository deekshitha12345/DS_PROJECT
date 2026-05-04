# Distributed Cache Metrics Report

**Generated:** 2026-05-05 01:13:09

## Summary

- **Total Operations:** 685
- **Success Rate:** 96.4%

## Operations Breakdown

| Operation Type | Count | Successful | Success Rate | Mean Latency (ms) | Throughput (ops/sec) |
|---|---|---|---|---|---|
| GET | 505 | 480 | 95.0% | 28.20 | 0.74 |
| PUT | 180 | 180 | 100.0% | 33.95 | 1.00 |

## Latency Statistics by Node Count

| Node Count | Operations | Mean Latency (ms) | Min (ms) | Max (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|---|---|---|
| 1 | 148 | 44.51 | 22.00 | 116.00 | 72.00 | 92.00 |
| 2 | 148 | 33.48 | 12.00 | 147.00 | 87.00 | 116.00 |
| 3 | 249 | 22.82 | 2.00 | 143.00 | 47.00 | 121.00 |
| 4 | 140 | 22.34 | 3.00 | 47.00 | 45.00 | 47.00 |

## Latency by Operation Type and Node Count

### Node Count = 1

| Operation Type | Count | Mean Latency (ms) | Min (ms) | Max (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|---|---|---|
| GET | 88 | 53.53 | 25.00 | 116.00 | 74.00 | 116.00 |
| PUT | 60 | 31.28 | 22.00 | 41.00 | 40.00 | 41.00 |

### Node Count = 2

| Operation Type | Count | Mean Latency (ms) | Min (ms) | Max (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|---|---|---|
| GET | 88 | 34.42 | 12.00 | 147.00 | 108.00 | 147.00 |
| PUT | 60 | 32.10 | 24.00 | 43.00 | 42.00 | 43.00 |

### Node Count = 3

| Operation Type | Count | Mean Latency (ms) | Min (ms) | Max (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|---|---|---|
| GET | 249 | 22.82 | 2.00 | 143.00 | 47.00 | 121.00 |

### Node Count = 4

| Operation Type | Count | Mean Latency (ms) | Min (ms) | Max (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|---|---|---|
| GET | 80 | 10.24 | 3.00 | 17.00 | 17.00 | 17.00 |
| PUT | 60 | 38.47 | 29.00 | 47.00 | 47.00 | 47.00 |


## Latency Statistics by Key Count

| Key Count | Operations | Mean Latency (ms) | Min (ms) | Max (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|---|---|---|
| 1000 | 140 | 20.21 | 2.00 | 46.00 | 42.00 | 44.00 |
| 5000 | 405 | 32.25 | 3.00 | 147.00 | 74.00 | 116.00 |
| 10000 | 140 | 31.84 | 15.00 | 49.00 | 46.00 | 48.00 |
