# Metrics Benchmark Guide

This guide explains how to use the metrics collection and reporting scripts for the Distributed Cache system.

## Overview

The metrics system consists of three main components:

1. **`metrics_benchmark.sh`** — Main benchmark script that collects metrics via the HTTP API
2. **`metrics_processor.py`** — Processes raw metrics logs and generates formatted reports (CSV, JSON, Markdown)
3. **`test_metrics_generation.sh`** — Test utility for generating sample metrics data

## Quick Start

### Option 1: Run Full Benchmark (with Docker)

```bash
# Make script executable
chmod +x scripts/metrics_benchmark.sh

# Run full benchmark (all nodes, all key counts, all formats)
./scripts/metrics_benchmark.sh

# Or with specific configuration
./scripts/metrics_benchmark.sh --nodes 3 --keys 5000 --format csv,md
```

The script will:
1. Start Docker Compose with the cache nodes
2. Run test scenarios for key count variation and node scaling
3. Generate results in `artifacts/metrics/` directory
4. Automatically stop Docker when complete

### Option 2: Test with Sample Data (Quick Validation)

```bash
# Generate sample metrics and test report generation
chmod +x scripts/test_metrics_generation.sh
./scripts/test_metrics_generation.sh

# Or manually process existing metrics
python3 scripts/metrics_processor.py --log artifacts/metrics/metrics_raw.log
```

## Metrics Collected

### Operation Metrics
- **Latency**: Response time in milliseconds (per operation)
- **Throughput**: Operations per second
- **Success Rate**: Percentage of successful operations
- **HTTP Status Codes**: 200 (success), 404 (not found), etc.

### Aggregated Statistics
- **Mean**: Average latency across all operations
- **Median (P50)**: Middle value (50th percentile)
- **P95**: 95th percentile (tail latency)
- **P99**: 99th percentile (worst-case latency)
- **Min/Max**: Minimum and maximum values

### Test Dimensions
- **Operation Type**: GET, PUT, DELETE
- **Node Count**: Number of cache nodes (1-4)
- **Key Count**: Number of keys in cache (1K, 5K, 10K)

## Detailed Usage

### Script: `metrics_benchmark.sh`

#### Full Options

```bash
./scripts/metrics_benchmark.sh [OPTIONS]

OPTIONS:
  --help                  Show help message
  --nodes NUM            Test with specific number of nodes (1-4)
  --keys NUM             Test with specific number of keys
  --output-dir DIR       Custom output directory (default: artifacts/metrics)
  --format FORMAT        Output format: csv, json, md, or all
  --scale-only           Only run node scaling tests
  --keys-only            Only run key count variation tests
  --dry-run              Show test plan without running
```

#### Examples

```bash
# Test plan without executing
./scripts/metrics_benchmark.sh --dry-run

# Test only with 2 nodes, 5000 keys
./scripts/metrics_benchmark.sh --nodes 2 --keys 5000

# Output only CSV (faster for large datasets)
./scripts/metrics_benchmark.sh --format csv

# Custom output directory
./scripts/metrics_benchmark.sh --output-dir /tmp/metrics

# Node scaling only (skip key count variation)
./scripts/metrics_benchmark.sh --scale-only

# Key count variation only (fixed 3 nodes)
./scripts/metrics_benchmark.sh --keys-only
```

#### What It Tests

**Scenario A: Key Count Variation** (Fixed 3 nodes)
- Test with 1K, 5K, 10K keys
- Measure PUT, GET, DELETE operations
- Observe impact of dataset size on latency/throughput

**Scenario B: Node Scaling** (Fixed 5K keys)
- Start with 1 node, warm up data
- Scale to 2, 3, 4 nodes
- Measure GET performance at each scale
- Observe throughput improvement with more nodes

### Script: `metrics_processor.py`

#### Processing Raw Metrics

```bash
python3 scripts/metrics_processor.py [OPTIONS]

OPTIONS:
  --log LOG                 Path to raw metrics log file
  --output-dir DIR          Output directory for reports
  --format FORMAT           Output formats (csv,json,md)
```

#### Examples

```bash
# Process with defaults
python3 scripts/metrics_processor.py

# Custom log and output location
python3 scripts/metrics_processor.py \
  --log /tmp/my_metrics.log \
  --output-dir reports/

# Generate only JSON
python3 scripts/metrics_processor.py --format json

# Generate CSV and Markdown only
python3 scripts/metrics_processor.py --format csv,md
```

### Script: `test_metrics_generation.sh`

Generate sample data for testing without running the full benchmark:

```bash
chmod +x scripts/test_metrics_generation.sh
./scripts/test_metrics_generation.sh

# Specify custom output directory
./scripts/test_metrics_generation.sh artifacts/test_metrics
```

This generates:
- 100 PUT operations (1000 keys, 1 node)
- 100 GET operations (1000 keys, 1 node)
- 100 PUT operations (5000 keys, 2 nodes)
- 100 GET operations (5000 keys, 2 nodes)
- 100 GET operations (10000 keys, 4 nodes)
- 10 failed operations

## Output Files

The metrics system generates three report formats:

### 1. CSV Format (`metrics_results.csv`)

**Use for**: Spreadsheet analysis (Excel, Google Sheets, etc.)

```csv
timestamp,operation_type,key,elapsed_ms,http_code,status,node_count,key_count
1620000100,PUT,key:000001,25,200,PASS,1,1000
1620000101,PUT,key:000002,28,200,PASS,1,1000
1620000102,GET,key:000001,15,200,PASS,1,1000
```

**Columns**:
- `timestamp`: Unix timestamp of operation
- `operation_type`: GET, PUT, or DELETE
- `key`: Cache key
- `elapsed_ms`: Response time in milliseconds
- `http_code`: HTTP status code (200, 404, etc.)
- `status`: PASS or FAIL
- `node_count`: Number of nodes at test time
- `key_count`: Total keys in cache at test time

### 2. JSON Format (`metrics_results.json`)

**Use for**: Programmatic analysis, integration with other tools

```json
{
  "metadata": {
    "generated_at": "2026-05-05T00:16:20",
    "total_operations": 510,
    "log_file": "artifacts/metrics/metrics_raw.log"
  },
  "summary": {
    "total_operations": 510,
    "success_rate": 98.0,
    "operations_by_type": {
      "GET": {
        "count": 310,
        "mean": 20.19,
        "p95": 37.0,
        "p99": 61.0
      },
      ...
    },
    "by_node_count": { ... },
    "by_key_count": { ... }
  },
  "operations": [ ... ]
}
```

### 3. Markdown Format (`metrics_results.md`)

**Use for**: Human-readable reports, documentation

Includes:
- Summary statistics
- Operations breakdown table
- Latency stats by node count
- Latency stats by key count
- Detailed operations log (if < 500 operations)

### 4. Raw Log Format (`metrics_raw.log`)

**Use for**: Detailed debugging, custom analysis

```
# Raw Metrics Log
# Format: timestamp|operation_type|key|elapsed_ms|http_code|status|node_count|key_count

1620000100|PUT|key:000001|25|200|PASS|1|1000
1620000101|PUT|key:000002|28|200|PASS|1|1000
```

## Interpreting Results

### Latency Analysis

**What to look for**:
- **Mean vs P99**: If P99 >> Mean, there are outliers (possibly GC pauses, network delays)
- **Trend with key count**: Ideally flat or slightly increasing
- **Trend with node count**: Should decrease (more nodes = better distribution)

**Example interpretation**:
```
Key Count: 1000, 1 Node   → Mean: 26ms, P99: 54ms
Key Count: 5000, 2 Nodes  → Mean: 30ms, P99: 62ms
Key Count: 10000, 4 Nodes → Mean: 16ms, P99: 30ms

Insight: Latency improves significantly when scaling from 2 to 4 nodes
despite larger key count, indicating good horizontal scalability.
```

### Throughput Analysis

**Operations per second** = Total operations / Total time

Higher throughput with same latency = better efficiency

### Success Rate

- **100%**: Perfect reliability
- **95%+**: Good (check why failures occur)
- **<95%**: Investigate network issues or overload

## Performance Benchmarks

Expected baseline metrics (from test data):

| Metric | 1 Node | 2 Nodes | 4 Nodes |
|--------|--------|---------|---------|
| GET mean latency | ~26ms | ~20ms | ~16ms |
| PUT mean latency | ~35ms | ~35ms | ~30ms |
| GET P99 latency | ~54ms | ~37ms | ~30ms |
| Success rate | 100% | 99%+ | 99%+ |

**Note**: Actual values depend on network, hardware, and Docker overhead

## Troubleshooting

### Docker Fails to Start

```bash
# Check Docker is running
docker ps

# Check compose version
docker-compose --version

# Try manual start
docker-compose -f docker-compose.yml up
```

### Gateway Health Check Fails

```bash
# Check if gateway is listening
curl -v http://localhost:8000/health

# Check Docker logs
docker-compose logs gateway
```

### Metrics Files Not Generated

```bash
# Check output directory exists
ls -la artifacts/metrics/

# Check raw log exists
head artifacts/metrics/metrics_raw.log

# Manually regenerate reports
python3 scripts/metrics_processor.py --log artifacts/metrics/metrics_raw.log
```

### Strange Latency Values

- **Very high**: Network congestion, Docker resource limits
- **Very low**: May indicate caching or timing precision issues
- **Inconsistent**: Indicates variable system load

## Advanced Usage

### Custom Analysis with Python

```python
import json

# Load generated JSON
with open('artifacts/metrics/metrics_results.json') as f:
    data = json.load(f)

# Access summary
print(f"Success rate: {data['summary']['success_rate']:.1f}%")

# Find slowest operations
ops = sorted(data['operations'], key=lambda x: x['elapsed_ms'], reverse=True)
print(f"Slowest operation: {ops[0]['elapsed_ms']}ms")

# Filter by operation type
gets = [op for op in data['operations'] if op['operation_type'] == 'GET']
get_latencies = [op['elapsed_ms'] for op in gets]
print(f"GET latency average: {sum(get_latencies) / len(get_latencies):.1f}ms")
```

### Batch Processing Multiple Runs

```bash
#!/bin/bash
# Compare metrics across different configurations

for nodes in 1 2 3 4; do
    echo "Testing with $nodes nodes..."
    ./scripts/metrics_benchmark.sh --nodes $nodes --keys 5000
    
    # Save results
    cp artifacts/metrics/metrics_results.json "results_${nodes}nodes.json"
    cp artifacts/metrics/metrics_results.md "results_${nodes}nodes.md"
done

# Now compare the results
```

### Plotting Results

```bash
# Convert CSV to plot-friendly format
awk -F, '{print $2, $4}' artifacts/metrics/metrics_results.csv | \
  sort | uniq | gnuplot
```

## Performance Tuning Tips

Based on metrics results:

1. **High latency with few nodes**: Add more nodes to distribute load
2. **High latency with many nodes**: May indicate network issues or resource contention
3. **High P99 vs P50**: May indicate occasional spikes (investigate GC, system load)
4. **Decreasing throughput**: May indicate memory pressure or eviction
5. **Failed operations**: Check node health and network connectivity

## Next Steps

1. **Baseline**: Run `test_metrics_generation.sh` to validate setup
2. **Local Test**: Use `--dry-run` to see test plan
3. **Small Scale**: Run with `--nodes 1 --keys 1000` to test quickly
4. **Full Suite**: Run `./scripts/metrics_benchmark.sh` for complete metrics
5. **Analyze**: Use Python or spreadsheet tools to compare results across runs

## Support

For issues or questions:
1. Check `artifacts/metrics/metrics_raw.log` for detailed operation logs
2. Review Docker logs: `docker-compose logs`
3. Verify network connectivity to localhost:8000
4. Check available disk space in `artifacts/metrics/`
