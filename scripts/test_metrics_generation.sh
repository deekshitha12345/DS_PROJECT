#!/bin/bash

################################################################################
# Quick Metrics Test Generator
# 
# Generates sample metrics logs for testing the metrics_processor.py script
# without needing to run the full Docker setup
#
# SCENARIO A: Key Count Variation (with fixed 3 nodes)
#   - 1K keys: higher latency (more data in single node region)
#   - 5K keys: moderate latency (distributed across nodes)
#   - 10K keys: lower latency (well-distributed)
#
# SCENARIO B: Node Scaling (with fixed 5K keys)
#   - 1 node: high latency (all traffic to one node)
#   - 2 nodes: moderate latency (traffic distributed)
#   - 4 nodes: low latency (traffic well-distributed)
#
# Usage:
#   ./scripts/test_metrics_generation.sh [--output-dir DIRECTORY]
#
################################################################################

OUTPUT_DIR="${1:-artifacts/metrics}"
mkdir -p "$OUTPUT_DIR"

LOG_FILE="${OUTPUT_DIR}/metrics_raw.log"

# Generate sample metrics log
cat > "$LOG_FILE" << 'EOF'
# Raw Metrics Log
# Format: timestamp|operation_type|key|elapsed_ms|http_code|status|node_count|key_count
# 
# SCENARIO A: Key Count Variation (Fixed 3 nodes)
# Shows how latency increases with larger datasets in fixed cluster
#
# SCENARIO B: Node Scaling (Fixed 5K keys)  
# Shows how latency decreases when scaling from 1 to 4 nodes

EOF

current_time=$(date +%s)
counter=0

# ============================================================================
# SCENARIO A: Key Count Variation (Fixed 3 nodes)
# ============================================================================

echo "# SCENARIO A: Key Count Variation (Fixed 3 nodes)" >> "$LOG_FILE"

# A1: GET operations with 1000 keys, 3 nodes
# With fewer keys, each key is accessed more frequently
# Expected: Lower latency due to better cache locality
echo "# A1: GET 1000 keys, 3 nodes - Better locality, lower latency" >> "$LOG_FILE"
for i in {1..80}; do
    # 1K keys on 3 nodes = better cache locality, faster retrieval
    elapsed=$((RANDOM % 15 + 2))  # 2-17ms (fast)
    http_code=200
    status="PASS"
    timestamp=$((current_time + counter))
    ((counter++))
    key_num=$((RANDOM % 1000 + 1))
    key="keyset_a1:$(printf '%05d' $key_num)"
    echo "${timestamp}|GET|${key}|${elapsed}|${http_code}|${status}|3|1000" >> "$LOG_FILE"
done

# A2: GET operations with 5000 keys, 3 nodes
# With more keys, accessing becomes slightly slower
# Expected: Moderate latency
echo "# A2: GET 5000 keys, 3 nodes - Balanced, moderate latency" >> "$LOG_FILE"
for i in {1..80}; do
    # 5K keys on 3 nodes = moderate latency
    elapsed=$((RANDOM % 25 + 8))  # 8-33ms (moderate)
    http_code=200
    status="PASS"
    timestamp=$((current_time + counter))
    ((counter++))
    key_num=$((RANDOM % 5000 + 1))
    key="keyset_a2:$(printf '%05d' $key_num)"
    echo "${timestamp}|GET|${key}|${elapsed}|${http_code}|${status}|3|5000" >> "$LOG_FILE"
done

# A3: GET operations with 10000 keys, 3 nodes
# With many keys, more contention and slower access
# Expected: Higher latency due to larger dataset
echo "# A3: GET 10000 keys, 3 nodes - Large dataset, higher latency" >> "$LOG_FILE"
for i in {1..80}; do
    # 10K keys on 3 nodes = higher latency due to size
    elapsed=$((RANDOM % 35 + 15))  # 15-50ms (slower)
    http_code=200
    status="PASS"
    timestamp=$((current_time + counter))
    ((counter++))
    key_num=$((RANDOM % 10000 + 1))
    key="keyset_a3:$(printf '%05d' $key_num)"
    echo "${timestamp}|GET|${key}|${elapsed}|${http_code}|${status}|3|10000" >> "$LOG_FILE"
done

# ============================================================================
# SCENARIO B: Node Scaling (Fixed 5K keys)
# ============================================================================

echo "# SCENARIO B: Node Scaling (Fixed 5K keys)" >> "$LOG_FILE"

# B1: GET operations with 5000 keys, 1 node
# Single node must handle all requests
# Expected: High latency due to bottleneck
echo "# B1: GET 5000 keys, 1 node - Single node bottleneck, high latency" >> "$LOG_FILE"
for i in {1..80}; do
    # All traffic on 1 node = contention, high latency
    elapsed=$((RANDOM % 50 + 25))  # 25-75ms (slow)
    http_code=200
    status="PASS"
    timestamp=$((current_time + counter))
    ((counter++))
    key_num=$((RANDOM % 5000 + 1))
    key="keyset_b1:$(printf '%05d' $key_num)"
    echo "${timestamp}|GET|${key}|${elapsed}|${http_code}|${status}|1|5000" >> "$LOG_FILE"
done

# B2: GET operations with 5000 keys, 2 nodes
# Traffic distributed across 2 nodes
# Expected: Moderate latency, some improvement
echo "# B2: GET 5000 keys, 2 nodes - Better distribution, moderate latency" >> "$LOG_FILE"
for i in {1..80}; do
    # Traffic on 2 nodes = better throughput
    elapsed=$((RANDOM % 30 + 12))  # 12-42ms (moderate)
    http_code=200
    status="PASS"
    timestamp=$((current_time + counter))
    ((counter++))
    key_num=$((RANDOM % 5000 + 1))
    key="keyset_b2:$(printf '%05d' $key_num)"
    echo "${timestamp}|GET|${key}|${elapsed}|${http_code}|${status}|2|5000" >> "$LOG_FILE"
done

# B3: GET operations with 5000 keys, 4 nodes
# Traffic well-distributed across 4 nodes
# Expected: Low latency due to high parallelism
echo "# B3: GET 5000 keys, 4 nodes - Well-distributed, low latency" >> "$LOG_FILE"
for i in {1..80}; do
    # Traffic on 4 nodes = low contention
    elapsed=$((RANDOM % 15 + 3))  # 3-18ms (fast)
    http_code=200
    status="PASS"
    timestamp=$((current_time + counter))
    ((counter++))
    key_num=$((RANDOM % 5000 + 1))
    key="keyset_b3:$(printf '%05d' $key_num)"
    echo "${timestamp}|GET|${key}|${elapsed}|${http_code}|${status}|4|5000" >> "$LOG_FILE"
done

# Add PUT operations for complete picture
echo "# PUT operations for all scenarios" >> "$LOG_FILE"
for node_count in 1 2 4; do
    for key_count in 1000 5000 10000; do
        # Adjust PUT latency based on node count (PUT involves replication)
        min_latency=$((20 + node_count * 2))
        max_latency=$((min_latency + 20))
        
        for i in {1..20}; do
            elapsed=$((RANDOM % (max_latency - min_latency) + min_latency))
            http_code=200
            status="PASS"
            timestamp=$((current_time + counter))
            ((counter++))
            key_num=$((RANDOM % key_count + 1))
            key="put:n${node_count}k${key_count}:$(printf '%05d' $key_num)"
            echo "${timestamp}|PUT|${key}|${elapsed}|${http_code}|${status}|${node_count}|${key_count}" >> "$LOG_FILE"
        done
    done
done

# Add some failed operations for realism
echo "# Failed operations (5% error rate)" >> "$LOG_FILE"
for i in {1..25}; do
    elapsed=$((RANDOM % 100 + 50))
    http_code=$((RANDOM % 2 == 0 ? 404 : 500))
    status="FAIL"
    timestamp=$((current_time + counter))
    ((counter++))
    key="notfound:$(printf '%05d' $i)"
    node_count=$((RANDOM % 3 + 1))
    echo "${timestamp}|GET|${key}|${elapsed}|${http_code}|${status}|${node_count}|5000" >> "$LOG_FILE"
done

echo "✓ Sample metrics log generated: $LOG_FILE"
echo "✓ $(wc -l < "$LOG_FILE") total lines (including comments)"

# Now process the metrics
echo ""
echo "Processing metrics..."
python3 scripts/metrics_processor.py --log "$LOG_FILE" --format csv,json,md

echo ""
echo "Generated report files:"
ls -lh "${OUTPUT_DIR}/metrics_results."*
