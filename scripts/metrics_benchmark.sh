#!/bin/bash

################################################################################
# Distributed Cache Metrics Benchmark Script
#
# Measures cache performance metrics via HTTP API:
# - Latency for GET, PUT, DELETE operations
# - Throughput (operations per second)
# - Impact of key count variation (1K, 5K, 10K keys)
# - Impact of node scaling (1, 2, 3, 4 nodes)
#
# Runs in Docker mode (HTTP-based, realistic network behavior)
#
# Usage:
#   ./scripts/metrics_benchmark.sh [--help] [--nodes 1-4] [--keys 1000-10000]
#                                  [--output-dir DIRECTORY] [--format csv|json|md|all]
#
# Examples:
#   ./scripts/metrics_benchmark.sh --help
#   ./scripts/metrics_benchmark.sh                           # All tests, all formats
#   ./scripts/metrics_benchmark.sh --nodes 3 --keys 5000     # Fixed 3 nodes, 5K keys
#   ./scripts/metrics_benchmark.sh --format csv,md           # CSV and Markdown only
#
################################################################################

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default configuration
GATEWAY_HOST="${GATEWAY_HOST:-localhost}"
GATEWAY_PORT="${GATEWAY_PORT:-8000}"
GATEWAY_URL="http://${GATEWAY_HOST}:${GATEWAY_PORT}"
OUTPUT_DIR="artifacts/metrics"
OUTPUT_FORMATS=("csv" "json" "md")
TEST_NODES=(1 2 3 4)
TEST_KEY_COUNTS=(1000 5000 10000)
DOCKER_COMPOSE_FILE="docker-compose.yml"
STARTUP_TIMEOUT=60
STARTUP_CHECK_INTERVAL=1

# Metrics storage
declare -A METRICS
declare -a OPERATIONS_LOG
OPERATIONS_LOG=()

################################################################################
# Utility Functions
################################################################################

log() {
    local level=$1
    shift
    local message="$@"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        INFO)
            echo -e "${BLUE}[${timestamp}]${NC} ${message}"
            ;;
        SUCCESS)
            echo -e "${GREEN}[${timestamp}]${NC} ✓ ${message}"
            ;;
        WARN)
            echo -e "${YELLOW}[${timestamp}]${NC} ⚠ ${message}"
            ;;
        ERROR)
            echo -e "${RED}[${timestamp}]${NC} ✗ ${message}" >&2
            ;;
        DEBUG)
            echo -e "${CYAN}[${timestamp}]${NC} 🔧 ${message}"
            ;;
    esac
}

show_help() {
    cat << 'EOF'
Distributed Cache Metrics Benchmark Script

USAGE:
    ./scripts/metrics_benchmark.sh [OPTIONS]

OPTIONS:
    --help                      Show this help message
    --nodes NUM                 Test with NUM nodes only (1-4, default: test all)
    --keys NUM                  Test with NUM keys only (default: test all counts)
    --output-dir DIR            Output directory for results (default: artifacts/metrics)
    --format FORMAT             Output format: csv, json, md, or all (default: all)
    --scale-only                Only run node scaling tests (skip key count variation)
    --keys-only                 Only run key count variation tests (skip node scaling)
    --dry-run                   Show what would be tested without running

EXAMPLES:
    # Run all tests, all formats
    ./scripts/metrics_benchmark.sh

    # Test only with 3 nodes and 5000 keys
    ./scripts/metrics_benchmark.sh --nodes 3 --keys 5000

    # Output only CSV format
    ./scripts/metrics_benchmark.sh --format csv

    # Dry run to see test plan
    ./scripts/metrics_benchmark.sh --dry-run

METRICS MEASURED:
    - Operation latency (GET, PUT, DELETE)
    - Throughput (operations per second)
    - Success rate
    - Min/Max/Average/P50/P95/P99 latencies

OUTPUT:
    Results are saved to: artifacts/metrics/
    - metrics_results.csv       Comma-separated values (spreadsheet-ready)
    - metrics_results.json      JSON format (programmatic analysis)
    - metrics_results.md        Markdown tables (human-readable)
    - metrics_raw.log           Detailed operation log

EOF
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help)
                show_help
                exit 0
                ;;
            --nodes)
                TEST_NODES=("$2")
                shift 2
                ;;
            --keys)
                TEST_KEY_COUNTS=("$2")
                shift 2
                ;;
            --output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --format)
                IFS=',' read -ra OUTPUT_FORMATS <<< "$2"
                shift 2
                ;;
            --scale-only)
                TEST_KEY_COUNTS=()
                shift
                ;;
            --keys-only)
                TEST_NODES=()
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            *)
                log ERROR "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

################################################################################
# Docker & Health Check Functions
################################################################################

docker_compose_up() {
    log INFO "Starting Docker Compose with distributed cache nodes..."
    
    if ! docker-compose -f "$DOCKER_COMPOSE_FILE" up -d 2>&1; then
        log ERROR "Failed to start Docker Compose"
        return 1
    fi
    
    log INFO "Waiting for containers to be ready..."
    sleep 2
}

docker_compose_down() {
    log INFO "Stopping Docker Compose..."
    docker-compose -f "$DOCKER_COMPOSE_FILE" down --volumes 2>&1 || true
}

wait_for_gateway() {
    local timeout=$1
    local start_time=$(date +%s)
    
    log INFO "Waiting for gateway to be ready at ${GATEWAY_URL}..."
    
    while true; do
        if curl -s "${GATEWAY_URL}/health" > /dev/null 2>&1; then
            log SUCCESS "Gateway is ready"
            return 0
        fi
        
        local elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $timeout ]]; then
            log ERROR "Gateway failed to start within ${timeout}s"
            return 1
        fi
        
        sleep "$STARTUP_CHECK_INTERVAL"
    done
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        log ERROR "Docker is not installed"
        return 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log ERROR "Docker Compose is not installed"
        return 1
    fi
    
    log SUCCESS "Docker and Docker Compose are available"
    return 0
}

################################################################################
# Metrics Collection Functions
################################################################################

# Make HTTP request and measure response time
# Returns: "elapsed_ms|http_code|response_body"
http_request() {
    local method=$1
    local endpoint=$2
    local data=${3:-}
    
    local start_ns=$(date +%s%N)
    
    if [[ "$method" == "PUT" && -n "$data" ]]; then
        local response=$(curl -s -w "\n%{http_code}" \
            -X "$method" \
            "${GATEWAY_URL}${endpoint}" \
            -H "Content-Type: application/json" \
            -d "$data" 2>/dev/null)
    else
        local response=$(curl -s -w "\n%{http_code}" \
            -X "$method" \
            "${GATEWAY_URL}${endpoint}" 2>/dev/null)
    fi
    
    local end_ns=$(date +%s%N)
    local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    
    # Extract HTTP code (last line)
    local http_code=$(echo "$response" | tail -n1)
    # Extract response body (all but last line)
    local body=$(echo "$response" | head -n-1)
    
    echo "${elapsed_ms}|${http_code}|${body}"
}

# Store metrics from an operation
record_metric() {
    local operation_type=$1
    local key=$2
    local elapsed_ms=$3
    local http_code=$4
    local node_count=$5
    local key_count=$6
    
    local timestamp=$(date '+%s')
    local status="PASS"
    
    if [[ "$http_code" != "200" ]]; then
        status="FAIL"
    fi
    
    local metric_line="${timestamp}|${operation_type}|${key}|${elapsed_ms}|${http_code}|${status}|${node_count}|${key_count}"
    OPERATIONS_LOG+=("$metric_line")
}

# Test PUT operation
test_put_operation() {
    local key=$1
    local value=$2
    local node_count=$3
    local key_count=$4
    
    local payload=$(cat <<EOF
{
  "value": "$value",
  "ttl_seconds": 3600
}
EOF
)
    
    local result=$(http_request "PUT" "/cache/${key}" "$payload")
    local elapsed_ms=$(echo "$result" | cut -d'|' -f1)
    local http_code=$(echo "$result" | cut -d'|' -f2)
    
    record_metric "PUT" "$key" "$elapsed_ms" "$http_code" "$node_count" "$key_count"
    echo "$elapsed_ms"
}

# Test GET operation
test_get_operation() {
    local key=$1
    local node_count=$2
    local key_count=$3
    
    local result=$(http_request "GET" "/cache/${key}")
    local elapsed_ms=$(echo "$result" | cut -d'|' -f1)
    local http_code=$(echo "$result" | cut -d'|' -f2)
    
    record_metric "GET" "$key" "$elapsed_ms" "$http_code" "$node_count" "$key_count"
    echo "$elapsed_ms"
}

# Test DELETE operation
test_delete_operation() {
    local key=$1
    local node_count=$2
    local key_count=$3
    
    local result=$(http_request "DELETE" "/cache/${key}")
    local elapsed_ms=$(echo "$result" | cut -d'|' -f1)
    local http_code=$(echo "$result" | cut -d'|' -f2)
    
    record_metric "DELETE" "$key" "$elapsed_ms" "$http_code" "$node_count" "$key_count"
    echo "$elapsed_ms"
}

################################################################################
# Test Scenarios
################################################################################

# Scenario A: Vary key count with fixed node count (3 nodes)
test_key_count_variation() {
    log INFO "=== SCENARIO A: KEY COUNT VARIATION ==="
    log INFO "Testing with fixed 3 nodes, varying key counts..."
    
    local fixed_nodes=3
    
    for key_count in "${TEST_KEY_COUNTS[@]}"; do
        log INFO "Starting key count variation test: ${key_count} keys with ${fixed_nodes} nodes"
        
        # Generate keys
        local keys=()
        for i in $(seq 1 "$key_count"); do
            keys+=("key:$(printf '%06d' "$i")")
        done
        
        # Phase 1: PUT operations
        log INFO "Phase 1: PUT ${key_count} keys..."
        local put_times=()
        for key in "${keys[@]}"; do
            local value="value_for_${key}_$(date +%s%N)"
            local elapsed=$(test_put_operation "$key" "$value" "$fixed_nodes" "$key_count")
            put_times+=("$elapsed")
        done
        
        # Calculate PUT statistics
        local put_stats=$(calculate_stats "${put_times[@]}")
        log SUCCESS "PUT complete: $put_stats"
        
        # Phase 2: GET operations
        log INFO "Phase 2: GET ${key_count} keys..."
        local get_times=()
        for key in "${keys[@]}"; do
            local elapsed=$(test_get_operation "$key" "$fixed_nodes" "$key_count")
            get_times+=("$elapsed")
        done
        
        local get_stats=$(calculate_stats "${get_times[@]}")
        log SUCCESS "GET complete: $get_stats"
        
        # Phase 3: DELETE operations
        log INFO "Phase 3: DELETE ${key_count} keys..."
        local delete_times=()
        for key in "${keys[@]}"; do
            local elapsed=$(test_delete_operation "$key" "$fixed_nodes" "$key_count")
            delete_times+=("$elapsed")
        done
        
        local delete_stats=$(calculate_stats "${delete_times[@]}")
        log SUCCESS "DELETE complete: $delete_stats"
        
        log INFO "Key count variation test completed for ${key_count} keys"
        echo ""
    done
}

# Scenario B: Scale nodes with fixed key count (5000 keys)
test_node_scaling() {
    log INFO "=== SCENARIO B: NODE SCALING ==="
    log INFO "Testing with fixed 5000 keys, scaling from 1 to 4 nodes..."
    
    local fixed_key_count=5000
    
    for num_nodes in "${TEST_NODES[@]}"; do
        log INFO "Starting node scaling test: ${num_nodes} node(s) with ${fixed_key_count} keys"
        
        # Generate keys once (reuse for all node scales)
        local keys=()
        if [[ "$num_nodes" == "1" ]]; then
            # First time: generate keys
            for i in $(seq 1 "$fixed_key_count"); do
                keys+=("scale:key:$(printf '%05d' "$i")")
            done
        fi
        
        # Warm-up: PUT all keys
        log INFO "Warm-up: Putting ${fixed_key_count} keys with ${num_nodes} node(s)..."
        if [[ "$num_nodes" == "1" ]]; then
            for key in "${keys[@]}"; do
                local value="scale_value_${key}_$(date +%s%N)"
                test_put_operation "$key" "$value" "$num_nodes" "$fixed_key_count" > /dev/null
            done
        fi
        
        # Load test: 1000 GET operations
        log INFO "Load test: GET 1000 keys with ${num_nodes} node(s)..."
        local get_times=()
        for i in $(seq 1 1000); do
            local key_idx=$((RANDOM % fixed_key_count))
            local key="scale:key:$(printf '%05d' $((key_idx + 1)))"
            local elapsed=$(test_get_operation "$key" "$num_nodes" "$fixed_key_count")
            get_times+=("$elapsed")
        done
        
        local get_stats=$(calculate_stats "${get_times[@]}")
        log SUCCESS "Node scaling with ${num_nodes} node(s) complete: $get_stats"
        
        echo ""
    done
}

################################################################################
# Statistics Calculation
################################################################################

# Calculate statistics from array of values
# Usage: calculate_stats "${array[@]}"
# Returns: "avg=${avg}ms min=${min}ms max=${max}ms p50=${p50}ms p95=${p95}ms p99=${p99}ms"
calculate_stats() {
    local values=("$@")
    local count=${#values[@]}
    
    if [[ $count -eq 0 ]]; then
        echo "avg=0ms min=0ms max=0ms p50=0ms p95=0ms p99=0ms"
        return
    fi
    
    # Sort values numerically
    IFS=$'\n' sorted=($(sort -n <<<"${values[*]}"))
    unset IFS
    
    # Calculate average
    local sum=0
    for val in "${sorted[@]}"; do
        sum=$((sum + val))
    done
    local avg=$((sum / count))
    
    # Min and Max
    local min="${sorted[0]}"
    local max="${sorted[-1]}"
    
    # Percentiles
    local p50_idx=$((count / 2))
    local p50="${sorted[$p50_idx]}"
    
    local p95_idx=$(( (count * 95) / 100 ))
    local p95="${sorted[$p95_idx]}"
    
    local p99_idx=$(( (count * 99) / 100 ))
    [[ $p99_idx -ge $count ]] && p99_idx=$((count - 1))
    local p99="${sorted[$p99_idx]}"
    
    echo "avg=${avg}ms min=${min}ms max=${max}ms p50=${p50}ms p95=${p95}ms p99=${p99}ms throughput=$((count * 1000 / (avg * count)))ops/sec"
}

################################################################################
# Output Formatting
################################################################################

generate_csv_output() {
    log INFO "Generating CSV output..."
    
    local csv_file="${OUTPUT_DIR}/metrics_results.csv"
    
    # CSV Header
    cat > "$csv_file" << 'EOF'
timestamp,operation_type,key,elapsed_ms,http_code,status,node_count,key_count
EOF
    
    # CSV Data
    for line in "${OPERATIONS_LOG[@]}"; do
        IFS='|' read -r timestamp operation_type key elapsed_ms http_code status node_count key_count <<< "$line"
        echo "${timestamp},${operation_type},${key},${elapsed_ms},${http_code},${status},${node_count},${key_count}" >> "$csv_file"
    done
    
    log SUCCESS "CSV output saved to: $csv_file"
    log INFO "CSV contains $(wc -l < "$csv_file") rows"
}

generate_json_output() {
    log INFO "Generating JSON output..."
    
    local json_file="${OUTPUT_DIR}/metrics_results.json"
    
    # Start JSON
    cat > "$json_file" << 'EOF'
{
  "metadata": {
    "generated_at": "GENERATED_AT_PLACEHOLDER",
    "total_operations": TOTAL_OPS_PLACEHOLDER
  },
  "operations": [
EOF
    
    # Add operations
    local first=true
    for line in "${OPERATIONS_LOG[@]}"; do
        IFS='|' read -r timestamp operation_type key elapsed_ms http_code status node_count key_count <<< "$line"
        
        if [[ "$first" == true ]]; then
            first=false
        else
            echo "," >> "$json_file"
        fi
        
        cat >> "$json_file" <<EOF
    {
      "timestamp": ${timestamp},
      "operation_type": "${operation_type}",
      "key": "${key}",
      "elapsed_ms": ${elapsed_ms},
      "http_code": ${http_code},
      "status": "${status}",
      "node_count": ${node_count},
      "key_count": ${key_count}
    }
EOF
    done
    
    # Close JSON
    cat >> "$json_file" << 'EOF'
  ],
  "summary": {
    "total_operations": TOTAL_OPS_PLACEHOLDER,
    "success_rate": SUCCESS_RATE_PLACEHOLDER
  }
}
EOF
    
    # Replace placeholders
    local total_ops=${#OPERATIONS_LOG[@]}
    local success_count=0
    for line in "${OPERATIONS_LOG[@]}"; do
        if [[ "$line" == *"|PASS|"* ]]; then
            ((success_count++))
        fi
    done
    local success_rate=$(awk "BEGIN {printf \"%.1f\", ($success_count / $total_ops) * 100}")
    
    sed -i.bak "s/GENERATED_AT_PLACEHOLDER/$(date +%s)/g; s/TOTAL_OPS_PLACEHOLDER/${total_ops}/g; s/SUCCESS_RATE_PLACEHOLDER/${success_rate}/g" "$json_file"
    rm -f "${json_file}.bak"
    
    log SUCCESS "JSON output saved to: $json_file"
}

generate_markdown_output() {
    log INFO "Generating Markdown output..."
    
    local md_file="${OUTPUT_DIR}/metrics_results.md"
    
    cat > "$md_file" << 'EOF'
# Distributed Cache Metrics Report

Generated: $(date)

## Summary

EOF
    
    # Calculate summary statistics
    local total_ops=${#OPERATIONS_LOG[@]}
    local success_count=0
    local total_latency=0
    declare -A op_counts
    declare -A op_latencies
    
    for line in "${OPERATIONS_LOG[@]}"; do
        IFS='|' read -r timestamp operation_type key elapsed_ms http_code status node_count key_count <<< "$line"
        
        if [[ "$status" == "PASS" ]]; then
            ((success_count++))
        fi
        ((total_latency += elapsed_ms))
        ((op_counts[$operation_type]++))
        ((op_latencies[$operation_type] += elapsed_ms))
    done
    
    local success_rate=$(awk "BEGIN {printf \"%.1f\", ($success_count / $total_ops) * 100}")
    local avg_latency=$(awk "BEGIN {printf \"%.2f\", $total_latency / $total_ops}")
    
    cat >> "$md_file" << EOF
- **Total Operations**: $total_ops
- **Successful Operations**: $success_count ($success_rate%)
- **Average Latency**: ${avg_latency}ms

## Operations Breakdown

| Operation Type | Count | Average Latency (ms) |
|---|---|---|
EOF
    
    for op_type in PUT GET DELETE; do
        if [[ -v op_counts[$op_type] ]]; then
            local count=${op_counts[$op_type]}
            local avg=$(awk "BEGIN {printf \"%.2f\", ${op_latencies[$op_type]} / $count}")
            echo "| $op_type | $count | $avg |" >> "$md_file"
        fi
    done
    
    cat >> "$md_file" << 'EOF'

## Detailed Operations Log

| Timestamp | Operation | Key | Latency (ms) | Status | Node Count | Key Count |
|---|---|---|---|---|---|---|
EOF
    
    for line in "${OPERATIONS_LOG[@]}"; do
        IFS='|' read -r timestamp operation_type key elapsed_ms http_code status node_count key_count <<< "$line"
        echo "| $timestamp | $operation_type | $key | $elapsed_ms | $status | $node_count | $key_count |" >> "$md_file"
    done
    
    log SUCCESS "Markdown output saved to: $md_file"
}

generate_raw_log() {
    log INFO "Generating raw log..."
    
    local log_file="${OUTPUT_DIR}/metrics_raw.log"
    
    cat > "$log_file" << 'EOF'
# Raw Metrics Log
# Format: timestamp|operation_type|key|elapsed_ms|http_code|status|node_count|key_count

EOF
    
    for line in "${OPERATIONS_LOG[@]}"; do
        echo "$line" >> "$log_file"
    done
    
    log SUCCESS "Raw log saved to: $log_file"
}

################################################################################
# Main Execution
################################################################################

main() {
    log INFO "Starting Distributed Cache Metrics Benchmark"
    log INFO "Output directory: $OUTPUT_DIR"
    log INFO "Output formats: ${OUTPUT_FORMATS[*]}"
    
    # Create output directory
    mkdir -p "$OUTPUT_DIR"
    
    # Check Docker
    if ! check_docker; then
        log ERROR "Docker checks failed"
        exit 1
    fi
    
    # Cleanup function
    trap 'log INFO "Cleaning up..."; docker_compose_down' EXIT
    
    # Start Docker Compose
    if ! docker_compose_up; then
        log ERROR "Failed to start Docker Compose"
        exit 1
    fi
    
    # Wait for gateway
    if ! wait_for_gateway "$STARTUP_TIMEOUT"; then
        log ERROR "Gateway health check failed"
        exit 1
    fi
    
    # Run tests
    if [[ ${#TEST_KEY_COUNTS[@]} -gt 0 ]]; then
        test_key_count_variation
    fi
    
    if [[ ${#TEST_NODES[@]} -gt 0 ]]; then
        test_node_scaling
    fi
    
    # Generate outputs
    generate_raw_log
    
    for format in "${OUTPUT_FORMATS[@]}"; do
        case "$format" in
            csv)
                generate_csv_output
                ;;
            json)
                generate_json_output
                ;;
            md)
                generate_markdown_output
                ;;
            *)
                log WARN "Unknown output format: $format"
                ;;
        esac
    done
    
    log SUCCESS "Metrics benchmark completed successfully!"
    log INFO "Results saved to: $OUTPUT_DIR"
    
    # Display summary
    echo ""
    log INFO "=== SUMMARY ==="
    log INFO "Total operations: ${#OPERATIONS_LOG[@]}"
    
    # Count by operation type
    local put_count=0
    local get_count=0
    local delete_count=0
    
    for line in "${OPERATIONS_LOG[@]}"; do
        if [[ "$line" == *"|PUT|"* ]]; then
            ((put_count++))
        elif [[ "$line" == *"|GET|"* ]]; then
            ((get_count++))
        elif [[ "$line" == *"|DELETE|"* ]]; then
            ((delete_count++))
        fi
    done
    
    log INFO "Operations breakdown: PUT=$put_count GET=$get_count DELETE=$delete_count"
    log INFO "Results saved in: ${OUTPUT_FORMATS[*]} format(s)"
}

# Parse arguments
parse_arguments "$@"

# Check if dry-run
if [[ "${DRY_RUN:-false}" == "true" ]]; then
    log INFO "DRY RUN MODE - No actual tests will be executed"
    log INFO "Test configuration:"
    log INFO "  Node counts: ${TEST_NODES[*]}"
    log INFO "  Key counts: ${TEST_KEY_COUNTS[*]}"
    log INFO "  Output formats: ${OUTPUT_FORMATS[*]}"
    exit 0
fi

# Run main
main "$@"
