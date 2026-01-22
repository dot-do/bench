#!/bin/bash
# Run all OLTP and Container benchmarks
# Usage: ./scripts/run-all-benchmarks.sh [size]
# Default size: 100mb

SIZE=${1:-100mb}
OLTP_URL="https://bench-oltp.dotdo.workers.dev"
CONTAINER_URL="https://bench-container-latency-v2.dotdo.workers.dev"

echo "========================================"
echo "BENCHMARK SUITE - Dataset size: $SIZE"
echo "========================================"
echo ""

# OLTP Databases
OLTP_DBS=("db4" "sqlite" "postgres" "evodb" "db4-mongo")

echo "=== OLTP Benchmarks (ecommerce/$SIZE) ==="
echo ""
printf "%-12s | %-10s | %-10s | %-10s | %-10s | %-10s\n" "Database" "full_scan" "agg_count" "agg_sum" "filter" "sort_all"
printf "%-12s | %-10s | %-10s | %-10s | %-10s | %-10s\n" "------------" "----------" "----------" "----------" "----------" "----------"

for db in "${OLTP_DBS[@]}"; do
  result=$(curl -s "$OLTP_URL/benchmark/oltp/$db/ecommerce/$SIZE" 2>/dev/null)

  if echo "$result" | jq -e '.benchmarks' > /dev/null 2>&1; then
    full_scan=$(echo "$result" | jq -r '.benchmarks[] | select(.name=="full_scan") | .meanMs | . * 100 | round / 100')
    agg_count=$(echo "$result" | jq -r '.benchmarks[] | select(.name=="aggregate_count") | .meanMs | . * 100 | round / 100')
    agg_sum=$(echo "$result" | jq -r '.benchmarks[] | select(.name=="aggregate_sum") | .meanMs | . * 100 | round / 100')
    filter=$(echo "$result" | jq -r '.benchmarks[] | select(.name=="complex_filter") | .meanMs | . * 100 | round / 100')
    sort_all=$(echo "$result" | jq -r '.benchmarks[] | select(.name=="sort_all") | .meanMs | . * 100 | round / 100')
    printf "%-12s | %8sms | %8sms | %8sms | %8sms | %8sms\n" "$db" "$full_scan" "$agg_count" "$agg_sum" "$filter" "$sort_all"
  else
    error=$(echo "$result" | jq -r '.error // "Unknown error"' 2>/dev/null || echo "Request failed")
    printf "%-12s | ERROR: %s\n" "$db" "$error"
  fi
done

echo ""
echo "=== Container Benchmarks ==="
echo ""

# Container Databases (that work)
CONTAINER_DBS=("postgres" "clickhouse" "sqlite")

printf "%-12s | %-12s | %-10s | %-10s | %-10s\n" "Database" "Cold Start" "Warm Avg" "P50" "P99"
printf "%-12s | %-12s | %-10s | %-10s | %-10s\n" "------------" "------------" "----------" "----------" "----------"

for db in "${CONTAINER_DBS[@]}"; do
  result=$(curl -s "$CONTAINER_URL/benchmark/container/$db" 2>/dev/null)

  if echo "$result" | jq -e '.container' > /dev/null 2>&1; then
    cold=$(echo "$result" | jq -r '.container.coldStartMs | . / 1000 | . * 10 | round / 10')
    avg=$(echo "$result" | jq -r '.container.avgMs | . * 10 | round / 10')
    p50=$(echo "$result" | jq -r '.container.p50Ms')
    p99=$(echo "$result" | jq -r '.container.p99Ms')
    printf "%-12s | %9ss | %8sms | %8sms | %8sms\n" "$db" "$cold" "$avg" "$p50" "$p99"
  else
    error=$(echo "$result" | jq -r '.error // "Unknown error"' 2>/dev/null || echo "Request failed")
    printf "%-12s | ERROR: %s\n" "$db" "$error"
  fi
done

echo ""
echo "========================================"
echo "Benchmark complete at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================"
