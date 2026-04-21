#!/bin/bash
# benchmark/run_v2_optimized.sh — Run benchmark v2 with cluster reuse
#
# Executes 10 tests in 3 phases, reusing clusters to minimize overhead.
# Total estimated: ~4 hr, ~$27
#
# Usage:
#   ./benchmark/run_v2_optimized.sh           # Run all phases
#   ./benchmark/run_v2_optimized.sh A         # Phase A only
#   ./benchmark/run_v2_optimized.sh B         # Phase B only
#   ./benchmark/run_v2_optimized.sh C         # Phase C only
#
# Author: Moon Hyuk Choi

set -eo pipefail
cd "$(dirname "$0")/.."

STORAGE="stgelb"
RG="rg-elb-koc"
BLOB_BASE="https://${STORAGE}.blob.core.windows.net"
RESULTS_DIR="benchmark/results/v2"
LOG_DIR="${RESULTS_DIR}/logs"
SUMMARY="${RESULTS_DIR}/summary.csv"

GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')]${NC} $*" >&2; }

mkdir -p "$LOG_DIR"

run_test() {
    local test_name="$1"
    local db_url="$2"
    local query_file="$3"
    local machine="$4"
    local nodes="$5"
    local cluster="$6"
    local extra_opts="${7:-}"
    
    local results_url="${BLOB_BASE}/results/v2/${test_name}"
    local logfile="${LOG_DIR}/${test_name}.log"
    local ini_file="${LOG_DIR}/${test_name}.ini"
    
    log "${CYAN}=== ${test_name} ===${NC}"
    log "  DB: $(basename $db_url)"
    log "  Query: $(basename $query_file)"
    log "  VM: ${machine} × ${nodes} nodes"
    
    # Clean up previous jobs before reuse
    if kubectl --context="${cluster}" get nodes &>/dev/null; then
        log "  Cleaning up previous jobs on cluster..."
        kubectl --context="${cluster}" delete jobs --all --wait=false 2>/dev/null || true
        kubectl --context="${cluster}" delete daemonset create-workspace 2>/dev/null || true
        sleep 5
    fi
    
    cat > "$ini_file" << EOF
[cloud-provider]
azure-region = koreacentral
azure-acr-resource-group = rg-elbacr
azure-acr-name = elbacr
azure-resource-group = ${RG}
azure-storage-account = ${STORAGE}
azure-storage-account-container = blast-db

[cluster]
name = ${cluster}
machine-type = ${machine}
num-nodes = ${nodes}
exp-use-local-ssd = true
reuse = true

[blast]
program = blastn
db = ${db_url}
queries = ${query_file}
results = ${results_url}
options = -max_target_seqs 500 -evalue 0.05 -word_size 28 -dust yes -soft_masking true -outfmt 7 ${extra_opts}
batch-len = 100000
mem-limit = 4G

[timeouts]
init-pv = 90
blast-k8s-job = 10080
EOF
    
    local start_time=$(date +%s)
    
    export PYTHONPATH=src:$PYTHONPATH
    export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
    python bin/elastic-blast submit \
    --cfg "$ini_file" \
    --loglevel DEBUG \
    2>&1 | tee "$logfile"
    
    local exit_code=${PIPESTATUS[0]}
    
    # Wait for completion using kubectl (more reliable than elastic-blast status)
    if [[ $exit_code -eq 0 ]]; then
        log "  Submit OK. Waiting for jobs to complete..."
        local wait_start=$(date +%s)
        local max_wait=7200  # 2 hr max
        
        while true; do
            # Check via kubectl directly
            local blast_info
            blast_info=$(kubectl --context="${cluster}" get jobs -l app=blast --no-headers 2>/dev/null || echo "")
            local total_blast=$(echo "$blast_info" | grep -c "." 2>/dev/null | tr -d '\n' || echo 0)
            local done_blast=$(echo "$blast_info" | grep -c "Complete" 2>/dev/null | tr -d '\n' || echo 0)
            local failed_blast=$(echo "$blast_info" | grep -c "Failed" 2>/dev/null | tr -d '\n' || echo 0)
            
            # Also check finalizer and submit-jobs
            local submit_done=$(kubectl --context="${cluster}" get job submit-jobs --no-headers 2>/dev/null | grep -c "Complete" | tr -d '\n' || echo 0)
            
            local elapsed=$(( $(date +%s) - wait_start ))
            
            if [[ $total_blast -gt 0 && $done_blast -eq $total_blast ]]; then
                log "  ALL BLAST JOBS COMPLETE ($done_blast/$total_blast) in ${elapsed}s"
                break
                elif [[ $failed_blast -gt 0 ]]; then
                err "  $failed_blast BLAST jobs FAILED"
                exit_code=1
                break
            fi
            
            if [[ $elapsed -gt $max_wait ]]; then
                err "  Timeout after ${max_wait}s"
                exit_code=1
                break
            fi
            
            if [[ $total_blast -gt 0 ]]; then
                log "  Jobs: $done_blast/$total_blast complete ($failed_blast failed, ${elapsed}s)"
            else
                local init_status=$(kubectl --context="${cluster}" get job init-ssd-0 --no-headers 2>/dev/null | awk '{print $2}' || echo "?")
                log "  Waiting: init-ssd=$init_status, submit=$submit_done, blast=0 (${elapsed}s)"
            fi
            sleep 30
        done
    fi
    
    local total_time=$(( $(date +%s) - start_time ))
    
    # Collect job timings
    if [[ $exit_code -eq 0 ]] && kubectl --context="${cluster}" get nodes &>/dev/null; then
        log "  Job timings:"
        kubectl --context="${cluster}" get jobs -o json 2>/dev/null | python3 -c "
import json, sys
from datetime import datetime
data = json.load(sys.stdin)
for j in sorted(data['items'], key=lambda x: x['metadata']['name']):
    name = j['metadata']['name']
    s = j.get('status', {})
    start = s.get('startTime', '')
    comp = s.get('completionTime', '')
    if start and comp:
        t0 = datetime.fromisoformat(start.replace('Z', '+00:00'))
        t1 = datetime.fromisoformat(comp.replace('Z', '+00:00'))
        dur = (t1 - t0).total_seconds()
        print(f'    {name}: {dur:.0f}s ({dur/60:.1f} min)')
        " 2>/dev/null || true
    fi
    
    if [[ $exit_code -eq 0 ]]; then
        log "  ${GREEN}PASS${NC}: ${test_name} (${total_time}s = $((total_time/60))min)"
        echo "${test_name},PASS,${total_time}" >> "$SUMMARY"
    else
        err "  ${RED}FAIL${NC}: ${test_name} (exit ${exit_code}, ${total_time}s)"
        echo "${test_name},FAIL,${total_time}" >> "$SUMMARY"
    fi
    echo ""
}

delete_cluster() {
    local cluster="$1"
    log "Deleting cluster: ${cluster}..."
    az aks delete -g "$RG" -n "${cluster}" --yes --no-wait 2>/dev/null || true
    log "Cluster ${cluster} delete initiated"
}

# ── DB URLs ──
CORE_NT="${BLOB_BASE}/blast-db/core_nt/core_nt"
QUERIES_10="${BLOB_BASE}/queries/pathogen-10.fa"
QUERIES_100="${BLOB_BASE}/queries/pathogen-100.fa"
QUERIES_300="${BLOB_BASE}/queries/pathogen-300.fa"
QUERIES_1000="${BLOB_BASE}/queries/pathogen-1000.fa"

# ── Phase A: E64s × 1 node (baseline) ──
phase_a() {
    log ""
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log " Phase A: E64s_v3 × 1 node (baseline)"
    log " Cluster: elb-v2-a"
    log ' Estimated: ~70 min, ~$5'
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log ""
    
    # A1: Full DB, 10 queries (baseline)
    run_test "A1-fulldb-10q-E64-1n" \
    "$CORE_NT" "$QUERIES_10" \
    "Standard_E64s_v3" 1 "elb-v2-a"
    
    # A2: Full DB, 300 queries
    run_test "A2-fulldb-300q-E64-1n" \
    "$CORE_NT" "$QUERIES_300" \
    "Standard_E64s_v3" 1 "elb-v2-a"
    
    delete_cluster "elb-v2-a"
}

# ── Phase B: E64s × 1 node (scale-up comparison) ──
phase_b() {
    log ""
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log " Phase B: E64s_v3 × 1 node (scale-up)"
    log " Cluster: elb-v2-b"
    log ' Estimated: ~75 min, ~$7.00'
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log ""
    
    # B1: Full DB, 10 queries (E64 vs E32 comparison)
    run_test "B1-fulldb-10q-E64-1n" \
    "$CORE_NT" "$QUERIES_10" \
    "Standard_E64s_v3" 1 "elb-v2-b"
    
    # B2: Full DB, 300 queries
    run_test "B2-fulldb-300q-E64-1n" \
    "$CORE_NT" "$QUERIES_300" \
    "Standard_E64s_v3" 1 "elb-v2-b"
    
    # B3: Full DB, 1000 queries
    run_test "B3-fulldb-1000q-E64-1n" \
    "$CORE_NT" "$QUERIES_1000" \
    "Standard_E64s_v3" 1 "elb-v2-b"
    
    delete_cluster "elb-v2-b"
}

# ── Phase C: E64s × 2 nodes (scale-out) ──
phase_c() {
    log ""
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log " Phase C: E64s_v3 × 2 nodes (scale-out)"
    log " Cluster: elb-v2-c"
    log ' Estimated: ~90 min, ~$12'
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log ""
    
    # C1: Full DB, 10 queries, 2N
    run_test "C1-fulldb-10q-E64-2n" \
    "$CORE_NT" "$QUERIES_10" \
    "Standard_E64s_v3" 2 "elb-v2-c"
    
    # C2: Full DB, 300 queries, 2N
    run_test "C2-fulldb-300q-E64-2n" \
    "$CORE_NT" "$QUERIES_300" \
    "Standard_E64s_v3" 2 "elb-v2-c"
    
    # C3: Full DB, 1000 queries, 2N
    run_test "C3-fulldb-1000q-E64-2n" \
    "$CORE_NT" "$QUERIES_1000" \
    "Standard_E64s_v3" 2 "elb-v2-c"
    
    delete_cluster "elb-v2-c"
}

# ── Main ──
echo "test,status,elapsed_s" > "$SUMMARY"

# Enable storage public access
log "Enabling storage public network access..."
az storage account update -n "$STORAGE" -g "$RG" --public-network-access Enabled -o none 2>/dev/null
log "Waiting 30s for propagation..."
sleep 30

PHASE="${1:-ALL}"

case "$PHASE" in
    A|a) phase_a ;;
    B|b) phase_b ;;
    C|c) phase_c ;;
    BC|bc)
        phase_b
        phase_c
    ;;
    ALL|all)
        phase_a
        phase_b
        phase_c
    ;;
    *)
        echo "Usage: $0 [A|B|C|ALL]"
        exit 1
    ;;
esac

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log " Benchmark Complete"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log ""
log "Results:"
column -t -s',' "$SUMMARY"
log ""
log "Logs: ${LOG_DIR}/"
log "Storage access is still ENABLED — run 'az storage account update -n stgelb -g rg-elb-koc --public-network-access Disabled -o none' when done"
