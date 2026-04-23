#!/bin/bash
# benchmark/run_elb_benchmarks.sh — Sequential ElasticBLAST benchmark runner
#
# Runs benchmarks one at a time: submit → poll status → delete → next.
# Skips tests whose DB isn't ready yet.
#
# Usage:
#   AZCOPY_AUTO_LOGIN_TYPE=AZCLI PYTHONPATH=src:$PYTHONPATH \
#     bash benchmark/run_elb_benchmarks.sh
#
# Author: Moon Hyuk Choi

set -eo pipefail
cd "$(dirname "$0")/.."

export AZCOPY_AUTO_LOGIN_TYPE="${AZCOPY_AUTO_LOGIN_TYPE:-AZCLI}"
export PYTHONPATH="src:${PYTHONPATH:-}"

ELB="python bin/elastic-blast"
RESULTS_DIR="benchmark/results/v3"
LOG_DIR="${RESULTS_DIR}/logs"
SUMMARY="${RESULTS_DIR}/summary_elb.csv"

GREEN='\033[0;32m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
err()  { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; }

mkdir -p "$LOG_DIR"

# Initialize summary if not exists
if [[ ! -f "$SUMMARY" ]]; then
    echo "test_id,status,elapsed_seconds,submit_time,end_time" > "$SUMMARY"
fi

# ── Run one benchmark ──
run_one() {
    local test_id="$1"
    local config="$2"
    local log_file="${LOG_DIR}/${test_id}.log"
    
    echo ""
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}  ${test_id}${NC}"
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log "Config: $config"
    
    local submit_time=$(date '+%Y-%m-%d %H:%M:%S')
    local start_epoch=$(date +%s)
    
    # Check if already completed
    if grep -q "^${test_id},SUCCESS," "$SUMMARY" 2>/dev/null; then
        log "Already completed. Skipping."
        return 0
    fi
    
    # Delete any previous resources
    log "Cleaning up any previous run..."
    $ELB delete --cfg "$config" >> "$log_file" 2>&1 || true
    sleep 5
    
    # Submit
    log "Submitting..."
    if ! $ELB submit --cfg "$config" >> "$log_file" 2>&1; then
        err "Submit failed"
        echo "${test_id},SUBMIT_FAILED,0,${submit_time},$(date '+%Y-%m-%d %H:%M:%S')" >> "$SUMMARY"
        $ELB delete --cfg "$config" >> "$log_file" 2>&1 || true
        return 1
    fi
    log "Submitted OK. Polling status..."
    
    # Poll
    local max_polls=180  # 180 * 20s = 60 min
    for i in $(seq 1 $max_polls); do
        sleep 20
        local status=$($ELB status --cfg "$config" 2>&1 | tail -1)
        
        if echo "$status" | grep -qi "SUCCESS"; then
            local end_epoch=$(date +%s)
            local elapsed=$((end_epoch - start_epoch))
            log "SUCCESS in ${elapsed}s"
            echo "${test_id},SUCCESS,${elapsed},${submit_time},$(date '+%Y-%m-%d %H:%M:%S')" >> "$SUMMARY"
            # Get run summary
            $ELB run-summary --cfg "$config" >> "$log_file" 2>&1 || true
            break
            elif echo "$status" | grep -qi "FAILURE\|ERROR"; then
            local end_epoch=$(date +%s)
            local elapsed=$((end_epoch - start_epoch))
            err "FAILURE after ${elapsed}s"
            echo "${test_id},FAILURE,${elapsed},${submit_time},$(date '+%Y-%m-%d %H:%M:%S')" >> "$SUMMARY"
            break
        fi
        
        # Progress every 5 polls (100s)
        if (( i % 5 == 0 )); then
            log "  Poll $i: $status"
        fi
    done
    
    # Cleanup
    log "Deleting resources..."
    $ELB delete --cfg "$config" >> "$log_file" 2>&1 || true
    sleep 10
    
    return 0
}

# ── Check if blob path has files ──
blob_has_files() {
    local prefix="$1"
    local count=$(az storage blob list --account-name stgelb --container-name blast-db \
    --prefix "$prefix" --query "length([?contains(name, '.nsq')])" -o tsv --auth-mode login 2>/dev/null)
    [[ "$count" -gt 0 ]]
}

# ── Main ──
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  ElasticBLAST v3 Benchmark Runner${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# B2 tests (pathogen DB ready)
if blob_has_files "core_nt_pathogen/"; then
    run_one "B2-pathogen-E16-1N" "benchmark/configs/v3/b2_subset/B2-pathogen-E16-1N.ini"
    run_one "B2-pathogen-E64-1N" "benchmark/configs/v3/b2_subset/B2-pathogen-E64-1N.ini"
else
    log "SKIP: B2-pathogen tests — pathogen DB not on blob"
fi

if blob_has_files "core_nt_virus/"; then
    run_one "B2-virus-E64-1N" "benchmark/configs/v3/b2_subset/B2-virus-E64-1N.ini"
else
    log "SKIP: B2-virus — virus DB not on blob yet"
fi

# B3 tests (index required)
if blob_has_files "core_nt_pathogen_indexed/"; then
    run_one "B3-idx-pathogen-E16" "benchmark/configs/v3/b3_index/B3-idx-pathogen-E16.ini"
    run_one "C3-subset-idx" "benchmark/configs/v3/combined/C3-subset-idx.ini"
else
    log "SKIP: B3/C3 tests — pathogen index not on blob yet"
fi

# Final summary
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  RESULTS${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
cat "$SUMMARY"
