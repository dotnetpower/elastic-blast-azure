#!/bin/bash
# benchmark/run_v3.sh — Run v3 DB optimization benchmark tests
#
# Orchestrates the v3 benchmark: taxonomy subset, DB sharding,
# MegaBLAST indexing, and combined strategies.
#
# Usage:
#   ./benchmark/run_v3.sh                        # Run all phases in order
#   ./benchmark/run_v3.sh b2                     # Axis B2 only (taxonomy subset)
#   ./benchmark/run_v3.sh b1                     # Axis B1 only (DB sharding)
#   ./benchmark/run_v3.sh b3                     # Axis B3 only (MegaBLAST index)
#   ./benchmark/run_v3.sh combined               # Combined strategies
#   ./benchmark/run_v3.sh B2-pathogen-E64-1N     # Single test by ID
#   ./benchmark/run_v3.sh --status               # Check running tests
#   ./benchmark/run_v3.sh --collect <test_id>    # Collect results for a test
#   ./benchmark/run_v3.sh --merge <test_id>      # Merge sharded results
#   ./benchmark/run_v3.sh --dry-run              # Show what would run
#
# Prerequisites:
#   - DB variants pre-staged (run prep_db_v3.sh first)
#   - Query files uploaded (pathogen-10.fa in blob)
#   - venv activated, PYTHONPATH set
#
# Author: Moon Hyuk Choi

set -eo pipefail
cd "$(dirname "$0")/.."

export PYTHONPATH="${PYTHONPATH:-src}:src"
export AZCOPY_AUTO_LOGIN_TYPE="${AZCOPY_AUTO_LOGIN_TYPE:-AZCLI}"

# ── Configuration ──
RG="rg-elb-koc"
STORAGE="stgelb"
BLOB_BASE="https://${STORAGE}.blob.core.windows.net"
CONFIGS_DIR="benchmark/configs/v3"
RESULTS_DIR="benchmark/results/v3"
LOG_DIR="${RESULTS_DIR}/logs"
DATA_DIR="${RESULTS_DIR}/data"
DRY_RUN=false

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; }
section() {
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  $*${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

mkdir -p "$LOG_DIR" "$DATA_DIR"

# ── Enable storage public access ──
enable_storage() {
    if $DRY_RUN; then
        log "[DRY-RUN] Would enable storage public access"
        return 0
    fi
    --public-network-access Enabled -o none 2>/dev/null || true
    log "Waiting 15s for propagation..."
    sleep 15
}

# ── Run a single test ──
run_test() {
    local ini="$1"
    local test_id
    test_id=$(basename "$ini" .ini)
    local logfile="${LOG_DIR}/${test_id}.log"
    local datafile="${DATA_DIR}/${test_id}.json"
    
    section "Running: ${test_id}"
    log "  Config: ${ini}"
    log "  Log:    ${logfile}"
    
    if $DRY_RUN; then
        log "  [DRY-RUN] Would run: elastic-blast submit --cfg ${ini}"
        echo "${test_id},DRY_RUN,0,0" >> "${RESULTS_DIR}/summary.csv"
        return 0
    fi
    
    local start_time
    start_time=$(date +%s)
    local start_iso
    start_iso=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    
    # Submit
    log "  Submitting..."
    PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
    python bin/elastic-blast submit \
    --cfg "$ini" \
    --loglevel DEBUG \
    2>&1 | tee "$logfile"
    local submit_exit=${PIPESTATUS[0]}
    
    if [[ $submit_exit -ne 0 ]]; then
        err "  SUBMIT FAILED (exit $submit_exit)"
        echo "${test_id},SUBMIT_FAIL,${submit_exit},0" >> "${RESULTS_DIR}/summary.csv"
        return $submit_exit
    fi
    
    # Poll status
    log "  Polling status..."
    local max_wait=7200  # 2 hours max
    local poll_interval=30
    local waited=0
    local status="UNKNOWN"
    
    while [[ $waited -lt $max_wait ]]; do
        status=$(PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast status \
        --cfg "$ini" 2>/dev/null | tail -1 || echo "UNKNOWN")
        
        if [[ "$status" == *"SUCCESS"* ]]; then
            log "  Status: ${GREEN}SUCCESS${NC}"
            break
            elif [[ "$status" == *"FAILURE"* ]]; then
            err "  Status: FAILURE"
            break
        fi
        
        if (( waited % 120 == 0 )); then
            log "  Status: ${status} (${waited}s elapsed)"
        fi
        
        sleep $poll_interval
        waited=$((waited + poll_interval))
    done
    
    local end_time
    end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    local elapsed_min=$((elapsed / 60))
    
    # Collect K8s timing data
    log "  Collecting timing data..."
    collect_timing "$test_id" "$ini" "$start_iso" "$elapsed"
    
    if [[ "$status" == *"SUCCESS"* ]]; then
        log "${GREEN}PASS${NC}: ${test_id} (${elapsed_min} min)"
        echo "${test_id},PASS,0,${elapsed}" >> "${RESULTS_DIR}/summary.csv"
        elif [[ "$status" == *"FAILURE"* ]]; then
        err "${RED}FAIL${NC}: ${test_id} (${elapsed_min} min)"
        echo "${test_id},FAIL,1,${elapsed}" >> "${RESULTS_DIR}/summary.csv"
    else
        warn "TIMEOUT: ${test_id} (${elapsed_min} min, status: ${status})"
        echo "${test_id},TIMEOUT,2,${elapsed}" >> "${RESULTS_DIR}/summary.csv"
    fi
}

# ── Collect timing data from K8s jobs ──
collect_timing() {
    local test_id="$1"
    local ini="$2"
    local start_iso="$3"
    local total_elapsed="$4"
    local datafile="${DATA_DIR}/${test_id}.json"
    
    # Get kubectl context (cluster name from INI)
    local cluster_name
    cluster_name=$(grep "^name" "$ini" | head -1 | awk -F= '{print $2}' | tr -d ' ')
    
    # Try to get K8s job timings
    local jobs_json
    jobs_json=$(kubectl get jobs -l app=setup -o json 2>/dev/null || echo '{"items":[]}')
    local blast_json
    blast_json=$(kubectl get jobs -l app=blast -o json 2>/dev/null || echo '{"items":[]}')
    
    # Extract init-ssd timing
    local init_start init_end init_elapsed
    init_start=$(echo "$jobs_json" | python3 -c "
import json,sys
data=json.load(sys.stdin)
for j in data.get('items',[]):
    if 'init-ssd' in j['metadata'].get('name','') or 'get-blastdb' in j['metadata'].get('name',''):
        print(j.get('status',{}).get('startTime',''))
        break
    " 2>/dev/null || echo "")
    
    init_end=$(echo "$jobs_json" | python3 -c "
import json,sys
data=json.load(sys.stdin)
for j in data.get('items',[]):
    if 'init-ssd' in j['metadata'].get('name','') or 'get-blastdb' in j['metadata'].get('name',''):
        print(j.get('status',{}).get('completionTime',''))
        break
    " 2>/dev/null || echo "")
    
    # Extract BLAST job timings
    local blast_times
    blast_times=$(echo "$blast_json" | python3 -c "
import json,sys
from datetime import datetime
data=json.load(sys.stdin)
times = []
for j in data.get('items',[]):
    s = j.get('status',{})
    start = s.get('startTime','')
    end = s.get('completionTime','')
    name = j['metadata'].get('name','')
    if start and end:
        try:
            st = datetime.fromisoformat(start.rstrip('Z'))
            et = datetime.fromisoformat(end.rstrip('Z'))
            elapsed = (et-st).total_seconds()
            times.append(f'{name}:{elapsed:.0f}')
        except: pass
for t in sorted(times):
    print(t)
    " 2>/dev/null || echo "")
    
    # Write structured data
    cat > "$datafile" <<EOF
{
    "test_id": "${test_id}",
    "config": "${ini}",
    "start_time": "${start_iso}",
    "total_elapsed_seconds": ${total_elapsed},
    "cluster": "${cluster_name}",
    "init_ssd_start": "${init_start}",
    "init_ssd_end": "${init_end}",
    "blast_job_times": [
$(echo "$blast_times" | while IFS=: read -r name elapsed; do
    [[ -z "$name" ]] && continue
    echo "        {\"name\": \"${name}\", \"elapsed_seconds\": ${elapsed:-0}}"
done | paste -sd',\n')
    ]
}
EOF
    log "  Timing data: ${datafile}"
}

# ── Merge sharded results ──
merge_results() {
    local test_id="$1"
    log "Merging sharded results for ${test_id}..."
    
    local results_blob="${BLOB_BASE}/results/v3/b1_shard/${test_id}"
    local local_dir="${RESULTS_DIR}/raw/${test_id}"
    mkdir -p "$local_dir"
    
    # Download results
    AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy cp \
    "${results_blob}/*" "$local_dir/" \
    --recursive --log-level=WARNING
    
    # Find result files
    local result_files
    result_files=$(find "$local_dir" -name "*.out.gz" -o -name "*.out" | sort)
    
    if [[ -z "$result_files" ]]; then
        err "No result files found in ${local_dir}"
        return 1
    fi
    
    # Use merger.py
    PYTHONPATH=benchmark:$PYTHONPATH python3 -c "
from strategies.merger import merge_shard_results
import glob
files = sorted(glob.glob('${local_dir}/**/*.out*', recursive=True))
print(f'Merging {len(files)} result files...')
merge_shard_results(files, '${RESULTS_DIR}/merged/${test_id}_merged.out', max_target_seqs=500)
print('Done.')
    "
    log "Merged: ${RESULTS_DIR}/merged/${test_id}_merged.out"
}

# ── Delete cluster for a test ──
cleanup_test() {
    local ini="$1"
    local test_id
    test_id=$(basename "$ini" .ini)
    
    log "Cleaning up: ${test_id}..."
    PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast delete \
    --cfg "$ini" \
    --loglevel DEBUG 2>&1 | tail -5
}

# ── Run tests for an axis ──
run_axis() {
    local axis="$1"
    local config_dir="${CONFIGS_DIR}/${axis}"
    
    if [[ ! -d "$config_dir" ]]; then
        err "Config directory not found: ${config_dir}"
        return 1
    fi
    
    section "Axis: ${axis}"
    
    local count
    count=$(ls "$config_dir"/*.ini 2>/dev/null | wc -l)
    log "Found ${count} test configs"
    
    for ini in "$config_dir"/*.ini; do
        [[ -f "$ini" ]] || continue
        run_test "$ini"
        
        # Cleanup between tests (delete cluster to save cost)
        if ! $DRY_RUN; then
            cleanup_test "$ini" || true
        fi
    done
}

# ── Check status of all tests ──
check_status() {
    log "Checking status of v3 tests..."
    for ini in "$CONFIGS_DIR"/*/*.ini; do
        local test_id
        test_id=$(basename "$ini" .ini)
        echo -n "  ${test_id}: "
        PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast status \
        --cfg "$ini" 2>/dev/null | tail -1 || echo "N/A"
    done
}

# ── Verify prerequisites ──
check_prereqs() {
    section "Prerequisites check"
    
    local ok=true
    
    # Check venv
    if [[ -z "${VIRTUAL_ENV:-}" ]]; then
        warn "Virtual environment not activated"
        warn "Run: source venv/bin/activate"
    fi
    
    # Check PYTHONPATH
    log "PYTHONPATH includes src/: $(echo "${PYTHONPATH:-}" | grep -c 'src' || echo 'NO')"
    
    # Check az login
    if az account show &>/dev/null; then
        local sub
        sub=$(az account show --query name -o tsv 2>/dev/null)
        log "Azure: logged in (${sub})"
    else
        err "Azure: not logged in. Run: az login"
        ok=false
    fi
    
    # Check configs
    local config_count
    config_count=$(find "$CONFIGS_DIR" -name "*.ini" 2>/dev/null | wc -l)
    if [[ $config_count -eq 0 ]]; then
        err "No configs found. Run: python benchmark/configs/v3/generate_configs.py"
        ok=false
    else
        log "Configs: ${config_count} INI files"
    fi
    
    # Check DB availability
    log "Checking blob storage for DB variants..."
    for db_name in core_nt_pathogen core_nt_virus core_nt_broad; do
        local count
        count=$(AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy list \
        "${BLOB_BASE}/blast-db/${db_name}/" 2>/dev/null | grep -c "INFO:" || echo "0")
        if [[ $count -gt 0 ]]; then
            log "  ✓ ${db_name}: ${count} files"
        else
            warn "  ✗ ${db_name}: NOT FOUND — run prep_db_v3.sh first"
            ok=false
        fi
    done
    
    if ! $ok; then
        err "Prerequisites not met. Fix issues above and retry."
        return 1
    fi
    
    log "All prerequisites OK"
}

# ── Print execution plan ──
print_plan() {
    section "v3 Execution Plan"
    
    echo ""
    echo -e "${BOLD}Phase 1: B2 Taxonomy Subset (cheapest, highest ROI)${NC}"
    for ini in "$CONFIGS_DIR"/b2_subset/*.ini; do
        echo "  $(basename "$ini" .ini)"
    done
    
    echo ""
    echo -e "${BOLD}Phase 2: B1 DB Sharding${NC}"
    for ini in "$CONFIGS_DIR"/b1_shard/*.ini; do
        echo "  $(basename "$ini" .ini)"
    done
    
    echo ""
    echo -e "${BOLD}Phase 3: Combined Strategies${NC}"
    for ini in "$CONFIGS_DIR"/combined/*.ini; do
        echo "  $(basename "$ini" .ini)"
    done
    
    echo ""
    echo -e "${BOLD}Phase 4: B3 MegaBLAST Index${NC}"
    for ini in "$CONFIGS_DIR"/b3_index/*.ini; do
        echo "  $(basename "$ini" .ini)"
    done
    
    echo ""
    local total
    total=$(find "$CONFIGS_DIR" -name "*.ini" | wc -l)
    echo -e "${BOLD}Total: ${total} tests${NC}"
}

# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════
main() {
    local cmd="${1:-all}"
    
    # Handle flags
    if [[ "$cmd" == "--dry-run" ]]; then
        DRY_RUN=true
        cmd="${2:-all}"
    fi
    
    case "$cmd" in
        --status)
            check_status
        ;;
        --plan)
            print_plan
        ;;
        --prereqs)
            check_prereqs
        ;;
        --collect)
            collect_timing "${2}" "${CONFIGS_DIR}/*/${2}.ini" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "0"
        ;;
        --merge)
            merge_results "${2}"
        ;;
        --cleanup)
            for ini in "$CONFIGS_DIR"/*/*.ini; do
                cleanup_test "$ini" || true
            done
        ;;
        b1)
            enable_storage
            echo "test_id,status,exit_code,elapsed_seconds" > "${RESULTS_DIR}/summary.csv"
            run_axis "b1_shard"
        ;;
        b2)
            enable_storage
            echo "test_id,status,exit_code,elapsed_seconds" > "${RESULTS_DIR}/summary.csv"
            run_axis "b2_subset"
        ;;
        b3)
            enable_storage
            echo "test_id,status,exit_code,elapsed_seconds" > "${RESULTS_DIR}/summary.csv"
            run_axis "b3_index"
        ;;
        combined|reference)
            enable_storage
            echo "test_id,status,exit_code,elapsed_seconds" > "${RESULTS_DIR}/summary.csv"
            run_axis "${cmd}"
        ;;
        all)
            enable_storage
            echo "test_id,status,exit_code,elapsed_seconds" > "${RESULTS_DIR}/summary.csv"
            
            section "v3 Benchmark — Full Run"
            print_plan
            
            # Phase 1: Taxonomy subset (cheapest)
            run_axis "b2_subset"
            
            # Phase 2: DB Sharding
            run_axis "b1_shard"
            
            # Phase 3: Combined
            run_axis "combined"
            
            # Phase 4: MegaBLAST Index
            run_axis "b3_index"
            
            section "ALL TESTS COMPLETE"
            log ""
            log "Summary:"
            cat "${RESULTS_DIR}/summary.csv"
            log ""
            log "Next: Generate report with"
            log "  python benchmark/generate_report_v3.py"
        ;;
        *)
            # Try as a specific test ID
            local found=false
            for ini in "$CONFIGS_DIR"/*/"${cmd}.ini"; do
                if [[ -f "$ini" ]]; then
                    enable_storage
                    echo "test_id,status,exit_code,elapsed_seconds" > "${RESULTS_DIR}/summary.csv"
                    run_test "$ini"
                    found=true
                    break
                fi
            done
            if ! $found; then
                err "Unknown command or test ID: ${cmd}"
                echo ""
                echo "Usage: $0 [b1|b2|b3|combined|reference|all|<test_id>]"
                echo "       $0 --status | --plan | --prereqs | --dry-run"
                echo "       $0 --collect <test_id> | --merge <test_id>"
                exit 1
            fi
        ;;
    esac
}

main "$@"
