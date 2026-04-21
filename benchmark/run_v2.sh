#!/bin/bash
# benchmark/run_v2.sh — Run benchmark v2 strategy tests
#
# Usage:
#   ./benchmark/run_v2.sh                    # Run all strategies
#   ./benchmark/run_v2.sh query_split        # Run one strategy
#   ./benchmark/run_v2.sh taxonomy 10        # Run specific strategy + scale
#   ./benchmark/run_v2.sh --status           # Check running tests
#   ./benchmark/run_v2.sh --upload-queries   # Upload query files to blob
#
# Author: Moon Hyuk Choi

set -euo pipefail
cd "$(dirname "$0")/.."

RG="rg-elb-koc"
STORAGE="stgelb"
BLOB_BASE="https://${STORAGE}.blob.core.windows.net"
CONFIGS_DIR="benchmark/configs/v2/strategies"
RESULTS_DIR="benchmark/results/v2"
LOG_DIR="${RESULTS_DIR}/logs"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')]${NC} $*" >&2; }

mkdir -p "$LOG_DIR"

# ── Upload query files ──
upload_queries() {
    log "Uploading query files to Blob Storage..."
    for f in benchmark/queries/pathogen-*.fa; do
        local name=$(basename "$f")
        log "  Uploading ${name}..."
        AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy cp "$f" \
        "${BLOB_BASE}/queries/${name}" \
        --overwrite=ifSourceNewer --log-level=WARNING
    done
    log "All query files uploaded."
}

# ── Enable storage public access (needed for this PC) ──
enable_storage() {
    log "Enabling storage public network access..."
    az storage account update -n "$STORAGE" -g "$RG" \
    --public-network-access Enabled -o none 2>/dev/null
    log "Waiting 15s for propagation..."
    sleep 15
}

# ── Disable storage public access (restore security) ──
disable_storage() {
    log "Disabling storage public network access..."
    az storage account update -n "$STORAGE" -g "$RG" \
    --public-network-access Disabled -o none 2>/dev/null
}

# ── Run a single test ──
run_test() {
    local ini="$1"
    local name=$(basename "$ini" .ini)
    local logfile="${LOG_DIR}/${name}.log"
    
    log "${CYAN}Running: ${name}${NC}"
    log "  Config: ${ini}"
    log "  Log: ${logfile}"
    
    local start_time=$(date +%s)
    
    PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
    python bin/elastic-blast submit \
    --cfg "$ini" \
    --loglevel DEBUG \
    2>&1 | tee "$logfile"
    
    local exit_code=${PIPESTATUS[0]}
    local elapsed=$(( $(date +%s) - start_time ))
    
    if [[ $exit_code -eq 0 ]]; then
        log "${GREEN}PASS${NC}: ${name} (${elapsed}s)"
        echo "${name},PASS,${elapsed}" >> "${RESULTS_DIR}/summary.csv"
    else
        err "${RED}FAIL${NC}: ${name} (exit ${exit_code}, ${elapsed}s)"
        echo "${name},FAIL,${elapsed}" >> "${RESULTS_DIR}/summary.csv"
    fi
}

# ── Status check ──
check_status() {
    log "Checking running ElasticBLAST jobs..."
    for ini in ${CONFIGS_DIR}/*/*.ini; do
        local name=$(basename "$ini" .ini)
        echo -n "  ${name}: "
        PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast status \
        --cfg "$ini" 2>/dev/null || echo "N/A"
    done
}

# ── Main ──
case "${1:-all}" in
    --upload-queries)
        enable_storage
        upload_queries
    ;;
    --status)
        check_status
    ;;
    --enable-storage)
        enable_storage
    ;;
    --disable-storage)
        disable_storage
    ;;
    all)
        log "Running all v2 benchmark strategies..."
        echo "test,status,elapsed_s" > "${RESULTS_DIR}/summary.csv"
        
        for strategy_dir in ${CONFIGS_DIR}/*/; do
            strategy=$(basename "$strategy_dir")
            log ""
            log "━━━ Strategy: ${strategy} ━━━"
            for ini in "${strategy_dir}"*.ini; do
                [[ -f "$ini" ]] || continue
                run_test "$ini"
            done
        done
        
        log ""
        log "All tests complete. Summary:"
        cat "${RESULTS_DIR}/summary.csv"
    ;;
    *)
        # Run specific strategy or strategy+scale
        STRATEGY="${1}"
        SCALE="${2:-}"
        
        if [[ -n "$SCALE" ]]; then
            # Specific test
            pattern="${CONFIGS_DIR}/${STRATEGY}/*-${SCALE}q*.ini"
        else
            # All tests for strategy
            pattern="${CONFIGS_DIR}/${STRATEGY}/*.ini"
        fi
        
        echo "test,status,elapsed_s" > "${RESULTS_DIR}/summary.csv"
        for ini in $pattern; do
            [[ -f "$ini" ]] || { err "No configs found: $pattern"; exit 1; }
            run_test "$ini"
        done
        
        log "Done. Summary:"
        cat "${RESULTS_DIR}/summary.csv"
    ;;
esac
