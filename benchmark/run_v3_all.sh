#!/bin/bash
# benchmark/run_v3_all.sh — Automated v3 benchmark runner using ElasticBLAST
#
# Runs B2/B3 benchmarks sequentially via elastic-blast submit/status/delete.
# Prep VM builds subset DBs in parallel when possible.
#
# Usage:
#   ./benchmark/run_v3_all.sh           # Run all feasible benchmarks
#   ./benchmark/run_v3_all.sh prep      # Prep only (upload DBs, no benchmarks)
#   ./benchmark/run_v3_all.sh run       # Run benchmarks (assumes DBs ready)
#
# Author: Moon Hyuk Choi

set -eo pipefail
cd "$(dirname "$0")/.."

# ── Config ──
RG="rg-elb-koc"
VM_NAME="elb-v3-prep"
STORAGE="stgelb"
BLOB_BASE="https://${STORAGE}.blob.core.windows.net/blast-db"
PYTHONPATH="src:${PYTHONPATH:-}"
export PYTHONPATH
ELB="python bin/elastic-blast"
RESULTS_DIR="benchmark/results/v3"
LOG_DIR="${RESULTS_DIR}/logs"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
err()  { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; }
bold() { echo -e "${BOLD}$*${NC}"; }

mkdir -p "$LOG_DIR"

# ── Helper: Run VM command with retry on Conflict ──
vm_run() {
    local script="$1"
    local max_retries=20
    for i in $(seq 1 "$max_retries"); do
        result=$(az vm run-command invoke -g "$RG" -n "$VM_NAME" \
            --command-id RunShellScript --scripts "$script" \
        -o tsv --query 'value[0].message' 2>&1)
        if echo "$result" | grep -q "Conflict"; then
            log "  VM locked, retry $i/$max_retries..."
            sleep 30
        else
            echo "$result"
            return 0
        fi
    done
    err "VM command timed out after $max_retries retries"
    return 1
}

# ── Helper: Run elastic-blast benchmark ──
run_benchmark() {
    local test_id="$1"
    local config="$2"
    local log_file="${LOG_DIR}/${test_id}.log"
    
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  Running: ${test_id}"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "Config: ${config}"
    log "Log:    ${log_file}"
    
    local start_time=$(date +%s)
    
    # Step 1: Submit
    log "Submitting..."
    $ELB submit --cfg "$config" 2>&1 | tee -a "$log_file"
    local submit_rc=${PIPESTATUS[0]}
    if [[ $submit_rc -ne 0 ]]; then
        err "Submit failed for ${test_id} (rc=$submit_rc)"
        echo "${test_id},SUBMIT_FAILED,${submit_rc},0" >> "${RESULTS_DIR}/summary.csv"
        # Cleanup
        $ELB delete --cfg "$config" 2>&1 | tee -a "$log_file" || true
        return 1
    fi
    
    # Step 2: Poll status until done
    log "Polling status..."
    local status=""
    local poll_count=0
    local max_polls=120  # 120 * 30s = 60 min max
    while [[ $poll_count -lt $max_polls ]]; do
        sleep 30
        status=$($ELB status --cfg "$config" 2>&1 | tee -a "$log_file" | tail -1)
        poll_count=$((poll_count + 1))
        
        if echo "$status" | grep -qi "SUCCESS"; then
            log "Status: SUCCESS"
            break
            elif echo "$status" | grep -qi "FAILURE\|ERROR"; then
            err "Status: FAILURE"
            break
        fi
        log "  Poll $poll_count: $status"
    done
    
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    
    # Determine final status
    local final_status="UNKNOWN"
    if echo "$status" | grep -qi "SUCCESS"; then
        final_status="SUCCESS"
        elif echo "$status" | grep -qi "FAILURE\|ERROR"; then
        final_status="FAILURE"
    else
        final_status="TIMEOUT"
    fi
    
    log "Result: ${test_id} = ${final_status} (${elapsed}s)"
    echo "${test_id},${final_status},0,${elapsed}" >> "${RESULTS_DIR}/summary.csv"
    
    # Step 3: Delete resources
    log "Cleaning up..."
    $ELB delete --cfg "$config" 2>&1 | tee -a "$log_file" || true
    
    # Wait for cluster deletion
    sleep 10
    
    return 0
}

# ── Phase 0: Upload pathogen DB ──
phase_upload_pathogen() {
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  Phase 0: Upload pathogen DB to blob"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Check if already uploaded
    local exists=$(az storage blob list --account-name "$STORAGE" --container-name blast-db \
    --prefix "core_nt_pathogen/" --query "length([?contains(name, '.nsq')])" -o tsv --auth-mode login 2>/dev/null)
    if [[ "$exists" -gt 0 ]]; then
        log "Pathogen DB already on blob ($exists .nsq files). Skipping upload."
        return 0
    fi
    
    log "Uploading pathogen DB from VM..."
    vm_run '#!/bin/bash
set -e
export BLASTDB=/blast/blastdb
SUBSET_DIR="/blast/subsets"
BLOB="https://stgelb.blob.core.windows.net/blast-db"

azcopy login --identity 2>/dev/null
START=$(date +%s)

azcopy cp "${SUBSET_DIR}/core_nt_pathogen.*" \
    "${BLOB}/core_nt_pathogen/" \
    --overwrite=ifSourceNewer \
    --block-size-mb=256 \
    --log-level=WARNING 2>&1 | tail -5

END=$(date +%s)
echo "RUNTIME upload-pathogen $((END - START)) seconds"
echo "UPLOAD_PATHOGEN_DONE"
    '
    log "Pathogen DB upload complete."
}

# ── Phase 0b: Build + upload virus DB ──
phase_build_virus() {
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  Phase 0b: Build + upload virus-only DB"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Check if already uploaded
    local exists=$(az storage blob list --account-name "$STORAGE" --container-name blast-db \
    --prefix "core_nt_virus/" --query "length([?contains(name, '.nsq')])" -o tsv --auth-mode login 2>/dev/null)
    if [[ "$exists" -gt 0 ]]; then
        log "Virus DB already on blob ($exists .nsq files). Skipping."
        return 0
    fi
    
    log "Building virus-only DB on VM (this takes ~40 min)..."
    vm_run '#!/bin/bash
set -e
export BLASTDB=/blast/blastdb
DB_DIR="/blast/blastdb"
SUBSET_DIR="/blast/subsets"
BLOB="https://stgelb.blob.core.windows.net/blast-db"
mkdir -p "$SUBSET_DIR"

echo "10239" > /tmp/virus_taxids.txt

# Delete pathogen FASTA to free space
rm -f ${SUBSET_DIR}/pathogen_subset.fa 2>/dev/null

echo "=== Extract virus subset FASTA ==="
START=$(date +%s)
blastdbcmd -db ${DB_DIR}/core_nt \
    -taxidlist /tmp/virus_taxids.txt \
    -out ${SUBSET_DIR}/virus_subset.fa 2>&1 | tail -3
END=$(date +%s)
VSIZE=$(du -sh ${SUBSET_DIR}/virus_subset.fa 2>/dev/null | cut -f1)
VSEQS=$(grep -c "^>" ${SUBSET_DIR}/virus_subset.fa 2>/dev/null || echo 0)
echo "RUNTIME extract-virus $((END - START)) seconds"
echo "Virus FASTA: ${VSIZE}, ${VSEQS} sequences"

echo "=== Generate virus taxid map ==="
blastdbcmd -db ${DB_DIR}/core_nt \
    -taxidlist /tmp/virus_taxids.txt \
    -outfmt "%a %T" > /tmp/virus_taxmap.txt 2>/dev/null
echo "Taxmap: $(wc -l < /tmp/virus_taxmap.txt) entries"

echo "=== Build virus BLAST DB ==="
START=$(date +%s)
makeblastdb -in ${SUBSET_DIR}/virus_subset.fa \
    -dbtype nucl \
    -out ${SUBSET_DIR}/core_nt_virus \
    -title "core_nt virus subset (taxid 10239)" \
    -parse_seqids \
    -blastdb_version 5 \
    -taxid_map /tmp/virus_taxmap.txt \
    2>&1 | tail -5
END=$(date +%s)
echo "RUNTIME makeblastdb-virus $((END - START)) seconds"
blastdbcmd -db ${SUBSET_DIR}/core_nt_virus -info 2>&1 | head -3

echo "=== Upload virus DB to blob ==="
rm -f ${SUBSET_DIR}/virus_subset.fa
azcopy login --identity 2>/dev/null
START=$(date +%s)
azcopy cp "${SUBSET_DIR}/core_nt_virus.*" \
    "${BLOB}/core_nt_virus/" \
    --overwrite=ifSourceNewer \
    --block-size-mb=256 \
    --log-level=WARNING 2>&1 | tail -5
END=$(date +%s)
echo "RUNTIME upload-virus $((END - START)) seconds"
echo "VIRUS_BUILD_DONE"
    '
    log "Virus DB build + upload complete."
}

# ── Phase 0c: Build MegaBLAST index for pathogen ──
phase_build_pathogen_index() {
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  Phase 0c: Build MegaBLAST index for pathogen DB"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    local exists=$(az storage blob list --account-name "$STORAGE" --container-name blast-db \
    --prefix "core_nt_pathogen_indexed/" --query "length([?contains(name, '.nsq')])" -o tsv --auth-mode login 2>/dev/null)
    if [[ "$exists" -gt 0 ]]; then
        log "Pathogen indexed DB already on blob ($exists .nsq files). Skipping."
        return 0
    fi
    
    log "Building MegaBLAST index for pathogen DB on VM..."
    vm_run '#!/bin/bash
set -e
export BLASTDB=/blast/blastdb
SUBSET_DIR="/blast/subsets"
BLOB="https://stgelb.blob.core.windows.net/blast-db"

echo "=== Build MegaBLAST index ==="
START=$(date +%s)

# Copy pathogen DB to indexed dir
mkdir -p ${SUBSET_DIR}/indexed
cp ${SUBSET_DIR}/core_nt_pathogen.* ${SUBSET_DIR}/indexed/

# Build index for each volume
cd ${SUBSET_DIR}/indexed
for NAL_OR_NSQ in core_nt_pathogen.??.nsq; do
    VOL=$(basename "$NAL_OR_NSQ" .nsq)
    echo "  Indexing $VOL..."
    makembindex -input "$VOL" -iformat blastdb -old_style_index false 2>&1 | tail -1
done

END=$(date +%s)
echo "RUNTIME makembindex-pathogen $((END - START)) seconds"
echo "Index files: $(ls *.idx 2>/dev/null | wc -l)"

echo "=== Upload indexed DB to blob ==="
azcopy login --identity 2>/dev/null
START=$(date +%s)
azcopy cp "${SUBSET_DIR}/indexed/core_nt_pathogen.*" \
    "${BLOB}/core_nt_pathogen_indexed/" \
    --overwrite=ifSourceNewer \
    --block-size-mb=256 \
    --log-level=WARNING 2>&1 | tail -5
END=$(date +%s)
echo "RUNTIME upload-pathogen-indexed $((END - START)) seconds"
echo "INDEX_BUILD_DONE"
    '
    log "Pathogen MegaBLAST index build + upload complete."
}

# ── Enable storage public access ──
enable_storage_access() {
    log "Enabling storage public network access..."
    az storage account update -n "$STORAGE" -g "$RG" \
    --public-network-access Enabled \
    --default-action Allow \
    -o none 2>/dev/null || true
    sleep 5
}

# ── Main ──
main() {
    local mode="${1:-all}"
    
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  v3 Benchmark — Automated Runner"
    bold "  Mode: ${mode}"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Initialize summary
    echo "test_id,status,exit_code,elapsed_seconds" > "${RESULTS_DIR}/summary.csv"
    
    enable_storage_access
    
    # Phase 0: Prep DBs
    if [[ "$mode" == "all" || "$mode" == "prep" ]]; then
        phase_upload_pathogen
        phase_build_virus
        phase_build_pathogen_index
    fi
    
    if [[ "$mode" == "prep" ]]; then
        log "Prep complete. Run with 'run' to execute benchmarks."
        return 0
    fi
    
    # Phase 1: B2 Taxonomy Subset (pathogen tests)
    bold ""
    bold "  Phase 1: B2 — Taxonomy Subset"
    bold ""
    
    run_benchmark "B2-pathogen-E16-1N" "benchmark/configs/v3/b2_subset/B2-pathogen-E16-1N.ini"
    run_benchmark "B2-pathogen-E64-1N" "benchmark/configs/v3/b2_subset/B2-pathogen-E64-1N.ini"
    run_benchmark "B2-virus-E64-1N"    "benchmark/configs/v3/b2_subset/B2-virus-E64-1N.ini"
    
    # Phase 2: B3 MegaBLAST Index
    bold ""
    bold "  Phase 2: B3 — MegaBLAST Index"
    bold ""
    
    run_benchmark "B3-idx-pathogen-E16" "benchmark/configs/v3/b3_index/B3-idx-pathogen-E16.ini"
    
    # Phase 3: C3 Combined (subset + index)
    bold ""
    bold "  Phase 3: Combined Strategies"
    bold ""
    
    run_benchmark "C3-subset-idx" "benchmark/configs/v3/combined/C3-subset-idx.ini"
    
    # Summary
    bold ""
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  ALL BENCHMARKS COMPLETE"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log ""
    log "Summary:"
    cat "${RESULTS_DIR}/summary.csv"
    log ""
    log "Skipped tests (require additional prep):"
    log "  B2-broad-E64-1N:  core_nt_broad DB not built (Bacteria subset too large)"
    log "  B3-idx-E64-1N:    full core_nt index not built (269 GB + 160 GB index)"
    log "  C1-subset-shard5: db-partitions not supported in ElasticBLAST config"
    log "  C2-subset-shard3: db-partitions not supported in ElasticBLAST config"
}

main "$@"
BLOB="https://stgelb.blob.core.windows.net/blast-db"

azcopy login --identity 2>/dev/null
START=$(date +%s)

azcopy cp "${SUBSET_DIR}/core_nt_pathogen.*" \
    "${BLOB}/core_nt_pathogen/" \
    --overwrite=ifSourceNewer \
    --block-size-mb=256 \
    --log-level=WARNING 2>&1 | tail -5

END=$(date +%s)
echo "RUNTIME upload-pathogen $((END - START)) seconds"
echo "UPLOAD_PATHOGEN_DONE"
'
    log "Pathogen DB upload complete."
}

# ── Phase 0b: Build + upload virus DB ──
phase_build_virus() {
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  Phase 0b: Build + upload virus-only DB"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Check if already uploaded
    local exists=$(az storage blob list --account-name "$STORAGE" --container-name blast-db \
        --prefix "core_nt_virus/" --query "length([?contains(name, '.nsq')])" -o tsv --auth-mode login 2>/dev/null)
    if [[ "$exists" -gt 0 ]]; then
        log "Virus DB already on blob ($exists .nsq files). Skipping."
        return 0
    fi

    log "Building virus-only DB on VM (this takes ~40 min)..."
    vm_run '#!/bin/bash
set -e
export BLASTDB=/blast/blastdb
DB_DIR="/blast/blastdb"
SUBSET_DIR="/blast/subsets"
BLOB="https://stgelb.blob.core.windows.net/blast-db"
mkdir -p "$SUBSET_DIR"

echo "10239" > /tmp/virus_taxids.txt

# Delete pathogen FASTA to free space
rm -f ${SUBSET_DIR}/pathogen_subset.fa 2>/dev/null

echo "=== Extract virus subset FASTA ==="
START=$(date +%s)
blastdbcmd -db ${DB_DIR}/core_nt \
    -taxidlist /tmp/virus_taxids.txt \
    -out ${SUBSET_DIR}/virus_subset.fa 2>&1 | tail -3
END=$(date +%s)
VSIZE=$(du -sh ${SUBSET_DIR}/virus_subset.fa 2>/dev/null | cut -f1)
VSEQS=$(grep -c "^>" ${SUBSET_DIR}/virus_subset.fa 2>/dev/null || echo 0)
echo "RUNTIME extract-virus $((END - START)) seconds"
echo "Virus FASTA: ${VSIZE}, ${VSEQS} sequences"

echo "=== Generate virus taxid map ==="
blastdbcmd -db ${DB_DIR}/core_nt \
    -taxidlist /tmp/virus_taxids.txt \
    -outfmt "%a %T" > /tmp/virus_taxmap.txt 2>/dev/null
echo "Taxmap: $(wc -l < /tmp/virus_taxmap.txt) entries"

echo "=== Build virus BLAST DB ==="
START=$(date +%s)
makeblastdb -in ${SUBSET_DIR}/virus_subset.fa \
    -dbtype nucl \
    -out ${SUBSET_DIR}/core_nt_virus \
    -title "core_nt virus subset (taxid 10239)" \
    -parse_seqids \
    -blastdb_version 5 \
    -taxid_map /tmp/virus_taxmap.txt \
    2>&1 | tail -5
END=$(date +%s)
echo "RUNTIME makeblastdb-virus $((END - START)) seconds"
blastdbcmd -db ${SUBSET_DIR}/core_nt_virus -info 2>&1 | head -3

echo "=== Upload virus DB to blob ==="
rm -f ${SUBSET_DIR}/virus_subset.fa
azcopy login --identity 2>/dev/null
START=$(date +%s)
azcopy cp "${SUBSET_DIR}/core_nt_virus.*" \
    "${BLOB}/core_nt_virus/" \
    --overwrite=ifSourceNewer \
    --block-size-mb=256 \
    --log-level=WARNING 2>&1 | tail -5
END=$(date +%s)
echo "RUNTIME upload-virus $((END - START)) seconds"
echo "VIRUS_BUILD_DONE"
'
    log "Virus DB build + upload complete."
}

# ── Phase 0c: Build MegaBLAST index for pathogen ──
phase_build_pathogen_index() {
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  Phase 0c: Build MegaBLAST index for pathogen DB"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    local exists=$(az storage blob list --account-name "$STORAGE" --container-name blast-db \
        --prefix "core_nt_pathogen_indexed/" --query "length([?contains(name, '.nsq')])" -o tsv --auth-mode login 2>/dev/null)
    if [[ "$exists" -gt 0 ]]; then
        log "Pathogen indexed DB already on blob ($exists .nsq files). Skipping."
        return 0
    fi

    log "Building MegaBLAST index for pathogen DB on VM..."
    vm_run '#!/bin/bash
set -e
export BLASTDB=/blast/blastdb
SUBSET_DIR="/blast/subsets"
BLOB="https://stgelb.blob.core.windows.net/blast-db"

echo "=== Build MegaBLAST index ==="
START=$(date +%s)

# Copy pathogen DB to indexed dir
mkdir -p ${SUBSET_DIR}/indexed
cp ${SUBSET_DIR}/core_nt_pathogen.* ${SUBSET_DIR}/indexed/

# Build index for each volume
cd ${SUBSET_DIR}/indexed
for NAL_OR_NSQ in core_nt_pathogen.??.nsq; do
    VOL=$(basename "$NAL_OR_NSQ" .nsq)
    echo "  Indexing $VOL..."
    makembindex -input "$VOL" -iformat blastdb -old_style_index false 2>&1 | tail -1
done

END=$(date +%s)
echo "RUNTIME makembindex-pathogen $((END - START)) seconds"
echo "Index files: $(ls *.idx 2>/dev/null | wc -l)"

echo "=== Upload indexed DB to blob ==="
azcopy login --identity 2>/dev/null
START=$(date +%s)
azcopy cp "${SUBSET_DIR}/indexed/core_nt_pathogen.*" \
    "${BLOB}/core_nt_pathogen_indexed/" \
    --overwrite=ifSourceNewer \
    --block-size-mb=256 \
    --log-level=WARNING 2>&1 | tail -5
END=$(date +%s)
echo "RUNTIME upload-pathogen-indexed $((END - START)) seconds"
echo "INDEX_BUILD_DONE"
'
    log "Pathogen MegaBLAST index build + upload complete."
}

# ── Enable storage public access ──
enable_storage_access() {
    log "Enabling storage public network access..."
    az storage account update -n "$STORAGE" -g "$RG" \
        --public-network-access Enabled \
        --default-action Allow \
        -o none 2>/dev/null || true
    sleep 5
}

# ── Main ──
main() {
    local mode="${1:-all}"

    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  v3 Benchmark — Automated Runner"
    bold "  Mode: ${mode}"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Initialize summary
    echo "test_id,status,exit_code,elapsed_seconds" > "${RESULTS_DIR}/summary.csv"

    enable_storage_access

    # Phase 0: Prep DBs
    if [[ "$mode" == "all" || "$mode" == "prep" ]]; then
        phase_upload_pathogen
        phase_build_virus
        phase_build_pathogen_index
    fi

    if [[ "$mode" == "prep" ]]; then
        log "Prep complete. Run with 'run' to execute benchmarks."
        return 0
    fi

    # Phase 1: B2 Taxonomy Subset (pathogen tests)
    bold ""
    bold "  Phase 1: B2 — Taxonomy Subset"
    bold ""

    run_benchmark "B2-pathogen-E16-1N" "benchmark/configs/v3/b2_subset/B2-pathogen-E16-1N.ini"
    run_benchmark "B2-pathogen-E64-1N" "benchmark/configs/v3/b2_subset/B2-pathogen-E64-1N.ini"
    run_benchmark "B2-virus-E64-1N"    "benchmark/configs/v3/b2_subset/B2-virus-E64-1N.ini"

    # Phase 2: B3 MegaBLAST Index
    bold ""
    bold "  Phase 2: B3 — MegaBLAST Index"
    bold ""

    run_benchmark "B3-idx-pathogen-E16" "benchmark/configs/v3/b3_index/B3-idx-pathogen-E16.ini"

    # Phase 3: C3 Combined (subset + index)
    bold ""
    bold "  Phase 3: Combined Strategies"
    bold ""

    run_benchmark "C3-subset-idx" "benchmark/configs/v3/combined/C3-subset-idx.ini"

    # Summary
    bold ""
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    bold "  ALL BENCHMARKS COMPLETE"
    bold "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log ""
    log "Summary:"
    cat "${RESULTS_DIR}/summary.csv"
    log ""
    log "Skipped tests (require additional prep):"
    log "  B2-broad-E64-1N:  core_nt_broad DB not built (Bacteria subset too large)"
    log "  B3-idx-E64-1N:    full core_nt index not built (269 GB + 160 GB index)"
    log "  C1-subset-shard5: db-partitions not supported in ElasticBLAST config"
    log "  C2-subset-shard3: db-partitions not supported in ElasticBLAST config"
}

main "$@"
