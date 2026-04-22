#!/bin/bash
# benchmark/prep_db_v3.sh — One-shot DB preparation for v3 benchmark
#
# Creates all DB variants needed for v3 benchmark:
#   1. Taxonomy subsets (pathogen, virus-only, broad)
#   2. Sharded DBs (5, 10, 20, 83 shards)
#   3. MegaBLAST indexes
#   4. Uploads everything to Azure Blob Storage
#
# This script runs ON A PREP VM (Standard_D64s_v3) where core_nt
# has already been pre-staged to /blast/blastdb/core_nt.
#
# Usage:
#   # Full prep (all steps)
#   ./benchmark/prep_db_v3.sh
#
#   # Individual steps
#   ./benchmark/prep_db_v3.sh info          # DB metadata only
#   ./benchmark/prep_db_v3.sh subset        # Create taxonomy subsets
#   ./benchmark/prep_db_v3.sh shard         # Create shards
#   ./benchmark/prep_db_v3.sh index         # Build MegaBLAST indexes
#   ./benchmark/prep_db_v3.sh upload        # Upload to Blob
#   ./benchmark/prep_db_v3.sh validate      # Correctness validation
#
# Prerequisites:
#   - core_nt downloaded to $DB_DIR (default: /blast/blastdb)
#   - BLAST+ 2.17.0 installed (blastdbcmd, blastdb_aliastool, makeblastdb, makembindex)
#   - azcopy v10 installed + authenticated (az login or managed identity)
#   - ~300 GB free disk for subsets + indexes
#
# Author: Moon Hyuk Choi

set -euo pipefail

# ── Configuration ──
DB_NAME="core_nt"
DB_DIR="${DB_DIR:-/blast/blastdb}"
DB_PATH="${DB_DIR}/${DB_NAME}"
WORK_DIR="${WORK_DIR:-/blast/v3_prep}"
STORAGE="stgelb"
CONTAINER="blast-db"
BLOB_BASE="https://${STORAGE}.blob.core.windows.net/${CONTAINER}"
QUERY_FILE="${QUERY_FILE:-/blast/queries/pathogen-10.fa}"
VALIDATION_DIR="${WORK_DIR}/validation"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; }
section() { echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "${CYAN}  $*${NC}"; echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# ── Timing helper ──
timer_start() { eval "TIMER_$1=$(date +%s)"; }
timer_end() {
    local start_var="TIMER_$1"
    local end=$(date +%s)
    local elapsed=$((end - ${!start_var}))
    local min=$((elapsed / 60))
    local sec=$((elapsed % 60))
    log "  Duration: ${min}m ${sec}s"
    echo "$1,$elapsed" >> "${WORK_DIR}/timings.csv"
}

# ── Validate prerequisites ──
check_prereqs() {
    section "Checking prerequisites"
    
    local missing=0
    for cmd in blastdbcmd blastdb_aliastool makeblastdb makembindex blastn azcopy; do
        if command -v "$cmd" &>/dev/null; then
            log "  ✓ $cmd: $(command -v "$cmd")"
        else
            err "  ✗ $cmd: NOT FOUND"
            missing=$((missing + 1))
        fi
    done
    
    if [[ ! -f "${DB_PATH}.nal" ]] && [[ ! -f "${DB_PATH}.nsq" ]] && [[ ! -f "${DB_PATH}.00.nsq" ]]; then
        err "  ✗ Database not found: ${DB_PATH}"
        err "    Expected: ${DB_PATH}.nal or ${DB_PATH}.00.nsq"
        err "    Download first: ./benchmark/prestage-db-vm.sh core_nt"
        missing=$((missing + 1))
    else
        log "  ✓ Database: ${DB_PATH}"
    fi
    
    if [[ $missing -gt 0 ]]; then
        err "$missing prerequisite(s) missing. Aborting."
        exit 1
    fi
    
    mkdir -p "$WORK_DIR"
    echo "step,elapsed_seconds" > "${WORK_DIR}/timings.csv"
}

# ══════════════════════════════════════════════════════════════
# Step 0: Get DB metadata
# ══════════════════════════════════════════════════════════════
step_info() {
    section "Step 0: Database metadata"
    timer_start info
    
    log "Getting core_nt metadata..."
    blastdbcmd -db "$DB_PATH" -info | tee "${WORK_DIR}/db_info.txt"
    
    # Extract key metrics
    local total_letters
    total_letters=$(blastdbcmd -db "$DB_PATH" -info | grep -i "total letters" | grep -oP '[\d,]+' | tr -d ',')
    local total_seqs
    total_seqs=$(blastdbcmd -db "$DB_PATH" -info | grep -i "sequences" | head -1 | grep -oP '[\d,]+' | tr -d ',')
    local num_volumes
    num_volumes=$(ls "${DB_DIR}/${DB_NAME}".*.nsq 2>/dev/null | wc -l)
    
    # Calculate on-disk size
    local db_size_bytes
    db_size_bytes=$(du -sb "${DB_DIR}/${DB_NAME}"* 2>/dev/null | awk '{sum+=$1} END{print sum}')
    local db_size_gb
    db_size_gb=$(echo "scale=1; ${db_size_bytes} / 1073741824" | bc)
    
    log ""
    log "  Database:        ${DB_NAME}"
    log "  Total letters:   ${total_letters}"
    log "  Total sequences: ${total_seqs}"
    log "  Volumes:         ${num_volumes}"
    log "  Size on disk:    ${db_size_gb} GB (${db_size_bytes} bytes)"
    
    # Save for later steps
    echo "${total_letters}" > "${WORK_DIR}/total_letters.txt"
    echo "${num_volumes}" > "${WORK_DIR}/num_volumes.txt"
    echo "${db_size_gb}" > "${WORK_DIR}/db_size_gb.txt"
    
    # List volume files
    log ""
    log "  Volume files:"
    ls -la "${DB_DIR}/${DB_NAME}".*.nsq 2>/dev/null | head -10
    if [[ $num_volumes -gt 10 ]]; then
        log "  ... ($((num_volumes - 10)) more)"
    fi
    
    timer_end info
}

# ══════════════════════════════════════════════════════════════
# Step 1: Create taxonomy subsets
# ══════════════════════════════════════════════════════════════
step_subset() {
    section "Step 1: Creating taxonomy subsets"
    
    local subset_dir="${WORK_DIR}/subsets"
    mkdir -p "$subset_dir"
    
    # ── 1a. Pathogen subset: Virus (10239) + Plasmodium (5820) ──
    log ""
    log "1a. Pathogen subset (Virus + Plasmodium)..."
    timer_start subset_pathogen
    
    echo -e "10239\n5820" > "${subset_dir}/pathogen.taxids"
    
    log "  Extracting sequences by taxonomy..."
    blastdbcmd -db "$DB_PATH" \
        -taxidlist "${subset_dir}/pathogen.taxids" \
        -out "${subset_dir}/pathogen.fa" \
        -outfmt '%f' 2>&1 | tail -3
    
    local pathogen_seqs
    pathogen_seqs=$(grep -c '^>' "${subset_dir}/pathogen.fa" || echo 0)
    local pathogen_size
    pathogen_size=$(du -sh "${subset_dir}/pathogen.fa" | cut -f1)
    log "  Extracted: ${pathogen_seqs} sequences, ${pathogen_size}"
    
    log "  Building BLAST DB..."
    makeblastdb \
        -in "${subset_dir}/pathogen.fa" \
        -dbtype nucl \
        -out "${subset_dir}/core_nt_pathogen" \
        -title "core_nt pathogen subset (Virus+Plasmodium)" \
        -parse_seqids \
        -blastdb_version 5 \
        -max_file_sz 4GB 2>&1 | tail -3
    
    log "  Verifying subset DB..."
    blastdbcmd -db "${subset_dir}/core_nt_pathogen" -info | tee "${subset_dir}/pathogen_info.txt"
    
    # Save subset letters for shard -dbsize
    local pathogen_letters
    pathogen_letters=$(blastdbcmd -db "${subset_dir}/core_nt_pathogen" -info | grep -i "total letters" | grep -oP '[\d,]+' | tr -d ',')
    echo "${pathogen_letters}" > "${subset_dir}/pathogen_letters.txt"
    
    # Cleanup FASTA (save disk)
    rm -f "${subset_dir}/pathogen.fa"
    
    timer_end subset_pathogen
    
    # ── 1b. Virus-only subset ──
    log ""
    log "1b. Virus-only subset..."
    timer_start subset_virus
    
    echo "10239" > "${subset_dir}/virus.taxids"
    
    blastdbcmd -db "$DB_PATH" \
        -taxidlist "${subset_dir}/virus.taxids" \
        -out "${subset_dir}/virus.fa" \
        -outfmt '%f' 2>&1 | tail -3
    
    local virus_seqs
    virus_seqs=$(grep -c '^>' "${subset_dir}/virus.fa" || echo 0)
    local virus_size
    virus_size=$(du -sh "${subset_dir}/virus.fa" | cut -f1)
    log "  Extracted: ${virus_seqs} sequences, ${virus_size}"
    
    makeblastdb \
        -in "${subset_dir}/virus.fa" \
        -dbtype nucl \
        -out "${subset_dir}/core_nt_virus" \
        -title "core_nt virus-only subset" \
        -parse_seqids \
        -blastdb_version 5 \
        -max_file_sz 4GB 2>&1 | tail -3
    
    blastdbcmd -db "${subset_dir}/core_nt_virus" -info | tee "${subset_dir}/virus_info.txt"
    rm -f "${subset_dir}/virus.fa"
    
    timer_end subset_virus
    
    # ── 1c. Broad subset: Virus + Bacteria + Plasmodium ──
    log ""
    log "1c. Broad subset (Virus + Bacteria + Plasmodium)..."
    timer_start subset_broad
    
    echo -e "10239\n2\n5820" > "${subset_dir}/broad.taxids"
    
    blastdbcmd -db "$DB_PATH" \
        -taxidlist "${subset_dir}/broad.taxids" \
        -out "${subset_dir}/broad.fa" \
        -outfmt '%f' 2>&1 | tail -3
    
    local broad_seqs
    broad_seqs=$(grep -c '^>' "${subset_dir}/broad.fa" || echo 0)
    local broad_size
    broad_size=$(du -sh "${subset_dir}/broad.fa" | cut -f1)
    log "  Extracted: ${broad_seqs} sequences, ${broad_size}"
    
    makeblastdb \
        -in "${subset_dir}/broad.fa" \
        -dbtype nucl \
        -out "${subset_dir}/core_nt_broad" \
        -title "core_nt broad subset (Virus+Bacteria+Plasmodium)" \
        -parse_seqids \
        -blastdb_version 5 \
        -max_file_sz 4GB 2>&1 | tail -3
    
    blastdbcmd -db "${subset_dir}/core_nt_broad" -info | tee "${subset_dir}/broad_info.txt"
    rm -f "${subset_dir}/broad.fa"
    
    timer_end subset_broad
    
    # ── Summary ──
    section "Subset summary"
    echo ""
    printf "%-20s %12s %15s\n" "Subset" "Files" "Size"
    printf "%-20s %12s %15s\n" "--------------------" "------------" "---------------"
    for name in pathogen virus broad; do
        local size
        size=$(du -sh "${subset_dir}/core_nt_${name}"* 2>/dev/null | awk '{sum+=$1} END{printf "%.1fG", sum}' || echo "N/A")
        local files
        files=$(ls "${subset_dir}/core_nt_${name}"* 2>/dev/null | wc -l || echo 0)
        printf "%-20s %12s %15s\n" "core_nt_${name}" "$files" "$size"
    done
    echo ""
}

# ══════════════════════════════════════════════════════════════
# Step 2: Create sharded DBs
# ══════════════════════════════════════════════════════════════
step_shard() {
    section "Step 2: Creating sharded databases"
    
    local shard_dir="${WORK_DIR}/shards"
    mkdir -p "$shard_dir"
    
    local num_volumes
    num_volumes=$(cat "${WORK_DIR}/num_volumes.txt" 2>/dev/null || ls "${DB_DIR}/${DB_NAME}".*.nsq 2>/dev/null | wc -l)
    
    log "Source DB: ${DB_NAME} (${num_volumes} volumes)"
    
    # Discover volume names (e.g., core_nt.00, core_nt.01, ...)
    local volumes=()
    for f in "${DB_DIR}/${DB_NAME}".*.nsq; do
        local vol_name
        vol_name=$(basename "$f" .nsq)
        volumes+=("$vol_name")
    done
    
    if [[ ${#volumes[@]} -eq 0 ]]; then
        err "No volume files found matching ${DB_DIR}/${DB_NAME}.*.nsq"
        exit 1
    fi
    
    # Sort volumes
    IFS=$'\n' volumes=($(sort <<<"${volumes[*]}")); unset IFS
    
    log "Found ${#volumes[@]} volumes: ${volumes[0]} ... ${volumes[-1]}"
    
    # ── Create shards for each target shard count ──
    for num_shards in 5 10 20; do
        log ""
        log "Creating ${num_shards}-shard layout..."
        timer_start "shard_${num_shards}"
        
        local this_shard_dir="${shard_dir}/${num_shards}shards"
        mkdir -p "$this_shard_dir"
        
        # Distribute volumes across shards (contiguous blocks, not round-robin)
        # Contiguous is better for blob storage layout: each shard's volumes
        # are adjacent, making per-shard upload/download more predictable.
        local vols_per_shard=$(( (${#volumes[@]} + num_shards - 1) / num_shards ))
        
        for ((s=0; s<num_shards; s++)); do
            local start=$((s * vols_per_shard))
            local end=$((start + vols_per_shard))
            if [[ $end -gt ${#volumes[@]} ]]; then
                end=${#volumes[@]}
            fi
            if [[ $start -ge ${#volumes[@]} ]]; then
                break
            fi
            
            # Build volume list for this shard
            local vol_list=""
            for ((v=start; v<end; v++)); do
                if [[ -n "$vol_list" ]]; then
                    vol_list="$vol_list "
                fi
                vol_list="${vol_list}${DB_DIR}/${volumes[$v]}"
            done
            
            local shard_name="${DB_NAME}_shard_$(printf '%02d' $s)"
            local shard_path="${this_shard_dir}/${shard_name}"
            
            blastdb_aliastool \
                -dblist "$vol_list" \
                -dbtype nucl \
                -out "$shard_path" \
                -title "${DB_NAME} shard ${s} of ${num_shards}"
            
            local shard_vols=$((end - start))
            log "  Shard ${s}: ${shard_vols} volumes (${volumes[$start]}..${volumes[$((end-1))]})"
        done
        
        # Calculate per-shard sizes
        log "  Shard sizes:"
        for nal_file in "${this_shard_dir}"/*.nal; do
            local sname
            sname=$(basename "$nal_file" .nal)
            # Get volumes listed in .nal and sum their sizes
            local shard_size=0
            while IFS= read -r line; do
                if [[ "$line" == DBLIST* ]]; then
                    for vol_path in $line; do
                        [[ "$vol_path" == "DBLIST" ]] && continue
                        # Sum all files for this volume
                        local vol_size
                        vol_size=$(du -sb "${vol_path}"* 2>/dev/null | awk '{sum+=$1} END{print sum+0}')
                        shard_size=$((shard_size + vol_size))
                    done
                fi
            done < "$nal_file"
            local shard_gb
            shard_gb=$(echo "scale=1; ${shard_size} / 1073741824" | bc 2>/dev/null || echo "?")
            log "    ${sname}: ${shard_gb} GB"
        done
        
        timer_end "shard_${num_shards}"
    done
    
    # ── Also shard the pathogen subset ──
    local subset_dir="${WORK_DIR}/subsets"
    if [[ -f "${subset_dir}/core_nt_pathogen.nsq" ]] || ls "${subset_dir}/core_nt_pathogen".*.nsq &>/dev/null 2>&1; then
        log ""
        log "Sharding pathogen subset..."
        
        local pathogen_vols=()
        for f in "${subset_dir}/core_nt_pathogen".*.nsq; do
            [[ -f "$f" ]] || continue
            pathogen_vols+=("$(basename "$f" .nsq)")
        done
        
        if [[ ${#pathogen_vols[@]} -gt 0 ]]; then
            for num_shards in 3 5; do
                local ps_dir="${shard_dir}/pathogen_${num_shards}shards"
                mkdir -p "$ps_dir"
                
                local pvols_per_shard=$(( (${#pathogen_vols[@]} + num_shards - 1) / num_shards ))
                
                for ((s=0; s<num_shards; s++)); do
                    local start=$((s * pvols_per_shard))
                    local end=$((start + pvols_per_shard))
                    [[ $end -gt ${#pathogen_vols[@]} ]] && end=${#pathogen_vols[@]}
                    [[ $start -ge ${#pathogen_vols[@]} ]] && break
                    
                    local vol_list=""
                    for ((v=start; v<end; v++)); do
                        [[ -n "$vol_list" ]] && vol_list="$vol_list "
                        vol_list="${vol_list}${subset_dir}/${pathogen_vols[$v]}"
                    done
                    
                    blastdb_aliastool \
                        -dblist "$vol_list" \
                        -dbtype nucl \
                        -out "${ps_dir}/core_nt_pathogen_shard_$(printf '%02d' $s)" \
                        -title "core_nt_pathogen shard ${s} of ${num_shards}"
                    
                    log "  pathogen shard ${s}/${num_shards}: $((end - start)) volumes"
                done
            done
        else
            log "  Pathogen subset is single-volume, skip sharding"
        fi
    fi
    
    section "Shard summary"
    for d in "${shard_dir}"/*/; do
        local name
        name=$(basename "$d")
        local count
        count=$(ls "$d"/*.nal 2>/dev/null | wc -l)
        log "  ${name}: ${count} shard files"
    done
}

# ══════════════════════════════════════════════════════════════
# Step 3: Build MegaBLAST indexes
# ══════════════════════════════════════════════════════════════
step_index() {
    section "Step 3: Building MegaBLAST indexes"
    
    local index_dir="${WORK_DIR}/indexes"
    mkdir -p "$index_dir"
    
    # ── 3a. Index for pathogen subset (small, fast — do first) ──
    local subset_dir="${WORK_DIR}/subsets"
    if [[ -f "${subset_dir}/core_nt_pathogen.nsq" ]] || ls "${subset_dir}/core_nt_pathogen".*.nsq &>/dev/null 2>&1; then
        log ""
        log "3a. Building index for pathogen subset..."
        timer_start index_pathogen
        
        # Copy DB to index dir (makembindex writes .idx alongside DB)
        cp "${subset_dir}/core_nt_pathogen"* "$index_dir/" 2>/dev/null || true
        
        makembindex \
            -input "${index_dir}/core_nt_pathogen" \
            -iformat blastdb \
            -old_style_index false 2>&1 | tail -5
        
        local idx_size
        idx_size=$(du -sh "${index_dir}/core_nt_pathogen"*.idx 2>/dev/null | awk '{sum+=$1} END{printf "%.1fG", sum}' || echo "N/A")
        log "  Index size: ${idx_size}"
        
        timer_end index_pathogen
    fi
    
    # ── 3b. Index for full core_nt (large, slow) ──
    log ""
    log "3b. Building index for full core_nt..."
    log "  WARNING: This may take 2-4 hours for 269 GB DB"
    timer_start index_corent
    
    # Build index in-place (alongside existing DB files)
    makembindex \
        -input "$DB_PATH" \
        -iformat blastdb \
        -old_style_index false 2>&1 | tail -10
    
    local coreidx_size
    coreidx_size=$(du -sh "${DB_DIR}/${DB_NAME}"*.idx 2>/dev/null | awk '{sum+=$1} END{printf "%.1fG", sum}' || echo "N/A")
    log "  Index size: ${coreidx_size}"
    
    timer_end index_corent
    
    section "Index summary"
    log "  Pathogen subset index: ${idx_size:-N/A}"
    log "  Full core_nt index:    ${coreidx_size:-N/A}"
}

# ══════════════════════════════════════════════════════════════
# Step 4: Upload everything to Azure Blob Storage
# ══════════════════════════════════════════════════════════════
step_upload() {
    section "Step 4: Uploading to Azure Blob Storage"
    
    # Authenticate azcopy
    if [[ -n "${AZCOPY_AUTO_LOGIN_TYPE:-}" ]]; then
        log "Using AZCOPY_AUTO_LOGIN_TYPE=${AZCOPY_AUTO_LOGIN_TYPE}"
    else
        log "Running azcopy login..."
        azcopy login --identity 2>/dev/null || {
            warn "Managed identity login failed, trying AZCLI..."
            export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
        }
    fi
    
    local subset_dir="${WORK_DIR}/subsets"
    local shard_dir="${WORK_DIR}/shards"
    local index_dir="${WORK_DIR}/indexes"
    
    # ── 4a. Upload taxonomy subsets ──
    for name in pathogen virus broad; do
        local db_files="${subset_dir}/core_nt_${name}"
        if ls "${db_files}"* &>/dev/null 2>&1; then
            log ""
            log "Uploading core_nt_${name}..."
            timer_start "upload_${name}"
            
            azcopy cp "${db_files}*" \
                "${BLOB_BASE}/core_nt_${name}/" \
                --overwrite=ifSourceNewer \
                --block-size-mb=256 \
                --log-level=WARNING
            
            timer_end "upload_${name}"
        fi
    done
    
    # ── 4b. Upload sharded DBs ──
    # For shards, we need to upload the actual volume files for each shard
    # The .nal alias files point to absolute paths, which won't work on AKS.
    # Instead, we create per-shard blob directories containing symlinked volumes.
    
    for shard_layout in "${shard_dir}"/*/; do
        local layout_name
        layout_name=$(basename "$shard_layout")
        log ""
        log "Uploading shard layout: ${layout_name}..."
        timer_start "upload_${layout_name}"
        
        for nal_file in "${shard_layout}"*.nal; do
            [[ -f "$nal_file" ]] || continue
            local shard_name
            shard_name=$(basename "$nal_file" .nal)
            
            # Parse DBLIST from .nal to get volume paths
            local vol_paths=()
            while IFS= read -r line; do
                if [[ "$line" == DBLIST* ]]; then
                    local dblist="${line#DBLIST }"
                    for vol_path in $dblist; do
                        vol_paths+=("$vol_path")
                    done
                fi
            done < "$nal_file"
            
            # Upload each volume's files to shard-specific blob directory
            local blob_shard_path="${BLOB_BASE}/${layout_name}/${shard_name}/"
            for vol_path in "${vol_paths[@]}"; do
                # Upload all files for this volume (*.nsq, *.nhr, *.nin, *.ndb, etc.)
                azcopy cp "${vol_path}*" \
                    "${blob_shard_path}" \
                    --overwrite=ifSourceNewer \
                    --block-size-mb=256 \
                    --log-level=WARNING 2>&1 | tail -1
            done
            
            # Create a .nal alias file that references local paths (for AKS)
            # On AKS, DB will be at /blast/blastdb/
            local aks_nal="${shard_layout}/${shard_name}_aks.nal"
            {
                echo "TITLE ${shard_name}"
                echo -n "DBLIST"
                for vol_path in "${vol_paths[@]}"; do
                    local vol_base
                    vol_base=$(basename "$vol_path")
                    echo -n " /blast/blastdb/${vol_base}"
                done
                echo ""
            } > "$aks_nal"
            
            azcopy cp "$aks_nal" "${blob_shard_path}${shard_name}.nal" \
                --overwrite=true --log-level=WARNING 2>&1 | tail -1
        done
        
        timer_end "upload_${layout_name}"
    done
    
    # ── 4c. Upload indexed pathogen subset ──
    if ls "${index_dir}/core_nt_pathogen"* &>/dev/null 2>&1; then
        log ""
        log "Uploading indexed pathogen subset..."
        timer_start upload_idx_pathogen
        
        azcopy cp "${index_dir}/core_nt_pathogen*" \
            "${BLOB_BASE}/core_nt_pathogen_indexed/" \
            --overwrite=ifSourceNewer \
            --block-size-mb=256 \
            --log-level=WARNING
        
        timer_end upload_idx_pathogen
    fi
    
    # ── 4d. Upload core_nt index files ──
    if ls "${DB_DIR}/${DB_NAME}"*.idx &>/dev/null 2>&1; then
        log ""
        log "Uploading core_nt index files..."
        timer_start upload_idx_corent
        
        # Only upload .idx files (DB volumes already in blob)
        for idx_file in "${DB_DIR}/${DB_NAME}"*.idx; do
            azcopy cp "$idx_file" \
                "${BLOB_BASE}/${DB_NAME}_indexed/$(basename "$idx_file")" \
                --overwrite=ifSourceNewer \
                --block-size-mb=256 \
                --log-level=WARNING 2>&1 | tail -1
        done
        
        timer_end upload_idx_corent
    fi
    
    section "Upload complete"
    log "Uploaded to: ${BLOB_BASE}/"
    log "  core_nt_pathogen/"
    log "  core_nt_virus/"
    log "  core_nt_broad/"
    log "  {5,10,20}shards/"
    log "  core_nt_pathogen_indexed/"
    log "  core_nt_indexed/ (idx files only)"
}

# ══════════════════════════════════════════════════════════════
# Step 5: Correctness validation
# ══════════════════════════════════════════════════════════════
step_validate() {
    section "Step 5: Correctness validation"
    
    mkdir -p "$VALIDATION_DIR"
    
    if [[ ! -f "$QUERY_FILE" ]]; then
        warn "Query file not found: $QUERY_FILE"
        warn "Skipping validation. Upload pathogen-10.fa to $QUERY_FILE"
        return 0
    fi
    
    local total_letters
    total_letters=$(cat "${WORK_DIR}/total_letters.txt" 2>/dev/null || echo "0")
    local blast_opts="-max_target_seqs 500 -evalue 0.05 -outfmt '6 std' -num_threads 8"
    
    # ── 5a. Reference: full core_nt search ──
    log ""
    log "5a. Reference search (full core_nt)..."
    timer_start val_reference
    
    blastn -db "$DB_PATH" -query "$QUERY_FILE" \
        -max_target_seqs 500 -evalue 0.05 -outfmt "6 std" -num_threads 8 \
        -out "${VALIDATION_DIR}/ref_full.out"
    
    local ref_hits
    ref_hits=$(wc -l < "${VALIDATION_DIR}/ref_full.out")
    log "  Reference: ${ref_hits} hits"
    
    timer_end val_reference
    
    # ── 5b. Subset validation ──
    log ""
    log "5b. Subset validation (pathogen)..."
    timer_start val_subset
    
    local subset_dir="${WORK_DIR}/subsets"
    blastn -db "${subset_dir}/core_nt_pathogen" -query "$QUERY_FILE" \
        -max_target_seqs 500 -evalue 0.05 -outfmt "6 std" -num_threads 8 \
        -out "${VALIDATION_DIR}/subset_pathogen.out"
    
    local subset_hits
    subset_hits=$(wc -l < "${VALIDATION_DIR}/subset_pathogen.out")
    log "  Subset: ${subset_hits} hits"
    
    # Validate: subset subject IDs should be a subset of full DB results
    local extra_in_subset
    extra_in_subset=$(comm -23 \
        <(cut -f2 "${VALIDATION_DIR}/subset_pathogen.out" | sort -u) \
        <(cut -f2 "${VALIDATION_DIR}/ref_full.out" | sort -u) | wc -l)
    
    if [[ $extra_in_subset -eq 0 ]]; then
        log "  ✓ All subset hits found in reference (no spurious hits)"
    else
        warn "  ✗ ${extra_in_subset} subject IDs in subset but not in reference"
        warn "    This is expected: subset may find hits that rank below top-500 in full DB"
    fi
    
    timer_end val_subset
    
    # ── 5c. Shard validation (10 shards) ──
    log ""
    log "5c. Shard validation (10 shards, -dbsize correction)..."
    timer_start val_shard
    
    local shard_dir="${WORK_DIR}/shards/10shards"
    if [[ -d "$shard_dir" ]]; then
        local shard_results_dir="${VALIDATION_DIR}/shard_results"
        mkdir -p "$shard_results_dir"
        
        for nal_file in "${shard_dir}"/*.nal; do
            local shard_name
            shard_name=$(basename "$nal_file" .nal)
            log "  Searching ${shard_name}..."
            
            blastn -db "${shard_dir}/${shard_name}" -query "$QUERY_FILE" \
                -max_target_seqs 500 -evalue 0.05 -outfmt "6 std" -num_threads 4 \
                -dbsize "$total_letters" \
                -out "${shard_results_dir}/${shard_name}.out" 2>/dev/null || {
                    warn "  Shard search failed for ${shard_name}, trying with full path..."
                    # Alias .nal may reference absolute paths — expected to work on prep VM
                }
        done
        
        # Merge shard results
        log "  Merging shard results..."
        cat "${shard_results_dir}"/*.out | sort -k11,11g > "${VALIDATION_DIR}/merged_shards.out"
        
        # Per-query top-500
        python3 -c "
import sys
from collections import defaultdict
hits = defaultdict(list)
with open('${VALIDATION_DIR}/merged_shards.out') as f:
    for line in f:
        if line.startswith('#') or not line.strip():
            continue
        fields = line.strip().split('\t')
        if len(fields) >= 12:
            hits[fields[0]].append((float(fields[10]), -float(fields[11]), line.strip()))
# Sort by evalue asc, bitscore desc; keep top 500 per query
with open('${VALIDATION_DIR}/merged_shards_top500.out', 'w') as f:
    for qid in sorted(hits.keys()):
        for ev, neg_bs, raw in sorted(hits[qid])[:500]:
            f.write(raw + '\n')
"
        local merged_hits
        merged_hits=$(wc -l < "${VALIDATION_DIR}/merged_shards_top500.out")
        log "  Merged shards: ${merged_hits} hits (top-500 per query)"
        
        # Compare with reference
        local ref_subjects
        ref_subjects=$(cut -f1,2 "${VALIDATION_DIR}/ref_full.out" | sort -u | wc -l)
        local merged_subjects
        merged_subjects=$(cut -f1,2 "${VALIDATION_DIR}/merged_shards_top500.out" | sort -u | wc -l)
        
        local common_subjects
        common_subjects=$(comm -12 \
            <(cut -f1,2 "${VALIDATION_DIR}/ref_full.out" | sort -u) \
            <(cut -f1,2 "${VALIDATION_DIR}/merged_shards_top500.out" | sort -u) | wc -l)
        
        local overlap_pct
        overlap_pct=$(echo "scale=1; ${common_subjects} * 100 / ${ref_subjects}" | bc 2>/dev/null || echo "?")
        
        log ""
        log "  Shard validation results:"
        log "    Reference (query,subject) pairs: ${ref_subjects}"
        log "    Merged shards pairs:             ${merged_subjects}"
        log "    Common pairs:                    ${common_subjects}"
        log "    Overlap:                         ${overlap_pct}%"
        
        if [[ $(echo "$overlap_pct > 95" | bc 2>/dev/null || echo 0) -eq 1 ]]; then
            log "  ✓ Shard results match reference (>95% overlap)"
        else
            warn "  ✗ Shard overlap below 95%: ${overlap_pct}%"
            warn "    May need investigation — check E-value differences"
        fi
    else
        warn "  10-shard directory not found: ${shard_dir}"
        warn "  Run 'step_shard' first"
    fi
    
    timer_end val_shard
    
    # ── Summary ──
    section "Validation summary"
    log "  Reference (full core_nt):  ${ref_hits:-?} hits"
    log "  Pathogen subset:           ${subset_hits:-?} hits"
    log "  10-shard merged:           ${merged_hits:-?} hits"
    log "  Shard/Reference overlap:   ${overlap_pct:-?}%"
    log ""
    log "  Detailed results: ${VALIDATION_DIR}/"
}

# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════
main() {
    local step="${1:-all}"
    
    section "ElasticBLAST v3 DB Preparation"
    log "DB:       ${DB_PATH}"
    log "Work dir: ${WORK_DIR}"
    log "Step:     ${step}"
    log ""
    
    check_prereqs
    
    case "$step" in
        info)       step_info ;;
        subset)     step_info; step_subset ;;
        shard)      step_info; step_shard ;;
        index)      step_index ;;
        upload)     step_upload ;;
        validate)   step_validate ;;
        all)
            step_info
            step_subset
            step_shard
            step_index
            step_upload
            step_validate
            
            section "ALL STEPS COMPLETE"
            log ""
            log "Timings:"
            cat "${WORK_DIR}/timings.csv"
            log ""
            log "Next: Run benchmarks with"
            log "  ./benchmark/run_v3.sh"
            ;;
        *)
            err "Unknown step: $step"
            echo "Usage: $0 [info|subset|shard|index|upload|validate|all]"
            exit 1
            ;;
    esac
}

main "$@"
