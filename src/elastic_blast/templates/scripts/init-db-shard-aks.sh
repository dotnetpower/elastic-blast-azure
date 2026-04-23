#!/bin/bash
# init-db-shard-aks.sh — Download a single DB shard to local SSD
#
# Each shard is defined by a manifest file listing volume names.
# This script downloads the manifest, then fetches each volume's files
# from the main DB directory on blob storage.
#
# Environment variables (set by K8s pod spec):
#   ELB_SHARD_IDX       - Shard index (zero-padded: 00, 01, ...)
#   ELB_PARTITION_PREFIX - Blob URL prefix for shard directories
#   ELB_DB              - BLAST database name (shard-specific, e.g. core_nt_shard_00)
#   ELB_DB_MOL_TYPE     - Database molecule type (nucl/prot)
#   STARTUP_DELAY       - Optional delay in seconds

set -o pipefail

echo "BASH version ${BASH_VERSION}"
echo "Shard download: idx=${ELB_SHARD_IDX} prefix=${ELB_PARTITION_PREFIX} db=${ELB_DB}"

if [ -n "$STARTUP_DELAY" ]; then
    echo "Waiting ${STARTUP_DELAY}s for workspace initialization"
    sleep "$STARTUP_DELAY"
fi

start=$(date +%s)

log() {
    local ts
    ts=$(date +'%F %T')
    printf '%s RUNTIME %s %f seconds\n' "$ts" "$1" "$2"
}

azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }

export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY_VALUE:-64}
export AZCOPY_BUFFER_GB=${AZCOPY_BUFFER_GB:-4}

retry_azcopy() {
    local max_attempts=3 attempt=1 wait_sec=5
    while [ $attempt -le $max_attempts ]; do
        if azcopy "$@"; then return 0; fi
        echo "azcopy attempt $attempt/$max_attempts failed, retrying in ${wait_sec}s..."
        sleep $wait_sec; wait_sec=$((wait_sec * 2)); attempt=$((attempt + 1))
    done
    echo "ERROR: azcopy failed after $max_attempts attempts"; return 1
}

# Step 1: Download manifest and .nal alias to get volume list
SHARD_URL="${ELB_PARTITION_PREFIX}${ELB_SHARD_IDX}/"
MANIFEST_URL="${SHARD_URL}${ELB_DB}.manifest"
NAL_URL="${SHARD_URL}${ELB_DB}.nal"
echo "Downloading manifest: ${MANIFEST_URL}"
retry_azcopy cp "${MANIFEST_URL}" /tmp/manifest.txt --log-level=ERROR || {
    echo "ERROR: manifest download failed"
    exit 1
}
# Download .nal alias file (points BLAST to the volume files)
retry_azcopy cp "${NAL_URL}" "./${ELB_DB}.nal" --log-level=ERROR || true
VOLUMES=$(cat /tmp/manifest.txt)
echo "Volumes: ${VOLUMES}"

# Step 2: Derive base DB URL (strip shard part from prefix)
# e.g. https://.../blast-db/10shards/core_nt_shard_ → https://.../blast-db/
DB_BASE_URL=$(echo "${ELB_PARTITION_PREFIX}" | sed 's|/[^/]*/[^/]*$|/|')
# Find the original DB name (strip shard suffix: core_nt_shard_00 → core_nt)
ORIG_DB=$(echo "${ELB_DB}" | sed 's/_shard_[0-9]*$//')
DB_URL="${DB_BASE_URL}${ORIG_DB}/"
echo "DB base URL: ${DB_URL}"

# Step 3: Download volume files flat using --include-pattern (single azcopy job)
# Build semicolon-separated include pattern for all volumes + taxonomy files
PATTERN=""
for VOL in $VOLUMES; do
    [ -n "$PATTERN" ] && PATTERN="${PATTERN};"
    PATTERN="${PATTERN}${VOL}.*"
done
PATTERN="${PATTERN};taxdb.btd;taxdb.bti;${ORIG_DB}.ndb;${ORIG_DB}.ntf;${ORIG_DB}.nto"
echo "Downloading with pattern: ${PATTERN}"

# Use trailing /* to enable wildcard matching, --flat to prevent subdirectory creation
retry_azcopy cp "${DB_URL}*" . \
    --include-pattern "${PATTERN}" \
    --block-size-mb=256 \
    --log-level=WARNING || exit 1

end=$(date +%s)
log "download-shard-${ELB_SHARD_IDX}" $((end - start))

echo "DB files downloaded: $(ls *.nsq 2>/dev/null | wc -l) .nsq files"
echo "Total size: $(du -sh . 2>/dev/null | cut -f1)"

# Build volume path string for BLAST -db (space-separated, bypasses .nal)
VOLPATHS=""
for VOL in $VOLUMES; do
    [ -n "$VOLPATHS" ] && VOLPATHS="$VOLPATHS "
    VOLPATHS="${VOLPATHS}$(pwd)/${VOL}"
done
echo "VOLPATHS=${VOLPATHS}" > /tmp/shard_volpaths.txt
echo "Volume paths: ${VOLPATHS}"

# Clean up azcopy processes
pkill -f azcopy 2>/dev/null || true
rm -rf /root/.azcopy 2>/dev/null || true

exit 0

# ── Install BLAST+ and azcopy ──
install_tools() {
    log "Installing BLAST+ and azcopy..."

    # BLAST+ from NCBI
    if ! command -v blastn &>/dev/null; then
        log "  Installing BLAST+ 2.17.0..."
        cd /tmp
        wget -q "https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/2.17.0/ncbi-blast-2.17.0+-x64-linux.tar.gz"
        tar xzf ncbi-blast-2.17.0+-x64-linux.tar.gz
        cp ncbi-blast-2.17.0+/bin/* /usr/local/bin/
        rm -rf ncbi-blast-2.17.0+*
        log "  BLAST+ installed: $(blastn -version | head -1)"
    fi

    # azcopy
    if ! command -v azcopy &>/dev/null; then
        log "  Installing azcopy..."
        cd /tmp
        wget -q "https://aka.ms/downloadazcopy-v10-linux" -O azcopy.tar.gz
        tar xzf azcopy.tar.gz --strip-components=1
        mv azcopy /usr/local/bin/
        rm -f azcopy.tar.gz
        log "  azcopy installed: $(azcopy --version)"
    fi

    # Azure CLI (for managed identity auth)
    if ! command -v az &>/dev/null; then
        log "  Installing Azure CLI..."
        curl -sL https://aka.ms/InstallAzureCLIDeb | bash
    fi
}

# ── Authenticate ──
setup_auth() {
    log "Setting up authentication..."
    # Use managed identity
    az login --identity --allow-no-subscriptions 2>/dev/null || {
        err "Managed identity login failed. Assign identity to VM."
        exit 1
    }
    azcopy login --identity || {
        err "azcopy identity login failed."
        exit 1
    }
    log "  Auth OK."
}

# ── Download core_nt from blob ──
download_db() {
    log "Downloading core_nt from Blob Storage..."
    mkdir -p "$DB_DIR"

    local count
    count=$(ls "${DB_DIR}/${DB_NAME}".*.nsq 2>/dev/null | wc -l)
    if [[ $count -gt 50 ]]; then
        log "  core_nt already downloaded (${count} volume files). Skipping."
        return 0
    fi

    local start=$(date +%s)
    azcopy cp "${BLOB_BASE}/${DB_NAME}/*" "${DB_DIR}/" \
        --recursive --block-size-mb=256 \
        --log-level=WARNING
    local end=$(date +%s)
    log "  Downloaded in $((end - start)) seconds"

    # Verify
    count=$(ls "${DB_DIR}/${DB_NAME}".*.nsq 2>/dev/null | wc -l)
    log "  Volume files: ${count}"

    # Quick sanity check
    blastdbcmd -db "${DB_DIR}/${DB_NAME}" -info | head -5
}

# ── Download pathogen query for validation ──
download_queries() {
    mkdir -p "$QUERY_DIR"
    if [[ ! -f "${QUERY_DIR}/pathogen-10.fa" ]]; then
        azcopy cp "https://${STORAGE}.blob.core.windows.net/queries/pathogen-10.fa" \
            "${QUERY_DIR}/pathogen-10.fa" --log-level=WARNING
    fi
}

# ── Step 1: Create taxonomy subsets ──
step_subset() {
    log ""
    log "━━━ Creating taxonomy subsets ━━━"

    local subset_dir="${WORK_DIR}/subsets"
    mkdir -p "$subset_dir"

    # Get total letters for reference
    local total_letters
    total_letters=$(blastdbcmd -db "${DB_DIR}/${DB_NAME}" -info | grep -i "total letters" | grep -oP '[\d,]+' | tr -d ',')
    echo "$total_letters" > "${WORK_DIR}/total_letters.txt"
    log "  Total letters: $total_letters"

    # ── Pathogen subset (Virus 10239 + Plasmodium 5820) ──
    log ""
    log "  [1/3] Pathogen subset (Virus + Plasmodium)..."
    local start=$(date +%s)

    echo -e "10239\n5820" > "${subset_dir}/pathogen.taxids"

    if [[ ! -f "${subset_dir}/core_nt_pathogen.nsq" ]] && ! ls "${subset_dir}/core_nt_pathogen".*.nsq &>/dev/null 2>&1; then
        blastdbcmd -db "${DB_DIR}/${DB_NAME}" \
            -taxidlist "${subset_dir}/pathogen.taxids" \
            -out "${subset_dir}/pathogen.fa" \
            -outfmt '%f' 2>&1 | tail -3

        local pathogen_seqs=$(grep -c '^>' "${subset_dir}/pathogen.fa" || echo 0)
        local pathogen_size=$(du -sh "${subset_dir}/pathogen.fa" | cut -f1)
        log "    Extracted: ${pathogen_seqs} sequences, ${pathogen_size}"

        makeblastdb \
            -in "${subset_dir}/pathogen.fa" \
            -dbtype nucl \
            -out "${subset_dir}/core_nt_pathogen" \
            -title "core_nt pathogen subset (Virus+Plasmodium)" \
            -parse_seqids \
            -blastdb_version 5 \
            -max_file_sz 4GB 2>&1 | tail -3

        rm -f "${subset_dir}/pathogen.fa"
    else
        log "    Already exists, skipping."
    fi

    blastdbcmd -db "${subset_dir}/core_nt_pathogen" -info 2>&1 | head -5
    local end=$(date +%s)
    log "    Duration: $((end - start))s"

    # ── Virus-only subset ──
    log ""
    log "  [2/3] Virus-only subset..."
    start=$(date +%s)

    echo "10239" > "${subset_dir}/virus.taxids"

    if [[ ! -f "${subset_dir}/core_nt_virus.nsq" ]] && ! ls "${subset_dir}/core_nt_virus".*.nsq &>/dev/null 2>&1; then
        blastdbcmd -db "${DB_DIR}/${DB_NAME}" \
            -taxidlist "${subset_dir}/virus.taxids" \
            -out "${subset_dir}/virus.fa" \
            -outfmt '%f' 2>&1 | tail -3

        local virus_seqs=$(grep -c '^>' "${subset_dir}/virus.fa" || echo 0)
        local virus_size=$(du -sh "${subset_dir}/virus.fa" | cut -f1)
        log "    Extracted: ${virus_seqs} sequences, ${virus_size}"

        makeblastdb \
            -in "${subset_dir}/virus.fa" \
            -dbtype nucl \
            -out "${subset_dir}/core_nt_virus" \
            -title "core_nt virus-only subset" \
            -parse_seqids \
            -blastdb_version 5 \
            -max_file_sz 4GB 2>&1 | tail -3

        rm -f "${subset_dir}/virus.fa"
    else
        log "    Already exists, skipping."
    fi

    blastdbcmd -db "${subset_dir}/core_nt_virus" -info 2>&1 | head -5
    end=$(date +%s)
    log "    Duration: $((end - start))s"

    # ── Broad subset (Virus + Bacteria + Plasmodium) ──
    log ""
    log "  [3/3] Broad subset (Virus + Bacteria + Plasmodium)..."
    start=$(date +%s)

    echo -e "10239\n2\n5820" > "${subset_dir}/broad.taxids"

    if [[ ! -f "${subset_dir}/core_nt_broad.nsq" ]] && ! ls "${subset_dir}/core_nt_broad".*.nsq &>/dev/null 2>&1; then
        blastdbcmd -db "${DB_DIR}/${DB_NAME}" \
            -taxidlist "${subset_dir}/broad.taxids" \
            -out "${subset_dir}/broad.fa" \
            -outfmt '%f' 2>&1 | tail -3

        local broad_seqs=$(grep -c '^>' "${subset_dir}/broad.fa" || echo 0)
        local broad_size=$(du -sh "${subset_dir}/broad.fa" | cut -f1)
        log "    Extracted: ${broad_seqs} sequences, ${broad_size}"

        makeblastdb \
            -in "${subset_dir}/broad.fa" \
            -dbtype nucl \
            -out "${subset_dir}/core_nt_broad" \
            -title "core_nt broad subset (Virus+Bacteria+Plasmodium)" \
            -parse_seqids \
            -blastdb_version 5 \
            -max_file_sz 4GB 2>&1 | tail -3

        rm -f "${subset_dir}/broad.fa"
    else
        log "    Already exists, skipping."
    fi

    blastdbcmd -db "${subset_dir}/core_nt_broad" -info 2>&1 | head -5
    end=$(date +%s)
    log "    Duration: $((end - start))s"

    # Summary
    log ""
    log "  Subset summary:"
    for name in pathogen virus broad; do
        local size=$(du -sh "${subset_dir}/core_nt_${name}"* 2>/dev/null | awk '{sum+=$1} END{printf "%.1f", sum}' || echo "?")
        local vols=$(ls "${subset_dir}/core_nt_${name}".*.nsq 2>/dev/null | wc -l || echo "?")
        log "    core_nt_${name}: ${vols} volumes, ${size} GB"
    done
}

# ── Step 2: Create shards ──
step_shard() {
    log ""
    log "━━━ Creating DB shards ━━━"

    local shard_dir="${WORK_DIR}/shards"
    mkdir -p "$shard_dir"

    # Discover volume files
    local volumes=()
    for f in "${DB_DIR}/${DB_NAME}".*.nsq; do
        volumes+=("$(basename "$f" .nsq)")
    done

    IFS=$'\n' volumes=($(sort <<<"${volumes[*]}")); unset IFS
    log "  Found ${#volumes[@]} volumes"

    for num_shards in 5 10; do
        log ""
        log "  Creating ${num_shards}-shard layout..."
        local start=$(date +%s)

        local this_dir="${shard_dir}/${num_shards}shards"
        mkdir -p "$this_dir"

        local vols_per_shard=$(( (${#volumes[@]} + num_shards - 1) / num_shards ))

        for ((s=0; s<num_shards; s++)); do
            local vol_start=$((s * vols_per_shard))
            local vol_end=$((vol_start + vols_per_shard))
            [[ $vol_end -gt ${#volumes[@]} ]] && vol_end=${#volumes[@]}
            [[ $vol_start -ge ${#volumes[@]} ]] && break

            local vol_list=""
            for ((v=vol_start; v<vol_end; v++)); do
                [[ -n "$vol_list" ]] && vol_list="$vol_list "
                vol_list="${vol_list}${DB_DIR}/${volumes[$v]}"
            done

            local shard_name="${DB_NAME}_shard_$(printf '%02d' $s)"

            blastdb_aliastool \
                -dblist "$vol_list" \
                -dbtype nucl \
                -out "${this_dir}/${shard_name}" \
                -title "${DB_NAME} shard ${s} of ${num_shards}" 2>/dev/null

            log "    Shard ${s}: $((vol_end - vol_start)) vols (${volumes[$vol_start]}..${volumes[$((vol_end-1))]})"
        done

        local end=$(date +%s)
        log "    Created in $((end - start))s"
    done
}

# ── Step 3: Upload to blob ──
step_upload() {
    log ""
    log "━━━ Uploading to Blob Storage ━━━"

    local subset_dir="${WORK_DIR}/subsets"
    local shard_dir="${WORK_DIR}/shards"

    # Upload subsets
    for name in pathogen virus broad; do
        local db_files="${subset_dir}/core_nt_${name}"
        if ls "${db_files}"* &>/dev/null 2>&1; then
            log ""
            log "  Uploading core_nt_${name}..."
            local start=$(date +%s)

            # Upload all DB files for this subset
            for f in "${db_files}"*; do
                local bname=$(basename "$f")
                azcopy cp "$f" "${BLOB_BASE}/core_nt_${name}/${bname}" \
                    --overwrite=ifSourceNewer --block-size-mb=256 \
                    --log-level=WARNING 2>&1 | grep -E "^(Final|Number)" || true
            done

            local end=$(date +%s)
            log "    Uploaded in $((end - start))s"
        fi
    done

    # Upload shards
    # For each shard, upload the volume files it references
    for shard_layout_dir in "${shard_dir}"/*/; do
        local layout_name=$(basename "$shard_layout_dir")
        log ""
        log "  Uploading shard layout: ${layout_name}..."
        local start=$(date +%s)

        for nal_file in "${shard_layout_dir}"*.nal; do
            [[ -f "$nal_file" ]] || continue
            local shard_name=$(basename "$nal_file" .nal)

            # Parse DBLIST from .nal
            local dblist_line=$(grep "^DBLIST" "$nal_file" || true)
            if [[ -z "$dblist_line" ]]; then
                # Try space-separated after DBLIST
                dblist_line=$(cat "$nal_file" | grep "DBLIST" || true)
            fi

            local vol_paths=()
            for token in $dblist_line; do
                [[ "$token" == "DBLIST" ]] && continue
                vol_paths+=("$token")
            done

            local blob_shard_dir="${BLOB_BASE}/${layout_name}/${shard_name}"

            for vol_path in "${vol_paths[@]}"; do
                # Upload all extension files for this volume
                for ext_file in "${vol_path}".*; do
                    [[ -f "$ext_file" ]] || continue
                    local fname=$(basename "$ext_file")
                    azcopy cp "$ext_file" "${blob_shard_dir}/${fname}" \
                        --overwrite=ifSourceNewer --block-size-mb=256 \
                        --log-level=WARNING 2>&1 | grep -E "^Final" || true
                done
            done

            # Create AKS-compatible .nal (local paths /blast/blastdb/)
            local aks_nal_content="TITLE ${shard_name}\nDBLIST"
            for vol_path in "${vol_paths[@]}"; do
                local vol_base=$(basename "$vol_path")
                aks_nal_content="${aks_nal_content} /blast/blastdb/${vol_base}"
            done
            echo -e "$aks_nal_content" > "${shard_layout_dir}/${shard_name}_aks.nal"
            azcopy cp "${shard_layout_dir}/${shard_name}_aks.nal" \
                "${blob_shard_dir}/${shard_name}.nal" \
                --overwrite=true --log-level=WARNING 2>&1 | grep "^Final" || true
        done

        local end=$(date +%s)
        log "    Uploaded in $((end - start))s"
    done

    log ""
    log "  Upload complete."
}

# ── Step 4: Correctness validation ──
step_validate() {
    log ""
    log "━━━ Correctness validation ━━━"

    download_queries

    local val_dir="${WORK_DIR}/validation"
    mkdir -p "$val_dir"
    local total_letters=$(cat "${WORK_DIR}/total_letters.txt" 2>/dev/null || echo "0")
    local subset_dir="${WORK_DIR}/subsets"

    # Reference search
    log ""
    log "  [1/3] Reference search (full core_nt, 10 queries)..."
    local start=$(date +%s)
    blastn -db "${DB_DIR}/${DB_NAME}" -query "${QUERY_DIR}/pathogen-10.fa" \
        -max_target_seqs 500 -evalue 0.05 -outfmt "6 std" -num_threads 16 \
        -out "${val_dir}/ref_full.out" 2>&1 | tail -3
    local end=$(date +%s)
    local ref_hits=$(wc -l < "${val_dir}/ref_full.out")
    log "    Reference: ${ref_hits} hits (${((end-start))}s)"

    # Subset validation
    log ""
    log "  [2/3] Pathogen subset search..."
    start=$(date +%s)
    blastn -db "${subset_dir}/core_nt_pathogen" -query "${QUERY_DIR}/pathogen-10.fa" \
        -max_target_seqs 500 -evalue 0.05 -outfmt "6 std" -num_threads 16 \
        -out "${val_dir}/subset_pathogen.out" 2>&1 | tail -3
    end=$(date +%s)
    local subset_hits=$(wc -l < "${val_dir}/subset_pathogen.out")
    log "    Subset: ${subset_hits} hits (${((end-start))}s)"

    # Shard validation (10 shards)
    log ""
    log "  [3/3] 10-shard validation..."
    start=$(date +%s)
    local shard_dir="${WORK_DIR}/shards/10shards"
    local shard_results="${val_dir}/shard_results"
    mkdir -p "$shard_results"

    for nal_file in "${shard_dir}"/*.nal; do
        local shard_name=$(basename "$nal_file" .nal)
        log "    Searching ${shard_name}..."
        blastn -db "${shard_dir}/${shard_name}" -query "${QUERY_DIR}/pathogen-10.fa" \
            -max_target_seqs 500 -evalue 0.05 -outfmt "6 std" -num_threads 4 \
            -dbsize "$total_letters" \
            -out "${shard_results}/${shard_name}.out" 2>/dev/null || true
    done

    # Merge and compare
    cat "${shard_results}"/*.out | sort -k1,1 -k11,11g > "${val_dir}/merged_shards_raw.out"

    # Per-query top-500
    python3 -c "
from collections import defaultdict
hits = defaultdict(list)
with open('${val_dir}/merged_shards_raw.out') as f:
    for line in f:
        fields = line.strip().split('\t')
        if len(fields) >= 12:
            hits[fields[0]].append((float(fields[10]), -float(fields[11]), line.strip()))
with open('${val_dir}/merged_shards_top500.out', 'w') as f:
    for qid in sorted(hits.keys()):
        for ev, neg_bs, raw in sorted(hits[qid])[:500]:
            f.write(raw + '\n')
"

    local merged_hits=$(wc -l < "${val_dir}/merged_shards_top500.out")
    end=$(date +%s)
    log "    Merged shards: ${merged_hits} hits (${((end-start))}s)"

    # Compute overlap
    local common=$(comm -12 \
        <(cut -f1,2 "${val_dir}/ref_full.out" | sort -u) \
        <(cut -f1,2 "${val_dir}/merged_shards_top500.out" | sort -u) | wc -l)
    local ref_pairs=$(cut -f1,2 "${val_dir}/ref_full.out" | sort -u | wc -l)
    local overlap_pct=$(echo "scale=1; ${common} * 100 / ${ref_pairs}" | bc 2>/dev/null || echo "?")

    log ""
    log "  ━━━ Validation Results ━━━"
    log "  Reference (full DB):     ${ref_hits} hits"
    log "  Pathogen subset:         ${subset_hits} hits"
    log "  10-shard merged:         ${merged_hits} hits"
    log "  Shard/Ref overlap:       ${overlap_pct}% (${common}/${ref_pairs} pairs)"

    if [[ $(echo "$overlap_pct > 95" | bc 2>/dev/null || echo 0) -eq 1 ]]; then
        log "  ✓ VALIDATION PASSED (>95% overlap)"
    else
        log "  ✗ VALIDATION CHECK: ${overlap_pct}% — review E-value differences"
    fi

    # Save timing data
    log ""
    log "  BLAST timing (for v3 estimate calibration):"
    log "    Full core_nt (32 vCPU):     see above"
    log "    Pathogen subset (32 vCPU):  see above"
}

# ── Main ──
main() {
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "  ElasticBLAST v3 — DB Preparation"
    log "  Step: $STEP"
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    mkdir -p "$WORK_DIR"

    install_tools
    setup_auth
    download_db

    case "$STEP" in
        subset)   step_subset ;;
        shard)    step_shard ;;
        upload)   step_upload ;;
        validate) step_validate ;;
        all)
            step_subset
            step_shard
            step_upload
            step_validate

            log ""
            log "━━━ ALL PREP STEPS COMPLETE ━━━"
            log "  Subsets: core_nt_pathogen, core_nt_virus, core_nt_broad"
            log "  Shards: 5shards, 10shards"
            log "  Uploaded to: ${BLOB_BASE}/"
            log "  Validation: see above"
            ;;
        *)
            err "Unknown step: $STEP"
            echo "Usage: $0 [all|subset|shard|upload|validate]"
            exit 1
            ;;
    esac
}

main
REMOTE_SCRIPT
}

# ══════════════════════════════════════════════════════════════
# Main script — runs locally, orchestrates the VM
# ══════════════════════════════════════════════════════════════

case "$STEP" in
    cleanup)
        cleanup_vm
        exit 0
    ;;
    status)
        az vm show -g "$RG" -n "$VM_NAME" --query '{status:provisioningState,size:hardwareProfile.vmSize}' -o table 2>&1
        exit 0
    ;;
    ssh)
        IP=$(az vm show -g "$RG" -n "$VM_NAME" --show-details --query publicIps -o tsv 2>/dev/null)
        if [[ -z "$IP" ]]; then
            err "VM not found. Create it first."
            exit 1
        fi
        log "SSH into ${VM_NAME} at ${IP}..."
        ssh -o StrictHostKeyChecking=no "azureuser@${IP}"
        exit 0
    ;;
    logs)
        IP=$(az vm show -g "$RG" -n "$VM_NAME" --show-details --query publicIps -o tsv 2>/dev/null)
        ssh -o StrictHostKeyChecking=no "azureuser@${IP}" "tail -100 /tmp/prep_db_v3.log"
        exit 0
    ;;
esac

log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "  Creating prep VM: ${VM_NAME} (${VM_SKU})"
log "  Step: ${STEP}"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check if VM already exists
EXISTING=$(az vm show -g "$RG" -n "$VM_NAME" --query provisioningState -o tsv 2>/dev/null || true)
if [[ "$EXISTING" == "Succeeded" ]]; then
    log "VM already exists and running."
    IP=$(az vm show -g "$RG" -n "$VM_NAME" --show-details --query publicIps -o tsv)
else
    log "Creating VM..."
    az vm create \
    -g "$RG" \
    -n "$VM_NAME" \
    --image "$VM_IMAGE" \
    --size "$VM_SKU" \
    --os-disk-size-gb 512 \
    --assign-identity \
    --admin-username azureuser \
    --generate-ssh-keys \
    -o table 2>&1 | tail -3
    
    IP=$(az vm show -g "$RG" -n "$VM_NAME" --show-details --query publicIps -o tsv)
    log "VM created: ${IP}"
    
    # Assign Storage Blob Data Contributor to the VM's identity
    log "Assigning storage access..."
    VM_IDENTITY=$(az vm show -g "$RG" -n "$VM_NAME" --query identity.principalId -o tsv)
    STORAGE_ID=$(az storage account show -n "$STORAGE" -g "$RG" --query id -o tsv)
    az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee-object-id "$VM_IDENTITY" \
    --assignee-principal-type ServicePrincipal \
    --scope "$STORAGE_ID" \
    -o none 2>&1 || warn "Role may already be assigned"
    
    log "Waiting 30s for identity propagation..."
    sleep 30
fi

# Generate and upload the remote script
log "Uploading prep script to VM..."
REMOTE_SCRIPT_CONTENT=$(build_remote_script "$STEP" | sed "s/__STEP__/$STEP/g")
echo "$REMOTE_SCRIPT_CONTENT" | ssh -o StrictHostKeyChecking=no "azureuser@${IP}" "cat > /tmp/prep_db_v3_run.sh && chmod +x /tmp/prep_db_v3_run.sh"

# Execute remotely
log "Starting DB preparation on VM..."
log "  This will take 30-60 minutes."
log "  Monitor: ssh azureuser@${IP} 'tail -f /tmp/prep_db_v3.log'"
log ""

ssh -o StrictHostKeyChecking=no "azureuser@${IP}" \
"sudo bash /tmp/prep_db_v3_run.sh 2>&1 | tee /tmp/prep_db_v3.log" 2>&1

PREP_EXIT=$?

if [[ $PREP_EXIT -eq 0 ]]; then
    log ""
    log "━━━ DB preparation completed successfully! ━━━"
    log ""
    
    # Optionally auto-cleanup
    read -p "Delete prep VM? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cleanup_vm
    else
        log "VM kept running. Delete later with: $0 cleanup"
        log "VM cost: ~\$1.53/hr (${VM_SKU})"
    fi
else
    err "DB preparation failed (exit code $PREP_EXIT)"
    err "Check logs: ssh azureuser@${IP} 'cat /tmp/prep_db_v3.log'"
    exit $PREP_EXIT
fi
