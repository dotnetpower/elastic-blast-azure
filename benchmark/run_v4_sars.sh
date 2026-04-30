#!/bin/bash
# benchmark/run_v4_sars.sh — V4 SARS-CoV-2 ORF1ab Benchmark
#
# Single query (21,290 bp) × 10 shards × 5 BLAST repetitions
# Measures every phase: cluster create, DB download, BLAST (5×), upload.
#
# Usage:
#   ./benchmark/run_v4_sars.sh              # Full benchmark
#   ./benchmark/run_v4_sars.sh create       # Create cluster only
#   ./benchmark/run_v4_sars.sh upload-query # Upload query to blob
#   ./benchmark/run_v4_sars.sh deploy       # Deploy shard jobs
#   ./benchmark/run_v4_sars.sh status       # Check job status
#   ./benchmark/run_v4_sars.sh results      # Collect results
#   ./benchmark/run_v4_sars.sh cleanup      # Delete cluster
#
# Author: Moon Hyuk Choi

set -eo pipefail
cd "$(dirname "$0")/.."

# ── Config ──
RG="rg-elb-koc"
CLUSTER="elb-v4-sars"
LOCATION="koreacentral"
VM_SIZE="Standard_E16s_v3"    # 16 vCPU, 128 GB RAM, $1.008/hr
NUM_NODES=10
NUM_SHARDS=10
BLAST_RUNS=5                  # Repeat BLAST 5 times per shard
ACR="elbacr"
ACR_RG="rg-elbacr"
STORAGE="stgelb"
BLOB_BASE="https://${STORAGE}.blob.core.windows.net/blast-db"
QUERY_LOCAL="benchmark/queries/sars_cov2_orf1ab.fa"
QUERY_BLOB="https://${STORAGE}.blob.core.windows.net/queries/sars_cov2_orf1ab.fa"
RESULTS_BLOB="https://${STORAGE}.blob.core.windows.net/results/v4/V4-SARS-S10"
DB_NAME="core_nt"
TOTAL_LETTERS=978954058562
BLAST_IMAGE="${ACR}.azurecr.io/ncbi/elb:1.4.0"
RESULTS_DIR="benchmark/results/v4"
LOG_DIR="${RESULTS_DIR}/logs"
DATA_DIR="${RESULTS_DIR}/data"

export AZCOPY_AUTO_LOGIN_TYPE="${AZCOPY_AUTO_LOGIN_TYPE:-AZCLI}"

GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'
log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; }
section() {
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  $*${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

mkdir -p "$LOG_DIR" "$DATA_DIR"

# ══════════════════════════════════════════════════
# Step 0: Upload query to blob storage
# ══════════════════════════════════════════════════
step_upload_query() {
    section "Uploading SARS-CoV-2 ORF1ab query to blob"
    if [[ ! -f "$QUERY_LOCAL" ]]; then
        err "Query file not found: $QUERY_LOCAL"
        exit 1
    fi
    local sz
    sz=$(wc -c < "$QUERY_LOCAL")
    log "Query file: $QUERY_LOCAL ($sz bytes, 1 sequence, 21,290 bp)"
    azcopy cp "$QUERY_LOCAL" "$QUERY_BLOB" --log-level=WARNING 2>&1 | tail -3
    log "Upload complete."
}

# ══════════════════════════════════════════════════
# Step 1: Create AKS cluster
# ══════════════════════════════════════════════════
step_create() {
    section "Creating AKS cluster: $CLUSTER ($NUM_NODES x $VM_SIZE)"
    local T_START=$(date +%s)
    
    local state
    state=$(az aks show -g "$RG" -n "$CLUSTER" --query provisioningState -o tsv 2>/dev/null || echo "NotFound")
    if [[ "$state" == "Succeeded" ]]; then
        log "Cluster already exists and running."
        az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
        local T_END=$(date +%s)
        log "PHASE cluster-create $((T_END - T_START)) seconds (reused)"
        return 0
    fi
    
    az aks create \
    -g "$RG" \
    -n "$CLUSTER" \
    --node-count "$NUM_NODES" \
    --node-vm-size "$VM_SIZE" \
    --attach-acr "$ACR" \
    --generate-ssh-keys \
    -o none 2>&1
    
    log "Getting credentials..."
    az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
    
    # Assign storage access to kubelet identity
    log "Assigning storage access..."
    local kubelet_id
    kubelet_id=$(az aks show -g "$RG" -n "$CLUSTER" --query identityProfile.kubeletidentity.objectId -o tsv)
    local storage_id
    storage_id=$(az storage account show -n "$STORAGE" -g "$RG" --query id -o tsv)
    az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee-object-id "$kubelet_id" \
    --assignee-principal-type ServicePrincipal \
    --scope "$storage_id" -o none 2>/dev/null || true
    
    local T_END=$(date +%s)
    log "PHASE cluster-create $((T_END - T_START)) seconds"
    log "Cluster ready. Nodes:"
    kubectl get nodes -o wide
}

# ══════════════════════════════════════════════════
# Step 2: Deploy shard BLAST jobs (with 5× repetition)
# ══════════════════════════════════════════════════
step_deploy() {
    section "Deploying $NUM_SHARDS shard jobs ($BLAST_RUNS BLAST runs each)"
    
    az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
    
    local nodes
    nodes=($(kubectl get nodes -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort))
    local num_worker_nodes=${#nodes[@]}
    log "Worker nodes: $num_worker_nodes"
    
    # Delete existing jobs
    kubectl delete jobs -l app=v4-sars-blast --ignore-not-found 2>/dev/null
    
    for ((s=0; s<NUM_SHARDS; s++)); do
        local shard_idx=$(printf '%02d' $s)
        local shard_name="${DB_NAME}_shard_${shard_idx}"
        local job_name="v4-sars-${shard_idx}"
        local node_idx=$((s % num_worker_nodes))
        local target_node="${nodes[$node_idx]}"
        
        log "  Creating job: $job_name -> $target_node (shard $shard_idx, $BLAST_RUNS runs)"
        
        cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  labels:
    app: v4-sars-blast
    shard: "${shard_idx}"
spec:
  backoffLimit: 1
  activeDeadlineSeconds: 10800
  template:
    metadata:
      labels:
        app: v4-sars-blast
        shard: "${shard_idx}"
    spec:
      nodeName: ${target_node}
      restartPolicy: Never
      containers:
      - name: blast
        image: ${BLAST_IMAGE}
        command: ["/bin/bash", "-c"]
        args:
        - |
          set -eo pipefail
          SHARD_IDX=${s}
          SHARD_NAME=${shard_name}
          DB_DIR=/workspace/blastdb
          QUERY_DIR=/workspace/queries
          RES_DIR=/workspace/results
          BLAST_RUNS=${BLAST_RUNS}
          mkdir -p \$DB_DIR \$QUERY_DIR \$RES_DIR

          echo "================================================================"
          echo "  V4 SARS-CoV-2 ORF1ab Benchmark — Shard \$SHARD_IDX"
          echo "  Query: NC_045512.2:266-21555 (21,290 bp)"
          echo "  BLAST runs: \$BLAST_RUNS"
          echo "================================================================"

          # ── Phase 1: Managed Identity Login ──
          echo ""
          echo "=== Phase 1: Auth ==="
          T_AUTH_S=\$(date +%s)
          azcopy login --identity 2>/dev/null || echo "azcopy login warning"
          T_AUTH_E=\$(date +%s)
          AUTH_SEC=\$((T_AUTH_E - T_AUTH_S))
          echo "TIMING auth \$AUTH_SEC seconds"

          # ── Phase 2: Download DB shard ──
          echo ""
          echo "=== Phase 2: Download DB Shard ==="
          T_DL_S=\$(date +%s)

          # Download manifest
          T_MANIFEST_S=\$(date +%s)
          azcopy cp "${BLOB_BASE}/${NUM_SHARDS}shards/${shard_name}/${shard_name}.manifest" \
              /tmp/manifest.txt --log-level=WARNING 2>/dev/null
          T_MANIFEST_E=\$(date +%s)
          VOLUMES=\$(cat /tmp/manifest.txt)
          echo "Manifest download: \$((T_MANIFEST_E - T_MANIFEST_S))s"
          echo "Volumes: \$VOLUMES"

          # Download each volume
          T_VOL_S=\$(date +%s)
          VOL_COUNT=0
          for VOL in \$VOLUMES; do
              echo "  Downloading \$VOL..."
              for EXT in nsq nhr nin nnd nni nog nos not; do
                  azcopy cp "${BLOB_BASE}/core_nt/\${VOL}.\${EXT}" "\${DB_DIR}/\${VOL}.\${EXT}" \
                      --block-size-mb=256 --log-level=ERROR 2>/dev/null || true
              done
              VOL_COUNT=\$((VOL_COUNT + 1))
          done
          T_VOL_E=\$(date +%s)
          VOL_SEC=\$((T_VOL_E - T_VOL_S))
          echo "Volume download: \${VOL_SEC}s (\${VOL_COUNT} volumes)"

          # Build volume paths
          VOLPATHS=""
          for VOL in \$VOLUMES; do
              [ -n "\$VOLPATHS" ] && VOLPATHS="\$VOLPATHS "
              VOLPATHS="\${VOLPATHS}\${DB_DIR}/\${VOL}"
          done

          # Download taxonomy files
          T_TAX_S=\$(date +%s)
          for F in taxdb.btd taxdb.bti core_nt.ndb core_nt.ntf core_nt.nto; do
              echo "  Downloading \$F..."
              azcopy cp "${BLOB_BASE}/core_nt/\${F}" "\${DB_DIR}/\${F}" \
                  --block-size-mb=256 --log-level=ERROR 2>/dev/null || true
          done
          T_TAX_E=\$(date +%s)
          TAX_SEC=\$((T_TAX_E - T_TAX_S))
          echo "Taxonomy download: \${TAX_SEC}s"

          T_DL_E=\$(date +%s)
          DL_TOTAL=\$((T_DL_E - T_DL_S))
          DB_SIZE=\$(du -sh \$DB_DIR 2>/dev/null | cut -f1)
          DB_FILES=\$(ls \${DB_DIR}/*.nsq 2>/dev/null | wc -l)
          echo ""
          echo "TIMING db-download \$DL_TOTAL seconds"
          echo "DB size: \$DB_SIZE (\${DB_FILES} nsq volumes)"

          # ── Phase 3: Download query ──
          echo ""
          echo "=== Phase 3: Download Query ==="
          T_QDL_S=\$(date +%s)
          azcopy cp "${QUERY_BLOB}" "\${QUERY_DIR}/query.fa" \
              --log-level=WARNING 2>/dev/null
          T_QDL_E=\$(date +%s)
          QDL_SEC=\$((T_QDL_E - T_QDL_S))
          QSEQS=\$(grep -c '^>' \${QUERY_DIR}/query.fa)
          QLEN=\$(grep -v '^>' \${QUERY_DIR}/query.fa | tr -d '\n' | wc -c)
          echo "TIMING query-download \$QDL_SEC seconds"
          echo "Query: \$QSEQS sequence(s), \$QLEN bp"

          # ── Phase 4: BLAST × N runs ──
          echo ""
          echo "=== Phase 4: BLAST Execution ($BLAST_RUNS runs) ==="
          NCPU=\$(nproc)
          echo "CPUs: \$NCPU"

          for ((R=1; R<=$BLAST_RUNS; R++)); do
              echo ""
              echo "--- Run \$R/$BLAST_RUNS ---"
              OUTFILE="\${RES_DIR}/shard_${shard_idx}_run\${R}.out"

              T_BL_S=\$(date +%s)
              blastn -db "\$VOLPATHS" \
                  -query "\${QUERY_DIR}/query.fa" \
                  -max_target_seqs 500 -evalue 0.05 \
                  -word_size 28 -dust yes -soft_masking true \
                  -outfmt 7 \
                  -dbsize ${TOTAL_LETTERS} \
                  -num_threads \$NCPU \
                  -out "\$OUTFILE" 2>&1
              T_BL_E=\$(date +%s)

              BL_SEC=\$((T_BL_E - T_BL_S))
              HITS=\$(grep -vc '^#' "\$OUTFILE" 2>/dev/null || echo 0)
              OUT_SIZE=\$(wc -c < "\$OUTFILE" 2>/dev/null || echo 0)
              echo "TIMING blast-run-\$R \$BL_SEC seconds"
              echo "HITS run-\$R \$HITS"
              echo "OUTPUT_SIZE run-\$R \$OUT_SIZE bytes"
          done

          # ── Phase 5: Upload results ──
          echo ""
          echo "=== Phase 5: Upload Results ==="
          T_UP_S=\$(date +%s)
          for ((R=1; R<=$BLAST_RUNS; R++)); do
              OUTFILE="\${RES_DIR}/shard_${shard_idx}_run\${R}.out"
              gzip -f "\$OUTFILE"
              azcopy cp "\${OUTFILE}.gz" \
                  "${RESULTS_BLOB}/shard_${shard_idx}_run\${R}.out.gz" \
                  --log-level=WARNING 2>/dev/null || true
          done
          T_UP_E=\$(date +%s)
          UP_SEC=\$((T_UP_E - T_UP_S))
          echo "TIMING upload \$UP_SEC seconds"

          # ── Summary ──
          echo ""
          echo "================================================================"
          echo "  SHARD \$SHARD_IDX COMPLETE"
          echo "================================================================"
          echo "TIMING_SUMMARY auth=\$AUTH_SEC db_download=\$DL_TOTAL query_download=\$QDL_SEC upload=\$UP_SEC"
          echo "SHARD_COMPLETE \$SHARD_IDX"
        resources:
          requests:
            cpu: "8"
            memory: "80Gi"
          limits:
            cpu: "15"
            memory: "110Gi"
        volumeMounts:
        - name: workspace
          mountPath: /workspace
      volumes:
      - name: workspace
        hostPath:
          path: /mnt/workspace-v4-shard-${shard_idx}
          type: DirectoryOrCreate
EOF
    done
    
    log ""
    log "All $NUM_SHARDS shard jobs deployed ($BLAST_RUNS BLAST runs each)."
    kubectl get jobs -l app=v4-sars-blast
}

# ══════════════════════════════════════════════════
# Step 3: Monitor status
# ══════════════════════════════════════════════════
step_status() {
    az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
    
    log "=== Job Status ==="
    kubectl get jobs -l app=v4-sars-blast -o custom-columns=\
    NAME:.metadata.name,\
    STATUS:.status.conditions[0].type,\
    START:.status.startTime,\
    COMPLETION:.status.completionTime,\
    SUCCEEDED:.status.succeeded,\
    FAILED:.status.failed 2>/dev/null
    
    echo ""
    log "=== Pod Status ==="
    kubectl get pods -l app=v4-sars-blast -o custom-columns=\
    NAME:.metadata.name,\
    STATUS:.status.phase,\
    NODE:.spec.nodeName,\
    START:.status.startTime 2>/dev/null
    
    echo ""
    local total=$(kubectl get jobs -l app=v4-sars-blast --no-headers 2>/dev/null | wc -l)
    local done_count=$(kubectl get jobs -l app=v4-sars-blast -o jsonpath='{.items[?(@.status.succeeded==1)].metadata.name}' 2>/dev/null | wc -w)
    local fail_count=$(kubectl get jobs -l app=v4-sars-blast -o jsonpath='{.items[?(@.status.failed>=1)].metadata.name}' 2>/dev/null | wc -w)
    log "Progress: $done_count/$total completed, $fail_count failed"
    
    # Show recent log from a running pod
    local running
    running=$(kubectl get pods -l app=v4-sars-blast --field-selector=status.phase=Running -o name 2>/dev/null | head -1)
    if [[ -n "$running" ]]; then
        echo ""
        log "=== Latest log ($running) ==="
        kubectl logs "$running" --tail=15 2>/dev/null || true
    fi
}

# ══════════════════════════════════════════════════
# Step 4: Collect results
# ══════════════════════════════════════════════════
step_results() {
    section "Collecting V4 Results"
    az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
    
    # Save K8s job metadata
    kubectl get jobs -l app=v4-sars-blast -o json > "$DATA_DIR/v4_sars_jobs.json" 2>/dev/null
    
    # Collect logs from each shard
    for ((s=0; s<NUM_SHARDS; s++)); do
        local idx=$(printf '%02d' $s)
        local pod
        pod=$(kubectl get pods -l "shard=$idx,app=v4-sars-blast" -o name 2>/dev/null | head -1)
        if [[ -n "$pod" ]]; then
            kubectl logs "$pod" > "$DATA_DIR/v4_shard_${idx}_log.txt" 2>/dev/null || true
        fi
    done
    
    # Parse and display results
    section "V4 SARS-CoV-2 ORF1ab Benchmark Results"
    echo ""
    echo "Query: NC_045512.2:266-21555 (SARS-CoV-2 ORF1ab, 21,290 bp)"
    echo "Config: $NUM_SHARDS shards × $NUM_NODES nodes × $BLAST_RUNS BLAST runs"
    echo ""
    
    # Phase timings per shard
    printf "${BOLD}%-8s %8s %10s %8s" "Shard" "Auth" "DB_DL" "Q_DL"
    for ((R=1; R<=BLAST_RUNS; R++)); do
        printf " %9s" "BLAST_${R}"
    done
    printf " %8s${NC}\n" "Upload"
    printf "%-8s %8s %10s %8s" "-----" "------" "--------" "------"
    for ((R=1; R<=BLAST_RUNS; R++)); do
        printf " %9s" "--------"
    done
    printf " %8s\n" "------"
    
    declare -a ALL_BLAST_TIMES=()
    local max_total=0
    
    for ((s=0; s<NUM_SHARDS; s++)); do
        local idx=$(printf '%02d' $s)
        local logfile="$DATA_DIR/v4_shard_${idx}_log.txt"
        if [[ ! -f "$logfile" ]]; then
            printf "%-8s  no log\n" "S${idx}"
            continue
        fi
        
        local auth=$(grep "^TIMING auth" "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
        local db_dl=$(grep "^TIMING db-download" "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
        local q_dl=$(grep "^TIMING query-download" "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
        local upload=$(grep "^TIMING upload" "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
        
        printf "%-8s %7.1fs %9.1fs %7.1fs" "S${idx}" "$auth" "$db_dl" "$q_dl"
        
        for ((R=1; R<=BLAST_RUNS; R++)); do
            local bt=$(grep "^TIMING blast-run-${R} " "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
            printf " %8.1fs" "$bt"
            if [[ "$bt" != "?" ]]; then
                ALL_BLAST_TIMES+=("$bt")
            fi
        done
        printf " %7.1fs\n" "$upload"
    done
    
    # BLAST run statistics
    echo ""
    section "BLAST Run Statistics (across all shards, $BLAST_RUNS runs each)"
    if [[ ${#ALL_BLAST_TIMES[@]} -gt 0 ]]; then
        # Calculate stats using awk
        printf '%s\n' "${ALL_BLAST_TIMES[@]}" | awk '
        {
            sum += $1; sumsq += $1*$1; count++
            vals[NR] = $1
            if (NR == 1 || $1 < min) min = $1
            if (NR == 1 || $1 > max) max = $1
        }
        END {
            mean = sum / count
            variance = (sumsq / count) - (mean * mean)
            if (variance < 0) variance = 0
            stddev = sqrt(variance)
            printf "  Total measurements: %d\n", count
            printf "  Min:    %.2f s\n", min
            printf "  Max:    %.2f s\n", max
            printf "  Mean:   %.2f s\n", mean
            printf "  Stddev: %.2f s\n", stddev
            printf "  CV:     %.1f%%\n", (mean > 0 ? stddev/mean*100 : 0)
        }'
    fi
    
    # Per-run average across shards
    echo ""
    log "Per-run averages (across $NUM_SHARDS shards):"
    for ((R=1; R<=BLAST_RUNS; R++)); do
        local run_avg
        run_avg=$(for ((s=0; s<NUM_SHARDS; s++)); do
                local idx=$(printf '%02d' $s)
                local logfile="$DATA_DIR/v4_shard_${idx}_log.txt"
                grep "^TIMING blast-run-${R} " "$logfile" 2>/dev/null | awk '{print $3}'
        done | awk '{sum+=$1; n++} END {if(n>0) printf "%.2f", sum/n; else print "?"}')
        log "  Run $R avg: ${run_avg}s"
    done
    
    # Hit counts
    echo ""
    log "Hit counts per shard (Run 1):"
    for ((s=0; s<NUM_SHARDS; s++)); do
        local idx=$(printf '%02d' $s)
        local logfile="$DATA_DIR/v4_shard_${idx}_log.txt"
        local hits=$(grep "^HITS run-1 " "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
        log "  Shard $idx: $hits hits"
    done
    
    # Download results from blob
    echo ""
    log "Downloading results from blob..."
    mkdir -p "$RESULTS_DIR/raw/V4-SARS-S10"
    AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy cp \
    "${RESULTS_BLOB}/*" "$RESULTS_DIR/raw/V4-SARS-S10/" \
    --recursive --log-level=WARNING 2>&1 | tail -3
    
    # K8s job timing
    echo ""
    section "K8s Job Timing (wall clock)"
    printf "%-12s %22s %22s %10s\n" "Job" "Start" "Completion" "Duration"
    printf "%-12s %22s %22s %10s\n" "---" "-----" "----------" "--------"
    for ((s=0; s<NUM_SHARDS; s++)); do
        local idx=$(printf '%02d' $s)
        local start_t=$(kubectl get job "v4-sars-${idx}" -o jsonpath='{.status.startTime}' 2>/dev/null || echo "?")
        local end_t=$(kubectl get job "v4-sars-${idx}" -o jsonpath='{.status.completionTime}' 2>/dev/null || echo "?")
        if [[ "$start_t" != "?" && "$end_t" != "?" ]]; then
            local s_epoch=$(date -d "$start_t" +%s 2>/dev/null || echo 0)
            local e_epoch=$(date -d "$end_t" +%s 2>/dev/null || echo 0)
            local dur=$((e_epoch - s_epoch))
            printf "%-12s %22s %22s %8ds\n" "v4-sars-${idx}" "$start_t" "$end_t" "$dur"
        else
            printf "%-12s %22s %22s %10s\n" "v4-sars-${idx}" "$start_t" "$end_t" "?"
        fi
    done
}

# ══════════════════════════════════════════════════
# Step 5: Cleanup
# ══════════════════════════════════════════════════
step_cleanup() {
    section "Deleting AKS cluster: $CLUSTER"
    az aks delete -g "$RG" -n "$CLUSTER" --yes --no-wait 2>&1
    log "Cluster deletion initiated."
}

# ══════════════════════════════════════════════════
# Full benchmark
# ══════════════════════════════════════════════════
step_full() {
    local BENCH_START=$(date +%s)
    
    section "V4 SARS-CoV-2 ORF1ab Benchmark"
    log "Query:  NC_045512.2:266-21555 (21,290 bp)"
    log "Shards: $NUM_SHARDS"
    log "Nodes:  $NUM_NODES x $VM_SIZE"
    log "BLAST:  $BLAST_RUNS runs per shard"
    log "Cost:   ~\$$(echo "scale=2; $NUM_NODES * 1.008" | bc)/hr"
    echo ""
    
    step_upload_query
    step_create
    step_deploy
    
    log ""
    log "Waiting for all shard jobs to complete..."
    log "Monitor with: $0 status"
    
    local max_wait=10800  # 3 hours (5 BLAST runs per shard)
    local waited=0
    while [[ $waited -lt $max_wait ]]; do
        local done_count=$(kubectl get jobs -l app=v4-sars-blast -o jsonpath='{.items[?(@.status.succeeded==1)].metadata.name}' 2>/dev/null | wc -w)
        local fail_count=$(kubectl get jobs -l app=v4-sars-blast -o jsonpath='{.items[?(@.status.failed>=1)].metadata.name}' 2>/dev/null | wc -w)
        local total=$((done_count + fail_count))
        
        if [[ $total -ge $NUM_SHARDS ]]; then
            log "All jobs finished: $done_count succeeded, $fail_count failed"
            break
        fi
        
        if (( waited % 60 == 0 )); then
            log "  Progress: $done_count/$NUM_SHARDS completed ($waited s elapsed)"
        fi
        sleep 15
        waited=$((waited + 15))
    done
    
    step_results
    
    local BENCH_END=$(date +%s)
    local ELAPSED=$((BENCH_END - BENCH_START))
    
    section "Benchmark Complete"
    log "Total wall clock: $((ELAPSED / 60)) min $((ELAPSED % 60)) sec"
    log "Results:  $RESULTS_DIR/"
    log "Cost:     ~\$$(echo "scale=2; $ELAPSED / 3600 * $NUM_NODES * 1.008" | bc)"
    
    echo ""
    read -p "Delete AKS cluster? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        step_cleanup
    else
        log "Cluster kept. Delete later: $0 cleanup"
        log "Cost: ~\$$(echo "scale=2; $NUM_NODES * 1.008" | bc)/hr"
    fi
}

# ══════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════
case "${1:-full}" in
    upload-query)  step_upload_query ;;
    create)        step_create ;;
    deploy)        step_deploy ;;
    status)        step_status ;;
    results)       step_results ;;
    cleanup)       step_cleanup ;;
    full)          step_full ;;
    *)
        echo "Usage: $0 {full|upload-query|create|deploy|status|results|cleanup}"
        exit 1
    ;;
esac
