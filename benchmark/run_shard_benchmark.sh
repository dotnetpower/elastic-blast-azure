#!/bin/bash
# benchmark/run_shard_benchmark.sh — Direct AKS shard benchmark
#
# Creates an AKS cluster, deploys K8s Jobs per shard, runs BLAST, collects results.
# Bypasses ElasticBLAST for clean timing measurement.
#
# Usage:
#   ./benchmark/run_shard_benchmark.sh             # Full 10-shard benchmark
#   ./benchmark/run_shard_benchmark.sh create       # Create cluster only
#   ./benchmark/run_shard_benchmark.sh deploy       # Deploy jobs only
#   ./benchmark/run_shard_benchmark.sh status       # Check job status
#   ./benchmark/run_shard_benchmark.sh results      # Collect results
#   ./benchmark/run_shard_benchmark.sh cleanup      # Delete cluster
#
# Author: Moon Hyuk Choi

set -eo pipefail
cd "$(dirname "$0")/.."

# ── Config ──
RG="rg-elb-koc"
CLUSTER="elb-v3-shard"
LOCATION="koreacentral"
VM_SIZE="Standard_E16s_v3"   # 16 vCPU, 128 GB RAM, $1.008/hr
NUM_NODES=10
NUM_SHARDS=10
ACR="elbacr"
ACR_RG="rg-elbacr"
STORAGE="stgelb"
BLOB_BASE="https://${STORAGE}.blob.core.windows.net/blast-db"
QUERY_BLOB="https://${STORAGE}.blob.core.windows.net/queries/pathogen-10.fa"
RESULTS_BLOB="https://${STORAGE}.blob.core.windows.net/results/v3/B1-S10"
DB_NAME="core_nt"
TOTAL_LETTERS=978954058562
BLAST_IMAGE="${ACR}.azurecr.io/ncbi/elb:1.4.0"
RESULTS_DIR="benchmark/results/v3"

GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'
log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; }

# ══════════════════════════════════════════════════
# Step 1: Create AKS cluster
# ══════════════════════════════════════════════════
step_create() {
    log "Creating AKS cluster: $CLUSTER ($NUM_NODES × $VM_SIZE)"
    
    # Check if cluster exists
    local state
    state=$(az aks show -g "$RG" -n "$CLUSTER" --query provisioningState -o tsv 2>/dev/null || echo "NotFound")
    if [[ "$state" == "Succeeded" ]]; then
        log "Cluster already exists and running."
        az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
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
    
    log "Cluster created. Getting credentials..."
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
    
    log "Cluster ready. Nodes:"
    kubectl get nodes -o wide
}

# ══════════════════════════════════════════════════
# Step 2: Deploy shard BLAST jobs
# ══════════════════════════════════════════════════
step_deploy() {
    log "Deploying $NUM_SHARDS shard BLAST jobs..."
    
    # Ensure credentials
    az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
    
    # Get node names for affinity
    local nodes
    nodes=($(kubectl get nodes -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort))
    local num_worker_nodes=${#nodes[@]}
    log "Worker nodes: $num_worker_nodes"
    
    if [[ $num_worker_nodes -lt $NUM_SHARDS ]]; then
        err "Not enough nodes ($num_worker_nodes) for $NUM_SHARDS shards"
        err "Some nodes will run multiple shards sequentially"
    fi
    
    # Delete any existing shard jobs
    kubectl delete jobs -l app=shard-blast --ignore-not-found 2>/dev/null
    
    # Deploy one Job per shard
    for ((s=0; s<NUM_SHARDS; s++)); do
        local shard_idx=$(printf '%02d' $s)
        local shard_name="${DB_NAME}_shard_${shard_idx}"
        local job_name="shard-blast-${shard_idx}"
        
        # Assign to a specific node (round-robin if more shards than nodes)
        local node_idx=$((s % num_worker_nodes))
        local target_node="${nodes[$node_idx]}"
        
        log "  Creating job: $job_name → $target_node"
        
        cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  labels:
    app: shard-blast
    shard: "${shard_idx}"
spec:
  backoffLimit: 1
  activeDeadlineSeconds: 7200
  template:
    metadata:
      labels:
        app: shard-blast
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
          RESULTS_DIR=/workspace/results
          mkdir -p \$DB_DIR \$QUERY_DIR \$RESULTS_DIR

          echo "=== Shard \$SHARD_IDX: Download ==="
          START_DL=\$(date +%s)

          # Auth
          azcopy login --identity 2>/dev/null || echo "azcopy login warning"

          # Download manifest
          azcopy cp "${BLOB_BASE}/${NUM_SHARDS}shards/${shard_name}/${shard_name}.manifest" \
              /tmp/manifest.txt --log-level=WARNING 2>/dev/null
          VOLUMES=\$(cat /tmp/manifest.txt)
          echo "Volumes: \$VOLUMES"

          # Download each volume
          for VOL in \$VOLUMES; do
              echo "  Downloading \$VOL..."
              for EXT in nsq nhr nin nnd nni nog nos not; do
                  azcopy cp "${BLOB_BASE}/core_nt/\${VOL}.\${EXT}" "\${DB_DIR}/\${VOL}.\${EXT}" \
                      --block-size-mb=256 --log-level=ERROR 2>/dev/null || true
              done
          done

          # Build space-separated volume list for direct BLAST -db usage
          # (bypasses .nal alias file entirely)
          VOLPATHS=""
          for VOL in \$VOLUMES; do
              [ -n "\$VOLPATHS" ] && VOLPATHS="\$VOLPATHS "
              VOLPATHS="\${VOLPATHS}\${DB_DIR}/\${VOL}"
          done
          echo "Volume paths: \$VOLPATHS"

          # Download taxonomy support files
          # core_nt.ndb (6.5 GB), .ntf (185 MB), .nto (487 MB) + taxdb
          for F in taxdb.btd taxdb.bti core_nt.ndb core_nt.ntf core_nt.nto; do
              echo "  Downloading \$F..."
              azcopy cp "${BLOB_BASE}/core_nt/\${F}" "\${DB_DIR}/\${F}" \
                  --block-size-mb=256 --log-level=ERROR 2>/dev/null || true
          done

          END_DL=\$(date +%s)
          DL_TIME=\$((END_DL - START_DL))
          echo "RUNTIME download-shard-\${SHARD_IDX} \${DL_TIME} seconds"
          echo "DB files: \$(ls \${DB_DIR}/*.nsq 2>/dev/null | wc -l) nsq volumes"

          # Download query
          azcopy cp "${QUERY_BLOB}" "\${QUERY_DIR}/query.fa" \
              --log-level=WARNING 2>/dev/null
          echo "Query: \$(grep -c '^>' \${QUERY_DIR}/query.fa) sequences"

          echo "=== Shard \$SHARD_IDX: BLAST ==="
          START_BL=\$(date +%s)

          blastn -db "\$VOLPATHS" \
              -query "\${QUERY_DIR}/query.fa" \
              -max_target_seqs 500 -evalue 0.05 \
              -word_size 28 -dust yes -soft_masking true \
              -outfmt 7 \
              -dbsize ${TOTAL_LETTERS} \
              -num_threads \$(nproc) \
              -out "\${RESULTS_DIR}/shard_${shard_idx}.out" 2>&1

          END_BL=\$(date +%s)
          BL_TIME=\$((END_BL - START_BL))
          HITS=\$(grep -vc '^#' "\${RESULTS_DIR}/shard_${shard_idx}.out" 2>/dev/null || echo 0)
          echo "RUNTIME blast-shard-\${SHARD_IDX} \${BL_TIME} seconds"
          echo "HITS shard-\${SHARD_IDX} \${HITS}"

          # Compress and upload results
          gzip -f "\${RESULTS_DIR}/shard_${shard_idx}.out"
          azcopy cp "\${RESULTS_DIR}/shard_${shard_idx}.out.gz" \
              "${RESULTS_BLOB}/shard_${shard_idx}.out.gz" \
              --log-level=WARNING 2>/dev/null || true

          # Write timing summary
          echo "{\"shard\":${s},\"download_sec\":\${DL_TIME},\"blast_sec\":\${BL_TIME},\"hits\":\${HITS}}" \
              > "\${RESULTS_DIR}/timing.json"
          azcopy cp "\${RESULTS_DIR}/timing.json" \
              "${RESULTS_BLOB}/shard_${shard_idx}_timing.json" \
              --log-level=WARNING 2>/dev/null || true

          TOTAL=\$((END_BL - START_DL))
          echo ""
          echo "=== SUMMARY shard-\${SHARD_IDX} ==="
          echo "  Download: \${DL_TIME}s"
          echo "  BLAST:    \${BL_TIME}s"
          echo "  Hits:     \${HITS}"
          echo "  Total:    \${TOTAL}s"
          echo "SHARD_COMPLETE \${SHARD_IDX}"
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
          path: /mnt/workspace-shard-${shard_idx}
          type: DirectoryOrCreate
EOF
    done
    
    log ""
    log "All $NUM_SHARDS shard jobs deployed."
    kubectl get jobs -l app=shard-blast
}

# ══════════════════════════════════════════════════
# Step 3: Monitor status
# ══════════════════════════════════════════════════
step_status() {
    az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
    
    log "=== Job Status ==="
    kubectl get jobs -l app=shard-blast -o custom-columns=\
    NAME:.metadata.name,\
    STATUS:.status.conditions[0].type,\
    START:.status.startTime,\
    COMPLETION:.status.completionTime,\
    SUCCEEDED:.status.succeeded,\
    FAILED:.status.failed 2>/dev/null
    
    echo ""
    log "=== Pod Status ==="
    kubectl get pods -l app=shard-blast -o custom-columns=\
    NAME:.metadata.name,\
    STATUS:.status.phase,\
    NODE:.spec.nodeName,\
    START:.status.startTime 2>/dev/null
    
    echo ""
    local total=$(kubectl get jobs -l app=shard-blast --no-headers 2>/dev/null | wc -l)
    local done=$(kubectl get jobs -l app=shard-blast -o jsonpath='{.items[?(@.status.succeeded==1)].metadata.name}' 2>/dev/null | wc -w)
    local failed=$(kubectl get jobs -l app=shard-blast -o jsonpath='{.items[?(@.status.failed>=1)].metadata.name}' 2>/dev/null | wc -w)
    log "Progress: $done/$total completed, $failed failed"
    
    # Show recent logs for running pods
    local running
    running=$(kubectl get pods -l app=shard-blast --field-selector=status.phase=Running -o name 2>/dev/null | head -1)
    if [[ -n "$running" ]]; then
        echo ""
        log "=== Latest log ($running) ==="
        kubectl logs "$running" --tail=10 2>/dev/null || true
    fi
}

# ══════════════════════════════════════════════════
# Step 4: Collect results
# ══════════════════════════════════════════════════
step_results() {
    az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
    mkdir -p "$RESULTS_DIR/data"
    
    log "=== Collecting timing data ==="
    
    # From K8s job metadata
    kubectl get jobs -l app=shard-blast -o json > "$RESULTS_DIR/data/shard_jobs.json" 2>/dev/null
    
    # From pod logs
    for ((s=0; s<NUM_SHARDS; s++)); do
        local idx=$(printf '%02d' $s)
        local pod
        pod=$(kubectl get pods -l "shard=$idx" -o name 2>/dev/null | head -1)
        if [[ -n "$pod" ]]; then
            kubectl logs "$pod" > "$RESULTS_DIR/data/shard_${idx}_log.txt" 2>/dev/null || true
        fi
    done
    
    # Parse timings
    log ""
    log "=== Results ==="
    printf "%-12s %10s %10s %8s %12s %12s\n" "Shard" "Download" "BLAST" "Hits" "Start" "End"
    printf "%-12s %10s %10s %8s %12s %12s\n" "-----" "--------" "-----" "----" "-----" "---"
    
    local total_blast=0
    local max_blast=0
    for ((s=0; s<NUM_SHARDS; s++)); do
        local idx=$(printf '%02d' $s)
        local logfile="$RESULTS_DIR/data/shard_${idx}_log.txt"
        if [[ -f "$logfile" ]]; then
            local dl_time=$(grep "RUNTIME download-shard" "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
            local bl_time=$(grep "RUNTIME blast-shard" "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
            local hits=$(grep "^HITS " "$logfile" 2>/dev/null | awk '{print $3}' || echo "?")
            local start_t=$(kubectl get job "shard-blast-${idx}" -o jsonpath='{.status.startTime}' 2>/dev/null || echo "?")
            local end_t=$(kubectl get job "shard-blast-${idx}" -o jsonpath='{.status.completionTime}' 2>/dev/null || echo "?")
            printf "%-12s %9ss %9ss %8s %12s %12s\n" "shard_${idx}" "$dl_time" "$bl_time" "$hits" "${start_t:11:8}" "${end_t:11:8}"
            
            if [[ "$bl_time" =~ ^[0-9]+$ ]]; then
                total_blast=$((total_blast + bl_time))
                [[ $bl_time -gt $max_blast ]] && max_blast=$bl_time
            fi
        else
            printf "%-12s %10s %10s %8s\n" "shard_${idx}" "no log" "" ""
        fi
    done
    
    echo ""
    log "Total BLAST time (sequential): ${total_blast}s"
    log "Max BLAST time (parallel):     ${max_blast}s"
    log "Speedup vs full DB (57 min):   $(echo "scale=1; 3420 / $max_blast" | bc 2>/dev/null || echo '?')x"
    
    # Download results from blob
    log ""
    log "=== Downloading results from blob ==="
    mkdir -p "$RESULTS_DIR/raw/B1-S10"
    AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy cp \
    "${RESULTS_BLOB}/*" "$RESULTS_DIR/raw/B1-S10/" \
    --recursive --log-level=WARNING 2>&1 | tail -3
}

# ══════════════════════════════════════════════════
# Step 5: Cleanup
# ══════════════════════════════════════════════════
step_cleanup() {
    log "Deleting AKS cluster: $CLUSTER"
    az aks delete -g "$RG" -n "$CLUSTER" --yes --no-wait 2>&1
    log "Cluster deletion initiated."
}

# ══════════════════════════════════════════════════
# Full benchmark
# ══════════════════════════════════════════════════
step_full() {
    local START=$(date +%s)
    
    step_create
    step_deploy
    
    log ""
    log "Waiting for all shard jobs to complete..."
    log "Monitor with: $0 status"
    
    local max_wait=7200
    local waited=0
    while [[ $waited -lt $max_wait ]]; do
        local done_count=$(kubectl get jobs -l app=shard-blast -o jsonpath='{.items[?(@.status.succeeded==1)].metadata.name}' 2>/dev/null | wc -w)
        local fail_count=$(kubectl get jobs -l app=shard-blast -o jsonpath='{.items[?(@.status.failed>=1)].metadata.name}' 2>/dev/null | wc -w)
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
    
    local END=$(date +%s)
    local ELAPSED=$((END - START))
    log ""
    log "━━━ Benchmark complete ━━━"
    log "Total wall clock: $((ELAPSED / 60)) min"
    log "Results: $RESULTS_DIR/"
    
    # Ask before cleanup
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

# ── Main ──
CMD="${1:-full}"
case "$CMD" in
    create)  step_create ;;
    deploy)  step_deploy ;;
    status)  step_status ;;
    results) step_results ;;
    cleanup) step_cleanup ;;
    full)    step_full ;;
    *)
        echo "Usage: $0 [full|create|deploy|status|results|cleanup]"
        exit 1
    ;;
esac
