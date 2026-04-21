#!/bin/bash
# benchmark/prestage-db-vm.sh — Pre-stage BLAST DB using a temporary Azure VM
#
# Creates a temporary high-bandwidth VM in Azure, downloads BLAST DB, and uploads
# to Azure Blob Storage. Automatically cleans up the VM when done.
#
# Source fallback order:
#   1. AWS S3  (ncbi-blast-databases, fastest — server-to-server azcopy)
#   2. GCP GCS (blast-db bucket, azcopy S3-compatible)
#   3. NCBI FTP (ftp.ncbi.nlm.nih.gov, tar.gz download + extract + upload)
#
# Why VM: Azure VMs have ~30 Gbps network bandwidth (D64s_v3) vs local PC (~1 Gbps).
#         For core_nt (~500 GB), this reduces transfer from hours to ~15-30 min.
#
# Usage:
#   ./benchmark/prestage-db-vm.sh core_nt
#   ./benchmark/prestage-db-vm.sh nt
#   ./benchmark/prestage-db-vm.sh --list        # List available DBs
#   DB_VM_SKU=Standard_D32s_v3 ./benchmark/prestage-db-vm.sh core_nt  # Custom SKU
#
# Author: Moon Hyuk Choi

set -euo pipefail

# ── Configuration ──
DB_NAME="${1:-core_nt}"
RG="rg-elb-koc"
STORAGE="stgelb"
CONTAINER="blast-db"
LOCATION="koreacentral"
VM_NAME="elb-prestage-vm"
VM_SKU="${DB_VM_SKU:-Standard_D64s_v3}"   # 64 vCPU, ~30 Gbps network
VM_IMAGE="Canonical:ubuntu-24_04-lts:server:latest"
BLOB_URL="https://$STORAGE.blob.core.windows.net/$CONTAINER"
S3_URL="https://ncbi-blast-databases.s3.amazonaws.com"
GCS_URL="https://storage.googleapis.com/blast-db"
FTP_URL="https://ftp.ncbi.nlm.nih.gov/blast/db"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; }

# ── List mode ──
if [[ "$DB_NAME" == "--list" ]]; then
    echo "Available BLAST DBs:"
    echo "  Nucleotide: nt, nt_prok, nt_euk, nt_viruses, core_nt, refseq_rna"
    echo "  Protein:    nr, swissprot, refseq_protein, pdbaa"
    echo "  Other:      16S_ribosomal_RNA, env_nt, env_nr"
    echo ""
    echo "Source fallback: AWS S3 -> GCP GCS -> NCBI FTP"
    echo "  S3:  $S3_URL"
    echo "  GCS: $GCS_URL"
    echo "  FTP: $FTP_URL"
    echo ""
    echo "Full list: https://ftp.ncbi.nlm.nih.gov/blast/db/"
    exit 0
fi

# ── Cleanup function (always runs) ──
cleanup() {
    local exit_code=$?
    echo ""
    if [[ $exit_code -eq 0 ]]; then
        log "Transfer completed successfully. Cleaning up..."
    else
        err "Transfer failed (exit code: $exit_code). Cleaning up..."
    fi
    
    log "Deleting VM and associated resources: $VM_NAME"
    az vm delete -g "$RG" -n "$VM_NAME" --yes --force-deletion true 2>/dev/null || true
    
    # Delete associated resources (NIC, NSG, Public IP, OS Disk)
    for resource_type in "Microsoft.Network/networkInterfaces" \
    "Microsoft.Network/networkSecurityGroups" \
    "Microsoft.Network/publicIPAddresses"; do
        local res_name="${VM_NAME}"
        case "$resource_type" in
            *networkInterfaces) res_name="${VM_NAME}VMNic" ;;
            *networkSecurityGroups) res_name="${VM_NAME}NSG" ;;
            *publicIPAddresses) res_name="${VM_NAME}PublicIP" ;;
        esac
        az resource delete -g "$RG" --resource-type "$resource_type" -n "$res_name" 2>/dev/null || true
    done
    
    # Delete OS disk
    local disk_id
    disk_id=$(az disk list -g "$RG" --query "[?starts_with(name,'${VM_NAME}')].id" -o tsv 2>/dev/null || true)
    if [[ -n "$disk_id" ]]; then
        az disk delete --ids "$disk_id" --yes 2>/dev/null || true
    fi
    
    # Delete VNet if created by this script
    az network vnet delete -g "$RG" -n "${VM_NAME}VNET" 2>/dev/null || true
    
    if [[ $exit_code -eq 0 ]]; then
        log "Cleanup complete."
    else
        warn "Cleanup attempted. Some resources may need manual deletion."
        warn "Check: az resource list -g $RG --query \"[?contains(name,'$VM_NAME')]\" -o table"
    fi
}
trap cleanup EXIT

# ── Start ──
echo "============================================"
echo " ElasticBLAST DB Pre-stage via Temporary VM"
echo "============================================"
echo ""
log "Database:  $DB_NAME"
log "VM SKU:    $VM_SKU"
log "Location:  $LOCATION"
log "Target:    $BLOB_URL/$DB_NAME/"
echo ""

# ── Step 1: Create VM ──
log "Step 1/4: Creating temporary VM ($VM_SKU)..."

# Check if VM already exists
if az vm show -g "$RG" -n "$VM_NAME" &>/dev/null; then
    warn "VM '$VM_NAME' already exists. Deleting first..."
    az vm delete -g "$RG" -n "$VM_NAME" --yes --force-deletion true
    sleep 10
fi

START_TIME=$(date +%s)

az vm create \
-g "$RG" \
-n "$VM_NAME" \
--image "$VM_IMAGE" \
--size "$VM_SKU" \
--os-disk-size-gb 64 \
--storage-sku StandardSSD_LRS \
--assign-identity "[system]" \
--admin-username azureuser \
--generate-ssh-keys \
--public-ip-sku Standard \
--accelerated-networking true \
--nsg-rule SSH \
--output none

VM_CREATE_TIME=$(( $(date +%s) - START_TIME ))
log "VM created in ${VM_CREATE_TIME}s"

# ── Step 2: Assign Storage Blob Data Contributor role to VM ──
log "Step 2/4: Assigning storage permissions to VM..."

VM_IDENTITY=$(az vm show -g "$RG" -n "$VM_NAME" \
--query "identity.principalId" -o tsv)

az role assignment create \
--assignee-object-id "$VM_IDENTITY" \
--assignee-principal-type ServicePrincipal \
--role "Storage Blob Data Contributor" \
--scope "/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RG/providers/Microsoft.Storage/storageAccounts/$STORAGE" \
--output none

log "Role assigned. Waiting 30s for propagation..."
sleep 30

# ── Step 3: Transfer DB with fallback (AWS S3 → GCP GCS → NCBI FTP) ──
log "Step 3/4: Transferring DB to Azure Blob..."

TRANSFER_START=$(date +%s)

# Get VM public IP
VM_IP=$(az vm show -g "$RG" -n "$VM_NAME" -d --query "publicIps" -o tsv)
log "VM IP: $VM_IP"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=30"

# Wait for SSH to become available
log "Waiting for SSH..."
for i in $(seq 1 30); do
    if ssh $SSH_OPTS azureuser@"$VM_IP" "echo ok" &>/dev/null; then
        break
    fi
    sleep 5
done

# Generate transfer script and upload to VM
REMOTE_SCRIPT="/tmp/transfer-db.sh"
cat <<VMSCRIPT | ssh $SSH_OPTS azureuser@"$VM_IP" "cat > $REMOTE_SCRIPT && chmod +x $REMOTE_SCRIPT"
#!/bin/bash
set -euo pipefail

DB_NAME="${DB_NAME}"
BLOB_URL="${BLOB_URL}"
S3_URL="${S3_URL}"
GCS_URL="${GCS_URL}"
FTP_URL="${FTP_URL}"

# ── Install azcopy ──
echo "=== Installing azcopy ==="
cd /tmp
curl -sL 'https://aka.ms/downloadazcopy-v10-linux' | tar xz --strip-components=1
sudo mv azcopy /usr/local/bin/
export AZCOPY_AUTO_LOGIN_TYPE=MSI

# ── Source 1: AWS S3 (fastest, server-to-server) ──
try_s3() {
    echo ""
    echo "=== [1/3] Trying AWS S3 ==="
    if ! curl -sf --head "\${S3_URL}/latest/\${DB_NAME}.00.tar.gz" >/dev/null 2>&1; then
        echo "NOT FOUND on S3."
        return 1
    fi
    echo "Found on S3. Starting azcopy transfer..."
    azcopy cp "\${S3_URL}/latest/\${DB_NAME}*" "\${BLOB_URL}/\${DB_NAME}/" \
        --block-size-mb=256 --cap-mbps=0 --log-level=WARNING --overwrite=ifSourceNewer
    return 0
}

# ── Source 2: GCP GCS ──
try_gcs() {
    echo ""
    echo "=== [2/3] Trying GCP GCS ==="
    local count
    count=\$(curl -sf "https://storage.googleapis.com/storage/v1/b/blast-db/o?prefix=latest/\${DB_NAME}.00&maxResults=1" 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('items',[])))" 2>/dev/null || echo "0")
    if [[ "\$count" == "0" ]]; then
        echo "NOT FOUND on GCS."
        return 1
    fi
    echo "Found on GCS. Starting azcopy transfer..."
    azcopy cp "\${GCS_URL}/latest/\${DB_NAME}*" "\${BLOB_URL}/\${DB_NAME}/" \
        --block-size-mb=256 --cap-mbps=0 --log-level=WARNING --overwrite=ifSourceNewer
    return 0
}

# ── Source 3: NCBI FTP (download tar.gz → extract → upload) ──
try_ftp() {
    echo ""
    echo "=== [3/3] Using NCBI FTP ==="
    if ! curl -sf --head "\${FTP_URL}/\${DB_NAME}.00.tar.gz" >/dev/null 2>&1; then
        echo "NOT FOUND on NCBI FTP. Database '\${DB_NAME}' does not exist."
        return 1
    fi

    # Discover volume count
    echo "Discovering volumes..."
    local vol_count=0
    while curl -sf --head "\${FTP_URL}/\${DB_NAME}.\$(printf '%02d' \$vol_count).tar.gz" >/dev/null 2>&1; do
        vol_count=\$((vol_count + 1))
        [[ \$vol_count -ge 200 ]] && break
    done
    echo "Found \${vol_count} volumes."

    # Use /mnt temp disk (largest disk on Azure VMs)
    WORK_DIR="/mnt/blast-staging"
    DL_DIR="\${WORK_DIR}/downloads"
    sudo mkdir -p "\$DL_DIR"
    sudo chown -R azureuser:azureuser "\$WORK_DIR"
    cd "\$WORK_DIR"

    echo "Disk space:"
    df -h /mnt | tail -1

    PARALLEL=8
    echo "Downloading \${vol_count} volumes (\${PARALLEL} parallel)..."

    download_extract() {
        local i=\$1
        local padded=\$(printf "%02d" \$i)
        local file="\${DB_NAME}.\${padded}.tar.gz"
        echo "  [\${padded}] Downloading \${file}..."
        curl -sf -o "\${DL_DIR}/\${file}" "\${FTP_URL}/\${file}"
        echo "  [\${padded}] Extracting..."
        tar xzf "\${DL_DIR}/\${file}" -C "\${WORK_DIR}"
        rm -f "\${DL_DIR}/\${file}"
        echo "  [\${padded}] Done."
    }
    export -f download_extract
    export DB_NAME FTP_URL WORK_DIR DL_DIR

    seq 0 \$((vol_count - 1)) | xargs -P \$PARALLEL -I{} bash -c 'download_extract {}'

    echo ""
    echo "Extraction complete."
    echo "Total files: \$(ls "\$WORK_DIR"/\${DB_NAME}* 2>/dev/null | wc -l)"
    echo "Total size: \$(du -sh "\$WORK_DIR" | cut -f1)"

    echo ""
    echo "Uploading to Azure Blob..."
    azcopy cp "\${WORK_DIR}/\${DB_NAME}*" "\${BLOB_URL}/\${DB_NAME}/" \
        --block-size-mb=256 --cap-mbps=0 --log-level=WARNING --overwrite=ifSourceNewer

    sudo rm -rf "\$WORK_DIR"
    return 0
}

# ── Execute with fallback ──
if try_s3; then
    echo "SOURCE=S3"
elif try_gcs; then
    echo "SOURCE=GCS"
elif try_ftp; then
    echo "SOURCE=FTP"
else
    echo "FATAL: Database '\${DB_NAME}' not found on any source."
    exit 1
fi

echo ""
echo "=== Transfer complete ==="
azcopy ls "\${BLOB_URL}/\${DB_NAME}/" 2>/dev/null | wc -l
echo "files uploaded"
VMSCRIPT

log "Transfer script uploaded. Executing on VM via SSH..."
log "(Fallback order: S3 -> GCS -> FTP)"

# Run the script on the VM (live output via SSH)
ssh $SSH_OPTS azureuser@"$VM_IP" "sudo bash $REMOTE_SCRIPT"

TRANSFER_TIME=$(( $(date +%s) - TRANSFER_START ))
log "Transfer completed in ${TRANSFER_TIME}s ($((TRANSFER_TIME / 60)) min)"

# ── Step 4: Verify ──
log "Step 4/4: Verifying upload..."

FILE_COUNT=$(AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy ls "${BLOB_URL}/${DB_NAME}/" 2>/dev/null | wc -l)
log "Files in ${BLOB_URL}/${DB_NAME}/: $FILE_COUNT"

TOTAL_SIZE=$(az storage blob list --account-name "$STORAGE" -c "$CONTAINER" \
    --auth-mode login --prefix "${DB_NAME}/" \
    --query "[].properties.contentLength" -o tsv 2>/dev/null \
    | python3 -c "import sys; vals=[int(x) for x in sys.stdin.read().split() if x]; print(f'{sum(vals)/(1024**3):.1f}')" \
2>/dev/null || echo "unknown")

log "Total size: ${TOTAL_SIZE} GB"

TOTAL_TIME=$(( $(date +%s) - START_TIME ))

echo ""
echo "============================================"
echo " Summary"
echo "============================================"
echo " Database:      $DB_NAME"
echo " VM SKU:        $VM_SKU"
echo " VM create:     ${VM_CREATE_TIME}s"
echo " Transfer:      ${TRANSFER_TIME}s ($((TRANSFER_TIME / 60)) min)"
echo " Total:         ${TOTAL_TIME}s ($((TOTAL_TIME / 60)) min)"
echo " Files:         $FILE_COUNT"
echo " Size:          ${TOTAL_SIZE} GB"
echo " Location:      $BLOB_URL/$DB_NAME/"
echo ""
echo " Use in ElasticBLAST config:"
echo "   db = $BLOB_URL/$DB_NAME/$DB_NAME"
echo "============================================"
echo ""
log "VM cleanup will run automatically (trap EXIT)..."
