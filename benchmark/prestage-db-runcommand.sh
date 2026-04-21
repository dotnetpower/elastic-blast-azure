#!/bin/bash
# benchmark/prestage-db-runcommand.sh — Pre-stage BLAST DB via Azure VM run-command
#
# Uses az vm run-command invoke (no SSH needed).
# Creates VM → runs transfer → verifies → deletes VM.
#
# Usage:
#   ./benchmark/prestage-db-runcommand.sh core_nt
#
# Author: Moon Hyuk Choi

set -euo pipefail

DB_NAME="${1:?Usage: $0 <db_name>}"
RG="rg-elb-koc"
STORAGE="stgelb"
LOCATION="koreacentral"
VM_NAME="elb-prestage-vm"
VM_SKU="${DB_VM_SKU:-Standard_D64s_v3}"
VM_IMAGE="Canonical:ubuntu-24_04-lts:server:latest"
BLOB_URL="https://$STORAGE.blob.core.windows.net/blast-db"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'
log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')]${NC} $*" >&2; }

cleanup() {
    local ec=$?
    echo ""
    [[ $ec -eq 0 ]] && log "Done. Cleaning up..." || err "Failed. Cleaning up..."
    
    # Restore storage network security
    log "Disabling storage public network access..."
    az storage account update -n "$STORAGE" -g "$RG" --public-network-access Disabled -o none 2>/dev/null || true
    
    az vm delete -g "$RG" -n "$VM_NAME" --yes --force-deletion true 2>/dev/null || true
    az network vnet delete -g "$RG" -n "${VM_NAME}VNET" 2>/dev/null || true
    local disk_id
    disk_id=$(az disk list -g "$RG" --query "[?starts_with(name,'${VM_NAME}')].id" -o tsv 2>/dev/null || true)
    [[ -n "$disk_id" ]] && az disk delete --ids "$disk_id" --yes 2>/dev/null || true
    for r in "${VM_NAME}VMNic" "${VM_NAME}NSG" "${VM_NAME}PublicIP"; do
        az resource delete -g "$RG" -n "$r" --resource-type "Microsoft.Network/$(echo $r | sed 's/.*VM/networkInterfaces/;s/.*NSG/networkSecurityGroups/;s/.*Public/publicIPAddresses/')" 2>/dev/null || true
    done
    log "Cleanup complete."
}
trap cleanup EXIT

START_TIME=$(date +%s)
log "=== Pre-staging $DB_NAME via VM ($VM_SKU) ==="

# ── Step 1: Create VM ──
log "Step 1/4: Creating VM..."
[[ $(az vm show -g "$RG" -n "$VM_NAME" 2>/dev/null | wc -c) -gt 2 ]] && {
    warn "Existing VM found, deleting..."
    az vm delete -g "$RG" -n "$VM_NAME" --yes --force-deletion true 2>/dev/null
    sleep 10
}

az vm create -g "$RG" -n "$VM_NAME" --image "$VM_IMAGE" --size "$VM_SKU" \
--os-disk-size-gb 64 --storage-sku StandardSSD_LRS \
--assign-identity "[system]" --admin-username azureuser \
--generate-ssh-keys --public-ip-sku Standard \
--accelerated-networking true --output none
log "VM created in $(($(date +%s) - START_TIME))s"

# ── Step 2: Assign role ──
log "Step 2/4: Assigning Storage Blob Data Contributor..."
VM_ID=$(az vm show -g "$RG" -n "$VM_NAME" --query "identity.principalId" -o tsv)
SUB_ID=$(az account show --query id -o tsv)
az role assignment create --assignee-object-id "$VM_ID" \
--assignee-principal-type ServicePrincipal \
--role "Storage Blob Data Contributor" \
--scope "/subscriptions/$SUB_ID/resourceGroups/$RG/providers/Microsoft.Storage/storageAccounts/$STORAGE" \
--output none
log "Role assigned. Waiting 30s..."
sleep 30

# ── Step 3: Transfer via run-command ──
log "Step 3/4: Running transfer on VM (this may take 30-60 min)..."

# Enable storage public access temporarily (VM needs it for azcopy upload)
log "Enabling storage public network access (will be disabled on cleanup)..."
az storage account update -n "$STORAGE" -g "$RG" --public-network-access Enabled -o none
sleep 10

log "  Source: NCBI FTP -> S3 -> GCS (auto-detect)"
log "  Target: $BLOB_URL/$DB_NAME/"

XFER_START=$(date +%s)

# Write the transfer script to a temp file to avoid quoting issues
SCRIPT_FILE=$(mktemp /tmp/prestage-XXXXX.sh)
cat > "$SCRIPT_FILE" << 'ENDSCRIPT'
#!/bin/bash
set -euo pipefail
DB_NAME="__DBNAME__"
BLOB_URL="__BLOBURL__"
FTP_URL="https://ftp.ncbi.nlm.nih.gov/blast/db"
S3_URL="https://ncbi-blast-databases.s3.amazonaws.com"

echo "=== Installing azcopy ==="
cd /tmp && curl -sL 'https://aka.ms/downloadazcopy-v10-linux' | tar xz --strip-components=1
sudo mv azcopy /usr/local/bin/ && export AZCOPY_AUTO_LOGIN_TYPE=MSI

# Try S3 first
if curl -sf --head "${S3_URL}/latest/${DB_NAME}.00.tar.gz" >/dev/null 2>&1; then
    echo "=== Found on S3 ==="
    azcopy cp "${S3_URL}/latest/${DB_NAME}*" "${BLOB_URL}/${DB_NAME}/" --block-size-mb=256 --log-level=WARNING --overwrite=ifSourceNewer
    echo "DONE SOURCE=S3"
    exit 0
fi
echo "Not on S3."

# Try GCS
GCS_COUNT=$(curl -sf "https://storage.googleapis.com/storage/v1/b/blast-db/o?prefix=latest/${DB_NAME}.00&maxResults=1" 2>/dev/null | python3 -c "import json,sys;print(len(json.load(sys.stdin).get('items',[])))" 2>/dev/null || echo 0)
if [ "$GCS_COUNT" != "0" ]; then
    echo "=== Found on GCS ==="
    azcopy cp "https://storage.googleapis.com/blast-db/latest/${DB_NAME}*" "${BLOB_URL}/${DB_NAME}/" --block-size-mb=256 --log-level=WARNING --overwrite=ifSourceNewer
    echo "DONE SOURCE=GCS"
    exit 0
fi
echo "Not on GCS."

# FTP fallback
echo "=== Using NCBI FTP ==="
VOL=0
while curl -sf --head "${FTP_URL}/${DB_NAME}.$(printf '%02d' $VOL).tar.gz" >/dev/null 2>&1; do
    VOL=$((VOL+1)); [ $VOL -ge 200 ] && break
done
echo "Found ${VOL} volumes"

W="/mnt/blast-staging"; DL="$W/dl"
sudo mkdir -p "$DL" && sudo chown -R $(whoami) "$W"
echo "Disk:"; df -h /mnt | tail -1

dlex() {
    local p=$(printf "%02d" $1)
    echo "[${p}] downloading..."
    curl -sf -o "${DL}/${DB_NAME}.${p}.tar.gz" "${FTP_URL}/${DB_NAME}.${p}.tar.gz"
    echo "[${p}] extracting..."
    tar xzf "${DL}/${DB_NAME}.${p}.tar.gz" -C "$W"
    rm -f "${DL}/${DB_NAME}.${p}.tar.gz"
    echo "[${p}] done"
}
export -f dlex; export DB_NAME FTP_URL W DL

seq 0 $((VOL-1)) | xargs -P 8 -I{} bash -c 'dlex {}'

echo "=== FTP download complete ==="
echo "Files: $(ls ${W}/${DB_NAME}* 2>/dev/null | wc -l)"
echo "Size: $(du -sh $W | cut -f1)"

echo "=== Uploading to Blob ==="
azcopy cp "${W}/" "${BLOB_URL}/${DB_NAME}/" --include-pattern "${DB_NAME}*" --block-size-mb=256 --log-level=WARNING --overwrite=ifSourceNewer --recursive

sudo rm -rf "$W"
echo "DONE SOURCE=FTP"
ENDSCRIPT

# Replace placeholders
sed -i "s|__DBNAME__|${DB_NAME}|g; s|__BLOBURL__|${BLOB_URL}|g" "$SCRIPT_FILE"

# Execute on VM
RESULT=$(az vm run-command invoke -g "$RG" -n "$VM_NAME" \
    --command-id RunShellScript \
    --scripts @"$SCRIPT_FILE" \
--query "value[0].message" -o tsv 2>&1)

rm -f "$SCRIPT_FILE"

XFER_TIME=$(($(date +%s) - XFER_START))
log "Transfer completed in ${XFER_TIME}s ($((XFER_TIME/60)) min)"

# Print output
echo "$RESULT" | while IFS= read -r line; do
    line="${line#\[stdout\]}"
    line="${line#\[stderr\]}"
    [[ -n "$line" ]] && echo "  $line"
done

# Check success
if echo "$RESULT" | grep -q "^DONE SOURCE="; then
    SOURCE=$(echo "$RESULT" | grep "^DONE SOURCE=" | cut -d= -f2)
    log "Transfer successful (source: $SOURCE)"
else
    err "Transfer may have failed. Check output above."
    # Don't exit 1 yet — verify in step 4
fi

# ── Step 4: Verify ──
log "Step 4/4: Verifying upload..."
VERIFY=$(az vm run-command invoke -g "$RG" -n "$VM_NAME" \
    --command-id RunShellScript \
    --scripts "export AZCOPY_AUTO_LOGIN_TYPE=MSI; /usr/local/bin/azcopy ls '${BLOB_URL}/${DB_NAME}/' 2>/dev/null | wc -l" \
--query "value[0].message" -o tsv 2>&1)

FILE_COUNT=$(echo "$VERIFY" | grep '\[stdout\]' | grep -oP '\d+' | head -1 || echo "0")
log "Files in blob: ${FILE_COUNT}"

TOTAL_TIME=$(($(date +%s) - START_TIME))
echo ""
echo "============================================"
echo " Summary"
echo "============================================"
echo " Database:  $DB_NAME"
echo " VM SKU:    $VM_SKU"
echo " Transfer:  ${XFER_TIME}s ($((XFER_TIME/60)) min)"
echo " Total:     ${TOTAL_TIME}s ($((TOTAL_TIME/60)) min)"
echo " Files:     ${FILE_COUNT}"
echo " Location:  $BLOB_URL/$DB_NAME/"
echo " Config:    db = $BLOB_URL/$DB_NAME/$DB_NAME"
echo "============================================"

[[ "${FILE_COUNT:-0}" -gt 0 ]] || { err "No files uploaded!"; exit 1; }
