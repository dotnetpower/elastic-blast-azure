#!/bin/bash
set -euo pipefail

DB_NAME="core_nt"
BLOB_URL="https://stgelb.blob.core.windows.net/blast-db"
FTP_URL="https://ftp.ncbi.nlm.nih.gov/blast/db"

echo "=== Installing azcopy ==="
cd /tmp
curl -sL 'https://aka.ms/downloadazcopy-v10-linux' | tar xz --strip-components=1
sudo mv azcopy /usr/local/bin/
export AZCOPY_AUTO_LOGIN_TYPE=MSI

echo "=== Checking sources ==="

# Source 1: AWS S3
if curl -sf --head "https://ncbi-blast-databases.s3.amazonaws.com/latest/${DB_NAME}.00.tar.gz" >/dev/null 2>&1; then
    echo "Found on S3!"
    azcopy cp "https://ncbi-blast-databases.s3.amazonaws.com/latest/${DB_NAME}*" "${BLOB_URL}/${DB_NAME}/" --block-size-mb=256 --log-level=WARNING --overwrite=ifSourceNewer
    echo "SOURCE=S3"
    exit 0
fi
echo "Not on S3."

# Source 2: GCP
COUNT=$(curl -sf "https://storage.googleapis.com/storage/v1/b/blast-db/o?prefix=latest/${DB_NAME}.00&maxResults=1" 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);print(len(d.get('items',[])))" 2>/dev/null || echo "0")
if [ "$COUNT" != "0" ]; then
    echo "Found on GCS!"
    azcopy cp "https://storage.googleapis.com/blast-db/latest/${DB_NAME}*" "${BLOB_URL}/${DB_NAME}/" --block-size-mb=256 --log-level=WARNING --overwrite=ifSourceNewer
    echo "SOURCE=GCS"
    exit 0
fi
echo "Not on GCS."

# Source 3: NCBI FTP
echo "=== Using NCBI FTP ==="
if ! curl -sf --head "${FTP_URL}/${DB_NAME}.00.tar.gz" >/dev/null 2>&1; then
    echo "FATAL: ${DB_NAME} not found on any source."
    exit 1
fi

# Count volumes
VOL=0
while curl -sf --head "${FTP_URL}/${DB_NAME}.$(printf '%02d' $VOL).tar.gz" >/dev/null 2>&1; do
    VOL=$((VOL + 1))
    [ $VOL -ge 200 ] && break
done
echo "Found ${VOL} volumes."

WORK="/mnt/blast-staging"
DL="${WORK}/downloads"
sudo mkdir -p "$DL"
sudo chown -R $(whoami) "$WORK"

echo "Disk space:"
df -h /mnt | tail -1

download_extract() {
    local i=$1
    local p=$(printf "%02d" $i)
    local f="${DB_NAME}.${p}.tar.gz"
    echo "[${p}/${VOL}] Downloading ${f}..."
    curl -sf -o "${DL}/${f}" "${FTP_URL}/${f}"
    echo "[${p}/${VOL}] Extracting..."
    tar xzf "${DL}/${f}" -C "${WORK}"
    rm -f "${DL}/${f}"
    echo "[${p}/${VOL}] Done."
}
export -f download_extract
export DB_NAME FTP_URL WORK DL VOL

seq 0 $((VOL - 1)) | xargs -P 8 -I{} bash -c 'download_extract {}'

echo ""
echo "=== Extraction complete ==="
echo "Files: $(ls ${WORK}/${DB_NAME}* 2>/dev/null | wc -l)"
echo "Size: $(du -sh ${WORK} | cut -f1)"

echo ""
echo "=== Uploading to Blob ==="
azcopy cp "${WORK}/${DB_NAME}*" "${BLOB_URL}/${DB_NAME}/" --block-size-mb=256 --log-level=WARNING --overwrite=ifSourceNewer

echo ""
echo "=== Upload complete ==="
azcopy ls "${BLOB_URL}/${DB_NAME}/" 2>/dev/null | wc -l
echo "files uploaded"

sudo rm -rf "$WORK"
echo "SOURCE=FTP"
