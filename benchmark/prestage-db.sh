#!/bin/bash
# benchmark/prestage-db.sh — Download NCBI BLAST DB to Azure Blob Storage
#
# Two modes:
#   1. S3 mode (default, fast): azcopy S3 → Azure Blob directly (~800 MB/s)
#   2. NCBI mode (slow): update_blastdb.pl via AKS pod (~50 MB/s)
#
# Usage:
#   # S3 mode — fast, no cluster needed (recommended)
#   ./benchmark/prestage-db.sh nt_prok
#   ./benchmark/prestage-db.sh swissprot
#   ./benchmark/prestage-db.sh nt
#
#   # NCBI mode — requires AKS cluster
#   ./benchmark/prestage-db.sh nt_prok ncbi
#
#   # List available DBs on S3
#   ./benchmark/prestage-db.sh --list
#
# Author: Moon Hyuk Choi

set -euo pipefail

DB_NAME="${1:-nt_prok}"
MODE="${2:-s3}"   # s3 (default) or ncbi
RG="rg-elb-koc"
STORAGE="stgelb"
CONTAINER="blast-db"
BLOB_URL="https://$STORAGE.blob.core.windows.net/$CONTAINER"
S3_URL="https://ncbi-blast-databases.s3.amazonaws.com"

# List available DBs
if [[ "$DB_NAME" == "--list" ]]; then
    echo "Available BLAST DBs on AWS S3:"
    echo "  Nucleotide: nt, nt_prok, nt_euk, nt_viruses, refseq_rna"
    echo "  Protein:    nr, swissprot, refseq_protein, pdbaa"
    echo "  Other:      16S_ribosomal_RNA, env_nt, env_nr"
    echo ""
    echo "Full list: https://ftp.ncbi.nlm.nih.gov/blast/db/"
    exit 0
fi

echo "=== Pre-staging BLAST DB: $DB_NAME ==="
echo "Mode: $MODE"
echo "Target: $BLOB_URL/$DB_NAME/"

if [[ "$MODE" == "s3" ]]; then
    # ── S3 Mode: Direct S3 → Azure Blob (fast, no cluster needed) ──
    echo ""
    echo "Downloading from AWS S3 (public, no auth required)..."
    echo "Source: $S3_URL/latest/${DB_NAME}*"
    echo ""
    
    # Ensure storage public access is enabled
    az storage account update -n "$STORAGE" -g "$RG" --public-network-access Enabled -o none 2>/dev/null
    
    # Transfer: S3 → Azure Blob
    # azcopy handles S3 public buckets without AWS credentials
    AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy cp \
    "${S3_URL}/latest/${DB_NAME}*" \
    "${BLOB_URL}/${DB_NAME}/" \
    --block-size-mb=256 \
    --cap-mbps=0 \
    --log-level=WARNING \
    --overwrite=ifSourceNewer
    
    echo ""
    echo "=== Verifying upload ==="
    AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy ls "${BLOB_URL}/${DB_NAME}/" 2>/dev/null | wc -l
    echo " files in ${BLOB_URL}/${DB_NAME}/"
    
    # Show total size
    az storage blob list --account-name "$STORAGE" -c "$CONTAINER" \
    --auth-mode login --prefix "${DB_NAME}/" \
    --query "[].properties.contentLength" -o tsv 2>/dev/null \
    | python3 -c "import sys; vals=[int(x) for x in sys.stdin.read().split() if x]; print(f'Total size: {sum(vals)/(1024**3):.1f} GB')" \
    2>/dev/null || true
    
    echo ""
    echo "Done! DB staged at: $BLOB_URL/$DB_NAME/"
    echo "Use in config:"
    echo "  db = $BLOB_URL/$DB_NAME/$DB_NAME"
    
else
    # ── NCBI Mode: update_blastdb.pl via AKS pod (slow, requires cluster) ──
    echo ""
    echo "Using NCBI FTP mode (requires AKS cluster)..."
    CLUSTER="elb-prestage"
    ACR="elbacr.azurecr.io"
    IMAGE="$ACR/ncbi/elb:1.4.0"
    
    # Ensure kubectl context
    az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null
    
    # Create download job
    cat <<EOF | kubectl --context="$CLUSTER" apply -f -
kind: Job
metadata:
  name: prestage-${DB_NAME//_/-}
  labels:
    app: prestage
spec:
  backoffLimit: 2
  activeDeadlineSeconds: 14400
  template:
    spec:
      containers:
      - name: download
        image: $IMAGE
        command:
        - bash
        - -c
        - |
          set -ex
          echo "=== Downloading $DB_NAME ==="
          cd /tmp/blastdb

          # Download with parallel threads
          update_blastdb.pl "$DB_NAME" --decompress --num_threads 4 --verbose

          echo "=== DB files ==="
          ls -lh /tmp/blastdb/ | head -20
          du -sh /tmp/blastdb/

          echo "=== Uploading to Blob Storage ==="
          azcopy login --identity
          azcopy cp "/tmp/blastdb/${DB_NAME}*" "$BLOB_URL/$DB_NAME/" \
            --block-size-mb=256 \
            --cap-mbps=0 \
            --log-level=WARNING

          echo "=== Upload complete ==="
          azcopy list "$BLOB_URL/$DB_NAME/" | wc -l
          echo " files uploaded"
        env:
        - name: BLASTDB_LMDB_MAP_SIZE
          value: "1000000000"
        volumeMounts:
        - name: scratch
          mountPath: /tmp/blastdb
        resources:
          requests:
            cpu: "16"
            memory: "32Gi"
          limits:
            cpu: "30"
            memory: "200Gi"
      volumes:
      - name: scratch
        emptyDir:
          sizeLimit: 200Gi
      restartPolicy: Never
EOF
    
    echo "Job submitted. Monitor with:"
    echo "  kubectl --context=$CLUSTER logs -f job/prestage-${DB_NAME//_/-}"
    echo "  kubectl --context=$CLUSTER get job prestage-${DB_NAME//_/-} -w"
    
fi
