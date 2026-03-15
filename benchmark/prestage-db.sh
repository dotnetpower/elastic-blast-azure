#!/bin/bash
# benchmark/prestage-db.sh — Download NCBI BLAST DB to Azure Blob Storage
#
# Uses update_blastdb.pl inside an AKS pod, then uploads to Blob via azcopy.
# Designed to run on existing AKS cluster with Managed Identity.
#
# Usage:
#   # Create cluster first:
#   az aks create -g rg-elb-koc -n elb-prestage -l koreacentral \
#     --node-count 1 --node-vm-size Standard_E32s_v3 --generate-ssh-keys
#
#   # Then run this script:
#   ./benchmark/prestage-db.sh nt_prok
#
# Author: Moon Hyuk Choi

set -euo pipefail

DB_NAME="${1:-nt_prok}"
RG="rg-elb-koc"
CLUSTER="elb-prestage"
STORAGE="stgelb"
CONTAINER="blast-db"
ACR="elbacr.azurecr.io"
IMAGE="$ACR/ncbi/elb:1.4.0"
BLOB_URL="https://$STORAGE.blob.core.windows.net/$CONTAINER"

echo "=== Pre-staging BLAST DB: $DB_NAME ==="
echo "Target: $BLOB_URL/$DB_NAME/"

# Ensure kubectl context
az aks get-credentials -g "$RG" -n "$CLUSTER" --overwrite-existing 2>/dev/null

# Create download job
cat <<EOF | kubectl --context="$CLUSTER" apply -f -
apiVersion: batch/v1
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
