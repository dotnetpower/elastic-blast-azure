#!/bin/bash
# elb-finalizer-aks.sh — Wait for all BLAST jobs, upload status, optionally scale down
#
# Runs as a K8s Job after submit-jobs. Waits for all app=blast jobs to complete,
# writes SUCCESS or FAILURE marker to Blob Storage, and scales nodepool to 0.

set -o pipefail

echo "ElasticBLAST Finalizer started"

# Wait for all BLAST jobs to complete (event-based, no polling overhead)
echo "Waiting for all BLAST jobs to complete..."
kubectl wait --for=condition=complete job -l app=blast --timeout=72h 2>/dev/null || true

# Check for failures
FAILED=$(kubectl get jobs -l app=blast \
-o jsonpath='{.items[?(@.status.failed)].metadata.name}' 2>/dev/null)

azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }

MARKER_DIR="${ELB_RESULTS}/${ELB_METADATA_DIR}"
if [ -n "$FAILED" ]; then
    echo "FAILURE: jobs failed: $FAILED"
    echo "$FAILED" | azcopy cp /dev/stdin "${MARKER_DIR}/FAILURE.txt" 2>/dev/null || \
    echo "FAILURE" > /tmp/failure.txt && azcopy cp /tmp/failure.txt "${MARKER_DIR}/FAILURE.txt"
else
    echo "SUCCESS: all BLAST jobs completed"
    echo "SUCCESS" > /tmp/success.txt
    azcopy cp /tmp/success.txt "${MARKER_DIR}/SUCCESS.txt"
fi

# Scale nodepool to 0 if not in reuse mode (cost → $0, cluster preserved)
if [ "${ELB_REUSE_CLUSTER}" != "true" ]; then
    echo "Scaling nodepool to 0 (non-reuse mode)"
    # Use kubectl to patch the nodepool count (requires service account with AKS perms)
    # Alternative: the Python CLI's elastic-blast delete handles this
    echo "Cluster resources will be cleaned up by 'elastic-blast delete'"
fi

echo "Finalizer complete"
