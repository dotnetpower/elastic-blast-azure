#!/usr/bin/env bash
# Periodically enable public network access on the storage account.
# Azure tenant policy may re-disable it; this script runs in a loop
# to ensure it stays enabled during benchmark execution.
#
# Usage:
#   ./benchmark/enable-public-access.sh              # default: every 5 min
#   ./benchmark/enable-public-access.sh 120           # every 2 min
#   ./benchmark/enable-public-access.sh 300 stgelb2   # custom interval + account

set -euo pipefail

INTERVAL="${1:-300}"
STORAGE_ACCOUNT="${2:-stgelb}"
RESOURCE_GROUP="${3:-rg-elb-koc}"

echo "[$(date '+%H:%M:%S')] Monitoring public network access for '${STORAGE_ACCOUNT}' every ${INTERVAL}s"
echo "Press Ctrl+C to stop"

while true; do
    current=$(az storage account show \
        --name "$STORAGE_ACCOUNT" \
        --resource-group "$RESOURCE_GROUP" \
    --query "publicNetworkAccess" -o tsv 2>/dev/null || echo "UNKNOWN")
    
    if [[ "$current" != "Enabled" ]]; then
        echo "[$(date '+%H:%M:%S')] Public access is '${current}' — re-enabling..."
        az storage account update \
        --name "$STORAGE_ACCOUNT" \
        --resource-group "$RESOURCE_GROUP" \
        --public-network-access Enabled \
        --output none 2>&1 && \
        echo "[$(date '+%H:%M:%S')] Public access re-enabled." || \
        echo "[$(date '+%H:%M:%S')] ERROR: Failed to enable public access."
    else
        echo "[$(date '+%H:%M:%S')] Public access is Enabled — OK"
    fi
    
    sleep "$INTERVAL"
done
