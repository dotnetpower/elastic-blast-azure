#!/bin/bash
# init-db-partitioned-aks.sh — Download partitioned BLAST database
#
# Downloads N database partitions from Azure Blob Storage, each to its own
# subdirectory on the PVC: /blast/blastdb/part_00/, part_01/, etc.
#
# Environment variables (set by K8s pod spec):
#   ELB_NUM_PARTITIONS    - Number of DB partitions to download
#   ELB_PARTITION_PREFIX  - Azure Blob URL prefix (e.g., https://.../dbname/part_)

set -o pipefail

echo "Downloading $ELB_NUM_PARTITIONS DB partitions"

azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }
start=$(date +%s)

# Optimize azcopy for parallel downloads
export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY_VALUE:-64}
export AZCOPY_BUFFER_GB=${AZCOPY_BUFFER_GB:-4}

# Retry wrapper for transient network errors
retry_azcopy() {
    local max_attempts=3 attempt=1 wait=5
    while [ $attempt -le $max_attempts ]; do
        if azcopy "$@"; then return 0; fi
        echo "azcopy attempt $attempt/$max_attempts failed, retrying in ${wait}s..."
        sleep $wait; wait=$((wait * 2)); attempt=$((attempt + 1))
    done
    echo "ERROR: azcopy failed after $max_attempts attempts"; return 1
}

for i in $(seq 0 $((ELB_NUM_PARTITIONS - 1))); do
    part_idx=$(printf '%02d' "$i")
    part_url="${ELB_PARTITION_PREFIX}${part_idx}/*"
    part_dir="part_${part_idx}"
    
    echo "Downloading partition $i from $part_url"
    mkdir -p "/blast/blastdb/${part_dir}"
    retry_azcopy cp "$part_url" "/blast/blastdb/${part_dir}/" --recursive --block-size-mb=256 --log-level=WARNING
    exit_code=$?
    [ $exit_code -eq 0 ] || exit $exit_code
done

end=$(date +%s)
echo "RUNTIME download-partitions $((end - start)) seconds"

for i in $(seq 0 $((ELB_NUM_PARTITIONS - 1))); do
    part_idx=$(printf '%02d' "$i")
    part_url="${ELB_PARTITION_PREFIX}${part_idx}/*"
    part_dir="part_${part_idx}"

    echo "Downloading partition $i from $part_url"
    mkdir -p "/blast/blastdb/${part_dir}"
    retry_azcopy cp "$part_url" "/blast/blastdb/${part_dir}/" --recursive --block-size-mb=256 --log-level=WARNING
    exit_code=$?
    [ $exit_code -eq 0 ] || exit $exit_code
done

end=$(date +%s)
echo "RUNTIME download-partitions $((end - start)) seconds"

# Clean up azcopy background processes to ensure container exits cleanly
pkill -f azcopy 2>/dev/null || true
rm -rf /root/.azcopy 2>/dev/null || true
