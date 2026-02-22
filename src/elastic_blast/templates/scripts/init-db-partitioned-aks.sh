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

azcopy login --identity
start=$(date +%s)

for i in $(seq 0 $((ELB_NUM_PARTITIONS - 1))); do
    part_idx=$(printf '%02d' "$i")
    part_url="${ELB_PARTITION_PREFIX}${part_idx}/*"
    part_dir="part_${part_idx}"

    echo "Downloading partition $i from $part_url"
    mkdir -p "/blast/blastdb/${part_dir}"
    azcopy cp "$part_url" "/blast/blastdb/${part_dir}/" --recursive
    exit_code=$?
    [ $exit_code -eq 0 ] || exit $exit_code
done

end=$(date +%s)
echo "RUNTIME download-partitions $((end - start)) seconds"
