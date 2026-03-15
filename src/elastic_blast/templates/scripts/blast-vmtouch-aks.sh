#!/bin/bash
# blast-vmtouch-aks.sh — Cache BLAST database into RAM using vmtouch
#
# Environment variables (set by K8s pod spec):
#   ELB_DB            - BLAST database name
#   ELB_DB_MOL_TYPE   - Database molecule type (nucl/prot)

set -o pipefail

echo "BASH version ${BASH_VERSION}"
start=$(date +%s)

log() {
    local ts
    ts=$(date +'%F %T')
    printf '%s RUNTIME %s %f seconds\n' "$ts" "$1" "$2"
}

# Use 80% of available RAM instead of hardcoded 5G for large DB support
AVAIL_MEM=$(awk '/MemAvailable/ {print int($2/1024/1024*0.8)"G"}' /proc/meminfo)
echo "vmtouch memory limit: ${AVAIL_MEM}"
blastdb_path -dbtype "$ELB_DB_MOL_TYPE" -db "$ELB_DB" -getvolumespath \
| tr ' ' '\n' \
| parallel vmtouch -tqm "$AVAIL_MEM"

mkdir -p results
exit_code=$?

end=$(date +%s)
log "cache-blastdbs-to-ram" $((end - start))
exit $exit_code
