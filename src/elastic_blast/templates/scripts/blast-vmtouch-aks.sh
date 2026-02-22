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

# Touch all DB volume files into page cache
blastdb_path -dbtype "$ELB_DB_MOL_TYPE" -db "$ELB_DB" -getvolumespath \
    | tr ' ' '\n' \
    | parallel vmtouch -tqm 5G

mkdir -p results
exit_code=$?

end=$(date +%s)
log "cache-blastdbs-to-ram" $((end - start))
exit $exit_code
