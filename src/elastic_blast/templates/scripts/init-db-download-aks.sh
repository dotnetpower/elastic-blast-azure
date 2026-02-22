#!/bin/bash
# init-db-download-aks.sh — Download BLAST database to PV or local SSD
#
# Environment variables (set by K8s pod spec):
#   ELB_DB            - BLAST database name
#   ELB_DB_PATH       - Azure Blob URL for custom database (empty for NCBI databases)
#   ELB_BLASTDB_SRC   - BLAST database source for update_blastdb.pl
#   ELB_DB_MOL_TYPE   - Database molecule type (nucl/prot)
#   ELB_TAXIDLIST     - Optional taxid list URL
#   STARTUP_DELAY     - Optional delay in seconds (used for local-SSD mode)

set -o pipefail

echo "BASH version ${BASH_VERSION}"

# Optional startup delay (local-SSD mode waits for workspace directory)
if [ -n "$STARTUP_DELAY" ]; then
    echo "Waiting ${STARTUP_DELAY}s for workspace initialization"
    sleep "$STARTUP_DELAY"
fi

start=$(date +%s)

log() {
    local ts
    ts=$(date +'%F %T')
    printf '%s RUNTIME %s %f seconds\n' "$ts" "$1" "$2"
}

azcopy login --identity

if [ -z "$ELB_DB_PATH" ]; then
    # NCBI standard database: use update_blastdb.pl
    echo "update_blastdb.pl $ELB_DB --decompress --source $ELB_BLASTDB_SRC"
    update_blastdb.pl "$ELB_DB" --decompress --source "$ELB_BLASTDB_SRC" \
        --verbose --verbose --verbose --verbose --verbose --verbose
    exit_code=$?
    [ $exit_code -eq 0 ] || exit $exit_code
else
    # Custom database from Azure Blob Storage
    echo "azcopy cp '$ELB_DB_PATH' ."
    azcopy cp "$ELB_DB_PATH" .
    exit_code=$?
    [ $exit_code -eq 0 ] || exit $exit_code
    [ -f "${ELB_DB}.tar.gz" ] && tar xzf "${ELB_DB}.tar.gz"
    [ -f "${ELB_DB}.tar.gz" ] && rm "${ELB_DB}.tar.gz"
fi

# Download taxonomy database
echo "update_blastdb.pl taxdb --decompress --source $ELB_BLASTDB_SRC"
update_blastdb.pl taxdb --decompress --source "$ELB_BLASTDB_SRC" \
    --verbose --verbose --verbose --verbose --verbose --verbose

end=$(date +%s)
log "download-blastdbs" $((end - start))
[ $exit_code -eq 0 ] || exit $exit_code

# Verify database integrity
echo "blastdbcmd -info -db $ELB_DB -dbtype $ELB_DB_MOL_TYPE"
blastdbcmd -info -db "$ELB_DB" -dbtype "$ELB_DB_MOL_TYPE"
exit_code=$?
[ $exit_code -eq 0 ] || exit $exit_code

echo "blastdbcheck -db $ELB_DB -dbtype $ELB_DB_MOL_TYPE -no_isam -ends 5"
blastdbcheck -db "$ELB_DB" -dbtype "$ELB_DB_MOL_TYPE" -no_isam -ends 5
exit_code=$?
[ $exit_code -eq 0 ] || exit $exit_code

# Optional: download taxid list
if [ -n "$ELB_TAXIDLIST" ]; then
    azcopy cp "$ELB_TAXIDLIST" /blast/blastdb
    exit_code=$?
fi

exit $exit_code
