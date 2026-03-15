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

azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }

# Optimize azcopy transfer performance
export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY_VALUE:-64}
export AZCOPY_BUFFER_GB=${AZCOPY_BUFFER_GB:-4}

# Retry wrapper for transient network errors (3 attempts, exponential backoff)
retry_azcopy() {
    local max_attempts=3 attempt=1 wait=5
    while [ $attempt -le $max_attempts ]; do
        if azcopy "$@"; then return 0; fi
        echo "azcopy attempt $attempt/$max_attempts failed, retrying in ${wait}s..."
        sleep $wait; wait=$((wait * 2)); attempt=$((attempt + 1))
    done
    echo "ERROR: azcopy failed after $max_attempts attempts"; return 1
}

if [ -z "$ELB_DB_PATH" ]; then
    # NCBI standard database: parallel download threads
    echo "update_blastdb.pl $ELB_DB --decompress --source $ELB_BLASTDB_SRC --num_threads ${ELB_NUM_DL_THREADS:-4}"
    update_blastdb.pl "$ELB_DB" --decompress --source "$ELB_BLASTDB_SRC" \
    --num_threads "${ELB_NUM_DL_THREADS:-4}" \
    --verbose --verbose --verbose --verbose --verbose --verbose
    exit_code=$?
    [ $exit_code -eq 0 ] || exit $exit_code
else
    # Custom database from Azure Blob Storage (optimized block size)
    echo "Downloading custom DB: $ELB_DB_PATH"
    retry_azcopy cp "$ELB_DB_PATH" . --block-size-mb=256 --log-level=WARNING
    exit_code=$?
    [ $exit_code -eq 0 ] || exit $exit_code
    [ -f "${ELB_DB}.tar.gz" ] && tar xzf "${ELB_DB}.tar.gz" && rm "${ELB_DB}.tar.gz"
fi

# Taxonomy database: only download if taxid filtering is used
if [ -n "$ELB_TAXIDLIST" ]; then
    echo "Downloading taxdb for taxid filtering"
    update_blastdb.pl taxdb --decompress --source "$ELB_BLASTDB_SRC" \
    --verbose --verbose --verbose --verbose --verbose --verbose
fi

end=$(date +%s)
log "download-blastdbs" $((end - start))

# DB integrity verification (skip for custom DBs with ELB_SKIP_DB_VERIFY=true)
if [ "${ELB_SKIP_DB_VERIFY:-false}" != "true" ]; then
    blastdbcmd -info -db "$ELB_DB" -dbtype "$ELB_DB_MOL_TYPE" || exit $?
    blastdbcheck -db "$ELB_DB" -dbtype "$ELB_DB_MOL_TYPE" -no_isam -ends 5 || exit $?
else
    echo "Skipping DB verification (ELB_SKIP_DB_VERIFY=true)"
fi

# Optional taxid list download
if [ -n "$ELB_TAXIDLIST" ]; then
    retry_azcopy cp "$ELB_TAXIDLIST" /blast/blastdb || exit 1
fi

exit ${exit_code:-0}
