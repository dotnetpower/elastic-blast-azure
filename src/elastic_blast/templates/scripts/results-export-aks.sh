#!/bin/bash
# results-export-aks.sh — Upload BLAST results to Azure Blob Storage
#
# Environment variables (set by K8s pod spec):
#   ELB_BLAST_PROGRAM - BLAST program name
#   ELB_DB            - BLAST database name
#   ELB_RESULTS       - Azure Blob Storage URL for results
#   JOB_NUM           - Batch job number
#   RESULTS_DIR       - Directory containing output files

# Wait for BLAST to finish (sidecar pattern)
until [ -s "$RESULTS_DIR/BLAST_EXIT_CODE.out" ]; do
    sleep 1
done

azcopy login --identity
set -ex

# Upload metadata and results
ls -1f "$RESULTS_DIR/BLASTDB_LENGTH.out"
azcopy cp "$RESULTS_DIR/BLASTDB_LENGTH.out" "$ELB_RESULTS/metadata/"
azcopy cp "$RESULTS_DIR/BLAST_RUNTIME-${JOB_NUM}.out" "$ELB_RESULTS/logs/"
azcopy cp "$RESULTS_DIR/batch_${JOB_NUM}-${ELB_BLAST_PROGRAM}-${ELB_DB}.out.gz" "$ELB_RESULTS/"

exit "$(cat "$RESULTS_DIR/BLAST_EXIT_CODE.out")"
