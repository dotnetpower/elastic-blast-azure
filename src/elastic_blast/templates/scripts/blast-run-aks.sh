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

azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }
set -ex

# Upload metadata and logs
azcopy cp "$RESULTS_DIR/BLASTDB_LENGTH.out" "$ELB_RESULTS/metadata/"
azcopy cp "$RESULTS_DIR/BLAST_RUNTIME-${JOB_NUM}.out" "$ELB_RESULTS/logs/"

# Upload performance metrics if collected by blast-run-aks.sh
PERF_FILE="$RESULTS_DIR/PERF_METRICS-${JOB_NUM}.log"
if [ -f "$PERF_FILE" ]; then
    azcopy cp "$PERF_FILE" "$ELB_RESULTS/logs/"
fi

# Upload result file only if not already streamed by blast-run-aks.sh
RESULT_FILE="$RESULTS_DIR/batch_${JOB_NUM}-${ELB_BLAST_PROGRAM}-${ELB_DB}.out.gz"
if [ -f "$RESULT_FILE" ]; then
    azcopy cp "$RESULT_FILE" "$ELB_RESULTS/"
fi

exit "$(cat "$RESULTS_DIR/BLAST_EXIT_CODE.out")"
echo "PERF_METRICS: $((METRICS_LINES - 1)) samples collected in $METRICS_LOG"

printf 'RUNTIME %s %f seconds\n' "blast-job-${JOB_NUM}" $((end - start))

echo "run end $JOB_NUM $BLAST_EXIT_CODE"
echo "$(date -u +"$ELB_TIMEFMT") run exitCode $JOB_NUM $BLAST_EXIT_CODE" >> "$BLAST_RUNTIME"
echo "$(date -u +"$ELB_TIMEFMT") run end $JOB_NUM" >> "$BLAST_RUNTIME"

# Compress results and save runtime info
if [ "${ELB_STREAM_RESULTS:-false}" = "true" ]; then
    # Direct-to-Blob streaming: avoid writing compressed file to disk
    gzip -c "$RESULTS_DIR/batch_${JOB_NUM}-${ELB_BLAST_PROGRAM}-${ELB_DB}.out" \
    | python3 /scripts/blob-stream-upload.py "$ELB_RESULTS" \
    "batch_${JOB_NUM}-${ELB_BLAST_PROGRAM}-${ELB_DB}.out.gz"
    rm "$RESULTS_DIR/batch_${JOB_NUM}-${ELB_BLAST_PROGRAM}-${ELB_DB}.out"
else
    gzip "$RESULTS_DIR/batch_${JOB_NUM}-${ELB_BLAST_PROGRAM}-${ELB_DB}.out"
fi
cp "$BLAST_RUNTIME" "$RESULTS_DIR/BLAST_RUNTIME-${JOB_NUM}.out"
echo "$BLAST_EXIT_CODE" > "$RESULTS_DIR/BLAST_EXIT_CODE.out"

# On failure, upload error details
if [[ $BLAST_EXIT_CODE -ne 0 ]]; then
    if ! azcopy cp "$ELB_RESULTS/metadata/FAILURE.txt" -; then
        azcopy cp "$ERROR_FILE" "$ELB_RESULTS/metadata/FAILURE.txt"
    fi
fi
