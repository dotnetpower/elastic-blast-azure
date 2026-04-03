#!/bin/bash
# blast-run-aks.sh — Execute BLAST search
#
# Environment variables (set by K8s pod spec):
#   ELB_BLAST_PROGRAM - BLAST program (blastn, blastp, blastx, etc.)
#   ELB_DB            - BLAST database name
#   ELB_NUM_CPUS      - Number of CPU threads for BLAST
#   ELB_BLAST_OPTIONS - Additional BLAST command-line options
#   ELB_RESULTS       - Azure Blob Storage URL for results
#   ELB_TIMEFMT       - Timestamp format string
#   JOB_NUM           - Batch job number (e.g., 000, 001, ...)
#   QUERY_DIR         - Directory containing query files
#   RESULTS_DIR       - Directory for output files

echo "BASH version ${BASH_VERSION}"

azcopy login --identity

BLAST_RUNTIME=$(mktemp)
ERROR_FILE=$(mktemp)
DATE_NOW=$(date -u +"$ELB_TIMEFMT")

# Get database length for metadata
blastdbcmd -info -db "$ELB_DB" \
| awk '/total/ {print $3}' \
| tr -d , > "$RESULTS_DIR/BLASTDB_LENGTH.out"

start=$(date +%s)
echo "run start $JOB_NUM $ELB_BLAST_PROGRAM $ELB_DB"

# Run BLAST with timing
# shellcheck disable=SC2086
echo "$ELB_BLAST_PROGRAM -db $ELB_DB -query $QUERY_DIR/batch_${JOB_NUM}.fa -out $RESULTS_DIR/batch_${JOB_NUM}-${ELB_BLAST_PROGRAM}-${ELB_DB}.out -num_threads $ELB_NUM_CPUS $ELB_BLAST_OPTIONS"

# shellcheck disable=SC2086
TIME="$DATE_NOW run start $JOB_NUM $ELB_BLAST_PROGRAM $ELB_DB %e %U %S %P" \
\time -o "$BLAST_RUNTIME" \
$ELB_BLAST_PROGRAM \
-db "$ELB_DB" \
-query "$QUERY_DIR/batch_${JOB_NUM}.fa" \
-out "$RESULTS_DIR/batch_${JOB_NUM}-${ELB_BLAST_PROGRAM}-${ELB_DB}.out" \
-num_threads "$ELB_NUM_CPUS" \
$ELB_BLAST_OPTIONS \
2>"$ERROR_FILE"
BLAST_EXIT_CODE=$?

end=$(date +%s)
cat "$ERROR_FILE"
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
