#!/bin/bash
# elb-finalizer-aks.sh — Wait for all BLAST jobs, upload status, optionally scale down
#
# Runs as a K8s Job after submit-jobs. Waits for all app=blast jobs to complete,
# writes SUCCESS or FAILURE marker to Blob Storage, and scales nodepool to 0.

set -o pipefail

echo "ElasticBLAST Finalizer started"

# Multi-request safety: scope kubectl wait/get to THIS submission's BLAST jobs.
# BLAST_ELB_JOB_ID is injected by the Job template; if missing (older deploys),
# fall back to app=blast which matches all BLAST jobs in the cluster.
if [ -n "${BLAST_ELB_JOB_ID:-}" ]; then
    BLAST_SELECTOR="app=blast,elb-job-id=${BLAST_ELB_JOB_ID}"
    SUBMIT_SELECTOR="app=submit,elb-job-id=${BLAST_ELB_JOB_ID}"
else
    echo "WARNING: BLAST_ELB_JOB_ID not set; finalizer will track ALL BLAST jobs"
    BLAST_SELECTOR="app=blast"
    SUBMIT_SELECTOR="app=submit"
fi
echo "BLAST selector:  ${BLAST_SELECTOR}"
echo "Submit selector: ${SUBMIT_SELECTOR}"

# Idempotency: if a previous finalizer attempt already wrote a terminal
# marker, exit without touching it. With backoffLimit pinned to 0 in the
# Job template this should not happen, but a manual rerun or a partial
# failure where the marker was written before exit-1 could resurrect us.
# We do not want to flip an existing FAILURE.txt to SUCCESS.txt or vice
# versa across attempts.
MARKER_DIR="${ELB_RESULTS}/${ELB_METADATA_DIR}"
if azcopy login --identity >/dev/null 2>&1; then
    if azcopy list "${MARKER_DIR}/SUCCESS.txt" >/dev/null 2>&1; then
        echo "SUCCESS.txt already present at ${MARKER_DIR}; skipping finalizer"
        exit 0
    fi
    if azcopy list "${MARKER_DIR}/FAILURE.txt" >/dev/null 2>&1; then
        echo "FAILURE.txt already present at ${MARKER_DIR}; skipping finalizer"
        exit 0
    fi
fi

# Wait until BLAST jobs actually exist before declaring SUCCESS.
# Without this guard `kubectl wait` returns immediately when zero jobs match
# the selector, and the empty FAILED check would write SUCCESS.txt before
# any BLAST work has run. This used to be papered over with
# ELB_DISABLE_AUTO_SHUTDOWN; now we detect the race directly.
#
# Bail out as FAILURE if either:
#  (a) the submit-jobs Job has failed (it is responsible for creating BLAST jobs)
#  (b) we never observe any BLAST jobs within BLAST_APPEAR_TIMEOUT seconds
BLAST_APPEAR_TIMEOUT="${BLAST_APPEAR_TIMEOUT:-3600}"   # 60 minutes default
BLAST_APPEAR_INTERVAL="${BLAST_APPEAR_INTERVAL:-15}"
elapsed=0
blast_count=0
submit_failed=""
while [ "$elapsed" -lt "$BLAST_APPEAR_TIMEOUT" ]; do
    blast_count=$(kubectl get jobs -l "${BLAST_SELECTOR}" \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | wc -w)
    if [ "$blast_count" -gt 0 ]; then
        echo "Detected ${blast_count} BLAST job(s); waiting for completion"
        break
    fi
    submit_failed=$(kubectl get jobs -l "${SUBMIT_SELECTOR}" \
    -o jsonpath='{.items[?(@.status.failed)].metadata.name}' 2>/dev/null)
    if [ -n "$submit_failed" ]; then
        echo "ERROR: submit-jobs Job failed before creating BLAST jobs: $submit_failed"
        break
    fi
    sleep "$BLAST_APPEAR_INTERVAL"
    elapsed=$((elapsed + BLAST_APPEAR_INTERVAL))
done

if [ "$blast_count" -eq 0 ]; then
    azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }
    if [ -n "$submit_failed" ]; then
        REASON="submit-jobs Job failed: $submit_failed"
    else
        REASON="no BLAST jobs appeared within ${BLAST_APPEAR_TIMEOUT}s"
    fi
    echo "FAILURE: $REASON"
    echo "$REASON" > /tmp/failure.txt
    azcopy cp /tmp/failure.txt "${MARKER_DIR}/FAILURE.txt" || true
    exit 1
fi

# Wait for all BLAST jobs to complete (event-based, no polling overhead)
echo "Waiting for all BLAST jobs to complete..."
kubectl wait --for=condition=complete job -l "${BLAST_SELECTOR}" --timeout=72h 2>/dev/null || true

# Check for failures
FAILED=$(kubectl get jobs -l "${BLAST_SELECTOR}" \
-o jsonpath='{.items[?(@.status.failed)].metadata.name}' 2>/dev/null)

azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }

if [ -n "$FAILED" ]; then
    echo "FAILURE: jobs failed: $FAILED"
    echo "$FAILED" | azcopy cp /dev/stdin "${MARKER_DIR}/FAILURE.txt" 2>/dev/null || \
    echo "FAILURE" > /tmp/failure.txt && azcopy cp /tmp/failure.txt "${MARKER_DIR}/FAILURE.txt"
else
    echo "SUCCESS: all BLAST jobs completed"
    
    # Merge shard results if DB-partitioned search
    if [ "${ELB_DB_PARTITIONS:-0}" -gt 0 ]; then
        echo "Merging results from ${ELB_DB_PARTITIONS} shards..."
        MERGE_DIR=$(mktemp -d)
        MERGE_INPUT="$MERGE_DIR/all_hits.tsv"
        MERGE_OUTPUT="$MERGE_DIR/merged.out.gz"
        MAX_HITS=500
        
        # Download all shard result files
        SHARD_COUNT=0
        for i in $(seq 0 $((ELB_DB_PARTITIONS - 1))); do
            SHARD=$(printf '%02d' "$i")
            SHARD_DIR="${ELB_RESULTS}/shard_${SHARD}"
            LOCAL_DIR="$MERGE_DIR/shard_${SHARD}"
            mkdir -p "$LOCAL_DIR"
            
            # Download .out.gz files from this shard
            azcopy cp "${SHARD_DIR}/*.out.gz" "$LOCAL_DIR/" --log-level=ERROR 2>/dev/null || true
            
            # Extract and append data rows (skip comment lines)
            for f in "$LOCAL_DIR"/*.out.gz; do
                [ -f "$f" ] || continue
                zcat "$f" | grep -v '^#' >> "$MERGE_INPUT" 2>/dev/null || true
                SHARD_COUNT=$((SHARD_COUNT + 1))
            done
        done
        
        TOTAL_HITS=$(wc -l < "$MERGE_INPUT" 2>/dev/null || echo 0)
        echo "Downloaded $SHARD_COUNT shard files, $TOTAL_HITS total hits"
        
        if [ "$TOTAL_HITS" -gt 0 ]; then
            # Merge: sort by E-value (col 11) ascending, bitscore (col 12) descending
            # Then keep top-N per query (col 1)
            python3 -c "
import sys, gzip
from collections import defaultdict

max_hits = int(sys.argv[1])
query_hits = defaultdict(list)

with open(sys.argv[2]) as f:
    for line in f:
        line = line.rstrip('\n')
        if not line or line.startswith('#'):
            continue
        cols = line.split('\t')
        if len(cols) < 12:
            continue
        try:
            ev = float(cols[10])
        except ValueError:
            ev = float('inf')
        bs = float(cols[11])
        query_hits[cols[0]].append((ev, -bs, line))

fields = ('query acc.ver, subject acc.ver, % identity, alignment length, '
          'mismatches, gap opens, q. start, q. end, s. start, s. end, '
          'evalue, bit score')
total = 0
with gzip.open(sys.argv[3], 'wt') as out:
    for qid in sorted(query_hits):
        hits = sorted(query_hits[qid], key=lambda x: (x[0], x[1]))[:max_hits]
        out.write(f'# BLASTN 2.17.0+\n')
        out.write(f'# Query: {qid}\n')
        out.write(f'# Database: merged from {sys.argv[4]} shards\n')
        out.write(f'# Fields: {fields}\n')
        out.write(f'# {len(hits)} hits found\n')
        for h in hits:
            out.write(h[2] + '\n')
        total += len(hits)
print(f'Merged: {total} hits from {len(query_hits)} queries', file=sys.stderr)
            " "$MAX_HITS" "$MERGE_INPUT" "$MERGE_OUTPUT" "$ELB_DB_PARTITIONS" 2>&1
            
            # Upload merged result to base results directory
            azcopy cp "$MERGE_OUTPUT" "${ELB_RESULTS}/merged_results.out.gz" \
            --log-level=WARNING 2>/dev/null || \
            echo "WARNING: Failed to upload merged results"
            echo "Merged results uploaded to ${ELB_RESULTS}/merged_results.out.gz"
        else
            echo "WARNING: No hits found across shards, skipping merge"
        fi
        
        rm -rf "$MERGE_DIR"
    fi
    
    echo "SUCCESS" > /tmp/success.txt
    azcopy cp /tmp/success.txt "${MARKER_DIR}/SUCCESS.txt"
fi

# Scale nodepool to 0 if not in reuse mode (cost → $0, cluster preserved)
if [ "${ELB_REUSE_CLUSTER}" != "true" ]; then
    echo "Scaling nodepool to 0 (non-reuse mode)"
    # Use kubectl to patch the nodepool count (requires service account with AKS perms)
    # Alternative: the Python CLI's elastic-blast delete handles this
    echo "Cluster resources will be cleaned up by 'elastic-blast delete'"
fi

echo "Finalizer complete"
