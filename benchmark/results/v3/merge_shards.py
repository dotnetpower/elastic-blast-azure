#!/usr/bin/env python3
"""Merge BLAST -outfmt 7 results from DB-partitioned (sharded) searches.

Reads per-shard .out.gz files, groups hits by query, sorts by E-value
ascending / bitscore descending, and keeps top-N per query.

Usage:
    python merge_shards.py <shard_dir> <output_file> [--max-hits 500]
"""
import argparse
import gzip
import os
import sys
from collections import defaultdict
from pathlib import Path


def parse_evalue(s: str) -> float:
    """Parse E-value string to float. Handles '0.0', '0', scientific notation."""
    try:
        return float(s)
    except ValueError:
        return float('inf')


def find_shard_files(shard_dir: str) -> list:
    """Find all shard .out.gz files under the given directory."""
    files = []
    for root, _, filenames in os.walk(shard_dir):
        for fn in sorted(filenames):
            if fn.endswith('.out.gz') and 'shard' in fn.lower():
                files.append(os.path.join(root, fn))
    if not files:
        # Fallback: any .out.gz file
        for root, _, filenames in os.walk(shard_dir):
            for fn in sorted(filenames):
                if fn.endswith('.out.gz'):
                    files.append(os.path.join(root, fn))
    return sorted(files)


def read_hits(filepath: str) -> list:
    """Read non-comment lines from a BLAST -outfmt 7 gzipped file."""
    hits = []
    with gzip.open(filepath, 'rt', errors='replace') as f:
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('#') or not line.strip():
                continue
            hits.append(line)
    return hits


def merge(shard_files: list, max_hits: int) -> dict:
    """Merge hits from multiple shard files.

    Returns dict: query_id -> list of (evalue, bitscore, original_line)
    sorted and trimmed to max_hits.
    """
    # Collect all hits grouped by query
    query_hits = defaultdict(list)
    total_input = 0

    for sf in shard_files:
        shard_hits = read_hits(sf)
        total_input += len(shard_hits)
        shard_name = os.path.basename(sf)
        print(f"  {shard_name}: {len(shard_hits)} hits", file=sys.stderr)

        for line in shard_hits:
            cols = line.split('\t')
            if len(cols) < 12:
                print(f"  WARNING: skipping malformed line in {shard_name}: {line[:80]}",
                      file=sys.stderr)
                continue
            query_id = cols[0]
            evalue = parse_evalue(cols[10])
            bitscore = float(cols[11])
            query_hits[query_id].append((evalue, -bitscore, line))

    print(f"\nTotal input hits: {total_input}", file=sys.stderr)
    print(f"Unique queries: {len(query_hits)}", file=sys.stderr)

    # Sort and trim per query
    merged = {}
    total_output = 0
    for qid in sorted(query_hits.keys()):
        hits = query_hits[qid]
        # Sort: E-value ascending, bitscore descending (negated)
        hits.sort(key=lambda x: (x[0], x[1]))
        trimmed = hits[:max_hits]
        merged[qid] = [h[2] for h in trimmed]
        total_output += len(trimmed)
        print(f"  {qid}: {len(hits)} total -> {len(trimmed)} kept", file=sys.stderr)

    print(f"\nTotal output hits: {total_output}", file=sys.stderr)
    return merged


def write_output(merged: dict, output_file: str, max_hits: int):
    """Write merged results in -outfmt 7 format."""
    fields = ("query acc.ver, subject acc.ver, % identity, alignment length, "
              "mismatches, gap opens, q. start, q. end, s. start, s. end, "
              "evalue, bit score")

    compress = output_file.endswith('.gz')
    opener = gzip.open if compress else open
    mode = 'wt' if compress else 'w'

    with opener(output_file, mode) as f:
        for qid, hits in merged.items():
            f.write(f"# BLASTN 2.17.0+\n")
            f.write(f"# Query: {qid}\n")
            f.write(f"# Database: core_nt (merged from shards)\n")
            f.write(f"# Fields: {fields}\n")
            f.write(f"# {len(hits)} hits found\n")
            for line in hits:
                f.write(line + '\n')


def main():
    parser = argparse.ArgumentParser(description='Merge sharded BLAST results')
    parser.add_argument('shard_dir', help='Directory containing shard_XX/ subdirs with .out.gz files')
    parser.add_argument('output', help='Output file path (.out or .out.gz)')
    parser.add_argument('--max-hits', type=int, default=500,
                        help='Max hits per query (default: 500, matching -max_target_seqs)')
    args = parser.parse_args()

    print(f"Shard directory: {args.shard_dir}", file=sys.stderr)
    print(f"Output: {args.output}", file=sys.stderr)
    print(f"Max hits per query: {args.max_hits}", file=sys.stderr)

    shard_files = find_shard_files(args.shard_dir)
    if not shard_files:
        print("ERROR: No shard .out.gz files found!", file=sys.stderr)
        sys.exit(1)

    print(f"\nFound {len(shard_files)} shard files:", file=sys.stderr)
    merged = merge(shard_files, args.max_hits)
    write_output(merged, args.output, args.max_hits)

    print(f"\nMerged output written to: {args.output}", file=sys.stderr)


if __name__ == '__main__':
    main()
