#!/usr/bin/env python3
"""Result merger for DB-sharded BLAST runs.

When DB is sharded across nodes, each shard produces partial results.
This module merges them into a single result set with correct ranking.
"""

import csv
import gzip
import heapq
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class BlastHit:
    """A single BLAST hit from tabular output (outfmt 6/7)."""
    qseqid: str
    sseqid: str
    pident: float
    length: int
    mismatch: int
    gapopen: int
    qstart: int
    qend: int
    sstart: int
    send: int
    evalue: float
    bitscore: float
    raw_line: str = ''

    def __lt__(self, other):
        # Sort by evalue ascending, then bitscore descending
        if self.evalue != other.evalue:
            return self.evalue < other.evalue
        return self.bitscore > other.bitscore


def parse_blast_tabular(filepath: str) -> list[BlastHit]:
    """Parse BLAST tabular output (outfmt 6 or 7)."""
    hits = []
    opener = gzip.open if filepath.endswith('.gz') else open

    with opener(filepath, 'rt') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            fields = line.split('\t')
            if len(fields) < 12:
                continue
            try:
                hit = BlastHit(
                    qseqid=fields[0],
                    sseqid=fields[1],
                    pident=float(fields[2]),
                    length=int(fields[3]),
                    mismatch=int(fields[4]),
                    gapopen=int(fields[5]),
                    qstart=int(fields[6]),
                    qend=int(fields[7]),
                    sstart=int(fields[8]),
                    send=int(fields[9]),
                    evalue=float(fields[10]),
                    bitscore=float(fields[11]),
                    raw_line=line,
                )
                hits.append(hit)
            except (ValueError, IndexError):
                continue
    return hits


def merge_shard_results(
    shard_result_files: list[str],
    output_file: str,
    max_target_seqs: int = 500,
) -> str:
    """Merge results from multiple DB shards into a single ranked output.

    For each query, collects hits from all shards, sorts by E-value,
    and keeps top max_target_seqs hits.

    Args:
        shard_result_files: List of result file paths (one per shard)
        output_file: Path for merged output
        max_target_seqs: Maximum hits to keep per query

    Returns:
        Path to merged output file
    """
    log.info(f'Merging {len(shard_result_files)} shard results -> {output_file}')

    # Collect all hits grouped by query
    hits_by_query: dict[str, list[BlastHit]] = defaultdict(list)

    for filepath in shard_result_files:
        if not os.path.exists(filepath):
            log.warning(f'Shard result not found: {filepath}')
            continue
        hits = parse_blast_tabular(filepath)
        log.info(f'  {os.path.basename(filepath)}: {len(hits)} hits')
        for hit in hits:
            hits_by_query[hit.qseqid].append(hit)

    # Sort and truncate per query
    total_hits = 0
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
    opener = gzip.open if output_file.endswith('.gz') else open

    with opener(output_file, 'wt') as f:
        for qid in sorted(hits_by_query.keys()):
            query_hits = sorted(hits_by_query[qid])  # sort by evalue, bitscore
            top_hits = query_hits[:max_target_seqs]
            for hit in top_hits:
                f.write(hit.raw_line + '\n')
            total_hits += len(top_hits)

    log.info(f'Merged: {len(hits_by_query)} queries, {total_hits} total hits')
    return output_file


def merge_query_split_results(
    batch_result_files: list[str],
    output_file: str,
) -> str:
    """Merge results from query-split batches (simple concatenation).

    Unlike shard merging, query-split results don't need re-ranking
    because each query appears in exactly one batch.

    Args:
        batch_result_files: List of batch result file paths
        output_file: Path for merged output

    Returns:
        Path to merged output file
    """
    log.info(f'Concatenating {len(batch_result_files)} batch results -> {output_file}')

    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
    opener = gzip.open if output_file.endswith('.gz') else open

    total_lines = 0
    with opener(output_file, 'wt') as out:
        for filepath in sorted(batch_result_files):
            if not os.path.exists(filepath):
                continue
            hits = parse_blast_tabular(filepath)
            for hit in hits:
                out.write(hit.raw_line + '\n')
                total_lines += 1

    log.info(f'Concatenated: {total_lines} total hits')
    return output_file
