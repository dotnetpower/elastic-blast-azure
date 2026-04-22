#!/usr/bin/env python3
"""Correctness validation for v3 DB optimization strategies.

Validates that sharded and subset BLAST results match the reference
(full core_nt) search. This is critical for production use — incorrect
E-values or missing hits would be unacceptable.

Usage:
    # Compare sharded results vs reference
    python benchmark/validate_results_v3.py compare \
        --reference results/ref_full.out \
        --test results/merged_shards.out \
        --label "10-shard merged"

    # Compare subset results vs reference
    python benchmark/validate_results_v3.py compare \
        --reference results/ref_full.out \
        --test results/subset_pathogen.out \
        --label "pathogen subset"

    # Full validation suite (requires result files)
    python benchmark/validate_results_v3.py suite \
        --results-dir benchmark/results/v3/raw/

Author: Moon Hyuk Choi
"""

import argparse
import csv
import gzip
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)


@dataclass
class BlastHit:
    """Parsed BLAST tabular hit."""
    qseqid: str
    sseqid: str
    pident: float
    length: int
    evalue: float
    bitscore: float
    raw_line: str


def parse_blast_file(filepath: str) -> list[BlastHit]:
    """Parse BLAST tabular output (outfmt 6/7)."""
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
                hits.append(BlastHit(
                    qseqid=fields[0],
                    sseqid=fields[1],
                    pident=float(fields[2]),
                    length=int(fields[3]),
                    evalue=float(fields[10]),
                    bitscore=float(fields[11]),
                    raw_line=line,
                ))
            except (ValueError, IndexError):
                continue
    return hits


def group_by_query(hits: list[BlastHit]) -> dict[str, list[BlastHit]]:
    """Group hits by query sequence ID."""
    grouped = defaultdict(list)
    for h in hits:
        grouped[h.qseqid].append(h)
    # Sort each group by evalue asc, bitscore desc
    for qid in grouped:
        grouped[qid].sort(key=lambda h: (h.evalue, -h.bitscore))
    return dict(grouped)


def compare_results(
    ref_file: str,
    test_file: str,
    label: str = "test",
    top_n: int = 500,
    evalue_tolerance: float = 0.01,
) -> dict:
    """Compare test results against reference.
    
    Returns validation metrics:
    - hit_overlap: % of reference (query, subject) pairs found in test
    - subject_overlap: % of reference subjects found in test
    - evalue_concordance: % of common hits with E-value within tolerance
    - extra_hits: hits in test but not in reference
    - missing_hits: hits in reference but not in test
    """
    log.info(f"\n{'='*60}")
    log.info(f" Validation: {label}")
    log.info(f" Reference: {ref_file}")
    log.info(f" Test:      {test_file}")
    log.info(f"{'='*60}\n")
    
    ref_hits = parse_blast_file(ref_file)
    test_hits = parse_blast_file(test_file)
    
    ref_by_query = group_by_query(ref_hits)
    test_by_query = group_by_query(test_hits)
    
    log.info(f"  Reference: {len(ref_hits)} hits, {len(ref_by_query)} queries")
    log.info(f"  Test:      {len(test_hits)} hits, {len(test_by_query)} queries")
    
    # Metric 1: Query coverage
    ref_queries = set(ref_by_query.keys())
    test_queries = set(test_by_query.keys())
    common_queries = ref_queries & test_queries
    missing_queries = ref_queries - test_queries
    extra_queries = test_queries - ref_queries
    
    log.info(f"\n  Query coverage:")
    log.info(f"    Common queries:  {len(common_queries)}")
    log.info(f"    Missing queries: {len(missing_queries)}")
    log.info(f"    Extra queries:   {len(extra_queries)}")
    
    if missing_queries:
        log.info(f"    Missing: {list(missing_queries)[:5]}")
    
    # Metric 2: Hit-level overlap (top-N per query)
    total_ref_pairs = 0
    total_test_pairs = 0
    common_pairs = 0
    evalue_concordant = 0
    evalue_total = 0
    
    per_query_results = []
    
    for qid in common_queries:
        ref_q = ref_by_query[qid][:top_n]
        test_q = test_by_query[qid][:top_n]
        
        ref_subjects = {h.sseqid for h in ref_q}
        test_subjects = {h.sseqid for h in test_q}
        
        common = ref_subjects & test_subjects
        total_ref_pairs += len(ref_subjects)
        total_test_pairs += len(test_subjects)
        common_pairs += len(common)
        
        # E-value concordance for common hits
        ref_evals = {h.sseqid: h.evalue for h in ref_q}
        test_evals = {h.sseqid: h.evalue for h in test_q}
        
        for sid in common:
            evalue_total += 1
            ref_ev = ref_evals[sid]
            test_ev = test_evals[sid]
            # Relative tolerance for E-values
            if ref_ev == 0 and test_ev == 0:
                evalue_concordant += 1
            elif ref_ev == 0 or test_ev == 0:
                pass  # one is zero, other isn't
            elif abs(ref_ev - test_ev) / max(ref_ev, test_ev) <= evalue_tolerance:
                evalue_concordant += 1
        
        overlap_pct = len(common) / len(ref_subjects) * 100 if ref_subjects else 100
        per_query_results.append({
            'qid': qid,
            'ref_hits': len(ref_subjects),
            'test_hits': len(test_subjects),
            'common': len(common),
            'overlap_pct': overlap_pct,
        })
    
    hit_overlap = common_pairs / total_ref_pairs * 100 if total_ref_pairs else 100
    evalue_pct = evalue_concordant / evalue_total * 100 if evalue_total else 100
    
    log.info(f"\n  Hit-level overlap (top-{top_n} per query):")
    log.info(f"    Reference pairs:  {total_ref_pairs}")
    log.info(f"    Test pairs:       {total_test_pairs}")
    log.info(f"    Common pairs:     {common_pairs}")
    log.info(f"    Overlap:          {hit_overlap:.1f}%")
    log.info(f"    E-value concordance: {evalue_pct:.1f}% "
             f"({evalue_concordant}/{evalue_total}, tolerance={evalue_tolerance*100}%)")
    
    # Per-query detail
    log.info(f"\n  Per-query breakdown:")
    log.info(f"    {'Query':<30} {'Ref':>5} {'Test':>5} {'Common':>6} {'Overlap':>8}")
    log.info(f"    {'-'*30} {'-'*5} {'-'*5} {'-'*6} {'-'*8}")
    for r in sorted(per_query_results, key=lambda x: x['overlap_pct']):
        log.info(f"    {r['qid']:<30} {r['ref_hits']:>5} {r['test_hits']:>5} "
                 f"{r['common']:>6} {r['overlap_pct']:>7.1f}%")
    
    # Overall verdict
    PASS_THRESHOLD = 95.0
    passed = hit_overlap >= PASS_THRESHOLD
    
    log.info(f"\n  {'='*40}")
    if passed:
        log.info(f"  PASS: {hit_overlap:.1f}% overlap >= {PASS_THRESHOLD}% threshold")
    else:
        log.info(f"  FAIL: {hit_overlap:.1f}% overlap < {PASS_THRESHOLD}% threshold")
    log.info(f"  {'='*40}\n")
    
    return {
        'label': label,
        'ref_file': ref_file,
        'test_file': test_file,
        'ref_hits': len(ref_hits),
        'test_hits': len(test_hits),
        'ref_queries': len(ref_by_query),
        'test_queries': len(test_by_query),
        'common_queries': len(common_queries),
        'hit_overlap_pct': hit_overlap,
        'evalue_concordance_pct': evalue_pct,
        'passed': passed,
        'per_query': per_query_results,
    }


def run_suite(results_dir: str) -> None:
    """Run full validation suite on downloaded results."""
    ref_file = os.path.join(results_dir, 'reference', 'REF-E64-1N.out')
    
    if not os.path.exists(ref_file):
        # Try to find any reference result
        ref_files = list(Path(results_dir).glob('**/REF*.out*'))
        if ref_files:
            ref_file = str(ref_files[0])
        else:
            log.error(f"Reference file not found: {ref_file}")
            log.error("Run the REF-E64-1N test first to establish baseline.")
            sys.exit(1)
    
    log.info(f"Reference: {ref_file}")
    
    # Find all test result files
    test_results = []
    for pattern in ['b1_shard/**/merged*.out*', 'b2_subset/**/*.out*',
                    'b3_index/**/*.out*', 'combined/**/*.out*']:
        test_results.extend(Path(results_dir).glob(pattern))
    
    if not test_results:
        log.error("No test result files found.")
        sys.exit(1)
    
    all_results = []
    for test_file in sorted(test_results):
        label = str(test_file.relative_to(results_dir))
        result = compare_results(ref_file, str(test_file), label=label)
        all_results.append(result)
    
    # Summary table
    log.info(f"\n{'='*70}")
    log.info(f" VALIDATION SUMMARY")
    log.info(f"{'='*70}\n")
    log.info(f"{'Test':<35} {'Hits':>6} {'Overlap':>8} {'E-val':>8} {'Status':>8}")
    log.info(f"{'-'*35} {'-'*6} {'-'*8} {'-'*8} {'-'*8}")
    
    all_passed = True
    for r in all_results:
        status = "PASS" if r['passed'] else "FAIL"
        log.info(f"{r['label']:<35} {r['test_hits']:>6} "
                 f"{r['hit_overlap_pct']:>7.1f}% "
                 f"{r['evalue_concordance_pct']:>7.1f}% "
                 f"{status:>8}")
        if not r['passed']:
            all_passed = False
    
    log.info(f"\n{'='*70}")
    if all_passed:
        log.info(f" ALL VALIDATIONS PASSED")
    else:
        log.info(f" SOME VALIDATIONS FAILED — review details above")
    log.info(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(description='v3 Result Correctness Validation')
    subparsers = parser.add_subparsers(dest='command')
    
    # Compare command
    cmp = subparsers.add_parser('compare', help='Compare test vs reference')
    cmp.add_argument('--reference', '-r', required=True, help='Reference result file')
    cmp.add_argument('--test', '-t', required=True, help='Test result file')
    cmp.add_argument('--label', '-l', default='test', help='Label for the test')
    cmp.add_argument('--top-n', type=int, default=500, help='Top N hits per query')
    cmp.add_argument('--tolerance', type=float, default=0.01, help='E-value tolerance')
    
    # Suite command
    suite = subparsers.add_parser('suite', help='Run full validation suite')
    suite.add_argument('--results-dir', '-d', default='benchmark/results/v3/raw/',
                       help='Results directory')
    
    args = parser.parse_args()
    
    if args.command == 'compare':
        result = compare_results(
            args.reference, args.test,
            label=args.label,
            top_n=args.top_n,
            evalue_tolerance=args.tolerance,
        )
        sys.exit(0 if result['passed'] else 1)
    elif args.command == 'suite':
        run_suite(args.results_dir)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
