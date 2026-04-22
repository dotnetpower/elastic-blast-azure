#!/usr/bin/env python3
"""Generate all benchmark v3 INI configs for DB optimization tests.

Creates configs for:
  - B1: DB Sharding (5/10/20 shards × E16s/E32s nodes)
  - B2: Taxonomy Subset (pathogen/virus/broad × E16s/E64s)
  - B3: MegaBLAST Index (full DB + subset, indexed)
  - C:  Combined strategies (subset+shard, subset+index)

Usage:
    python benchmark/configs/v3/generate_configs.py
    python benchmark/configs/v3/generate_configs.py --dry-run

Author: Moon Hyuk Choi
"""

import os
import sys

STORAGE = "stgelb"
BLOB_BASE = f"https://{STORAGE}.blob.core.windows.net"
DB_BASE = f"{BLOB_BASE}/blast-db"
QUERIES_BASE = f"{BLOB_BASE}/queries"
RESULTS_BASE = f"{BLOB_BASE}/results/v3"

COMMON_CP = """[cloud-provider]
azure-region = koreacentral
azure-acr-resource-group = rg-elbacr
azure-acr-name = elbacr
azure-resource-group = rg-elb-koc
azure-storage-account = stgelb
azure-storage-account-container = blast-db
"""

# Standard BLAST options (same as v2)
BLAST_OPTIONS = "-max_target_seqs 500 -evalue 0.05 -word_size 28 -dust yes -soft_masking true -outfmt 7"

# Default query
DEFAULT_QUERY = "pathogen-10.fa"
DEFAULT_BATCH_LEN = 100000

# DB total letters for core_nt (measured on prep VM 2026-04-22)
# core_nt: 124,309,873 sequences; 978,954,058,562 total bases
CORE_NT_TOTAL_LETTERS = 978954058562

# VM costs for reference
VM_COSTS = {
    "Standard_E16s_v3": 1.008,
    "Standard_E32s_v3": 2.016,
    "Standard_E48s_v3": 3.024,
    "Standard_E64s_v3": 4.032,
}

OUT_DIR = os.path.dirname(os.path.abspath(__file__))


def write_ini(filepath: str, content: str, dry_run: bool = False):
    """Write INI file, creating directories as needed."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if dry_run:
        print(f"  [DRY-RUN] {filepath}")
        return
    with open(filepath, 'w') as f:
        f.write(content)
    print(f"  {filepath}")


# ═══════════════════════════════════════════════════════════
# B1: DB Sharding
# ═══════════════════════════════════════════════════════════

def gen_b1_shard(dry_run=False):
    """B1: DB Shard tests — each node gets 1/N of the DB."""
    print("\n=== B1: DB Sharding ===")
    
    tests = [
        # (test_id, num_shards, num_nodes, vm, notes)
        ("B1-S5-5N",    5,  5,  "Standard_E16s_v3",  "5 shards on 5×E16s (54 GB/node)"),
        ("B1-S10-10N",  10, 10, "Standard_E16s_v3",  "10 shards on 10×E16s (27 GB/node)"),
        ("B1-S10-5N",   10, 5,  "Standard_E16s_v3",  "10 shards on 5 nodes (2 shards/node)"),
        ("B1-S20-10N",  20, 10, "Standard_E16s_v3",  "20 shards on 10 nodes (13.5 GB/node)"),
        ("B1-S10-E32",  10, 10, "Standard_E32s_v3",  "10 shards on 10×E32s (verify RAM effect)"),
    ]
    
    for test_id, num_shards, num_nodes, vm, notes in tests:
        # For sharded runs, ElasticBLAST can't natively handle per-node DB assignment.
        # We use the db-partitions config key + init-db-partitioned-aks.sh.
        # The shard layout is pre-uploaded to: blast-db/{num_shards}shards/
        shard_prefix = f"{DB_BASE}/{num_shards}shards/core_nt_shard_"
        
        ini = f"""{COMMON_CP}
# {test_id}: {notes}
# Axis B1 — DB Sharding
# Each node downloads only its shard(s), not the full 269 GB DB.
# BLAST uses -dbsize {CORE_NT_TOTAL_LETTERS} for E-value correction.

[cluster]
name = elb-v3-b1
machine-type = {vm}
num-nodes = {num_nodes}
exp-use-local-ssd = true

[blast]
program = blastn
db = {DB_BASE}/core_nt/core_nt
queries = {QUERIES_BASE}/{DEFAULT_QUERY}
results = {RESULTS_BASE}/b1_shard/{test_id}
options = {BLAST_OPTIONS} -dbsize {CORE_NT_TOTAL_LETTERS}
batch-len = {DEFAULT_BATCH_LEN}
mem-limit = 4G
db-partitions = {num_shards}
db-partition-prefix = {shard_prefix}

[timeouts]
init-pv = 30
"""
        write_ini(f"{OUT_DIR}/b1_shard/{test_id}.ini", ini, dry_run)


# ═══════════════════════════════════════════════════════════
# B2: Taxonomy Subset
# ═══════════════════════════════════════════════════════════

def gen_b2_subset(dry_run=False):
    """B2: Taxonomy Subset tests — use smaller, filtered DB."""
    print("\n=== B2: Taxonomy Subset ===")
    
    tests = [
        # (test_id, db_name, vm, nodes, notes)
        ("B2-pathogen-E64-1N", "core_nt_pathogen", "Standard_E64s_v3", 1,
         "Pathogen subset (Virus+Plasmodium), E64s single node"),
        ("B2-virus-E64-1N",    "core_nt_virus",    "Standard_E64s_v3", 1,
         "Virus-only subset, E64s single node"),
        ("B2-broad-E64-1N",    "core_nt_broad",    "Standard_E64s_v3", 1,
         "Broad subset (Virus+Bacteria+Plasmodium), E64s single node"),
        ("B2-pathogen-E16-1N", "core_nt_pathogen", "Standard_E16s_v3", 1,
         "Pathogen subset on cheaper E16s (small DB fits in 128 GB)"),
    ]
    
    for test_id, db_name, vm, nodes, notes in tests:
        db_url = f"{DB_BASE}/{db_name}/{db_name}"
        ini = f"""{COMMON_CP}
# {test_id}: {notes}
# Axis B2 — Taxonomy Subset
# Uses pre-built subset DB instead of full core_nt (269 GB).
# No -dbsize correction needed: subset DB has its own statistics.

[cluster]
name = elb-v3-b2
machine-type = {vm}
num-nodes = {nodes}
exp-use-local-ssd = true

[blast]
program = blastn
db = {db_url}
queries = {QUERIES_BASE}/{DEFAULT_QUERY}
results = {RESULTS_BASE}/b2_subset/{test_id}
options = {BLAST_OPTIONS}
batch-len = {DEFAULT_BATCH_LEN}
mem-limit = 4G

[timeouts]
init-pv = 30
"""
        write_ini(f"{OUT_DIR}/b2_subset/{test_id}.ini", ini, dry_run)


# ═══════════════════════════════════════════════════════════
# B3: MegaBLAST Index
# ═══════════════════════════════════════════════════════════

def gen_b3_index(dry_run=False):
    """B3: MegaBLAST Index tests — use pre-built word-lookup index."""
    print("\n=== B3: MegaBLAST Index ===")
    
    tests = [
        # (test_id, db_name, db_blob_dir, vm, nodes, notes)
        ("B3-idx-E64-1N", "core_nt", "core_nt_indexed",
         "Standard_E64s_v3", 1,
         "Full core_nt with MegaBLAST index, E64s single node"),
        ("B3-idx-pathogen-E16", "core_nt_pathogen", "core_nt_pathogen_indexed",
         "Standard_E16s_v3", 1,
         "Pathogen subset with MegaBLAST index, E16s single node"),
    ]
    
    for test_id, db_name, db_blob_dir, vm, nodes, notes in tests:
        db_url = f"{DB_BASE}/{db_blob_dir}/{db_name}"
        ini = f"""{COMMON_CP}
# {test_id}: {notes}
# Axis B3 — MegaBLAST Indexing
# Uses pre-built makembindex word-lookup index for faster seeding.
# NOTE: -use_index is in ElasticBLAST's UNSUPPORTED_OPTIONS list.
#       This test uses a custom blast-run script that adds -use_index.
#       See: benchmark/scripts/blast-run-indexed-aks.sh

[cluster]
name = elb-v3-b3
machine-type = {vm}
num-nodes = {nodes}
exp-use-local-ssd = true

[blast]
program = blastn
db = {db_url}
queries = {QUERIES_BASE}/{DEFAULT_QUERY}
results = {RESULTS_BASE}/b3_index/{test_id}
options = {BLAST_OPTIONS}
batch-len = {DEFAULT_BATCH_LEN}
mem-limit = 4G

[timeouts]
init-pv = 60
"""
        write_ini(f"{OUT_DIR}/b3_index/{test_id}.ini", ini, dry_run)


# ═══════════════════════════════════════════════════════════
# C: Combined Strategies
# ═══════════════════════════════════════════════════════════

def gen_combined(dry_run=False):
    """Combined strategy tests."""
    print("\n=== C: Combined Strategies ===")
    
    tests = [
        # (test_id, db_name, db_blob_dir, vm, nodes, num_shards, use_index, notes)
        ("C1-subset-shard5", "core_nt_pathogen", None,
         "Standard_E16s_v3", 5, 5, False,
         "Pathogen subset + 5 shards on 5×E16s"),
        ("C2-subset-shard3", "core_nt_pathogen", None,
         "Standard_E16s_v3", 3, 3, False,
         "Pathogen subset + 3 shards on 3×E16s"),
        ("C3-subset-idx", "core_nt_pathogen", "core_nt_pathogen_indexed",
         "Standard_E16s_v3", 1, 0, True,
         "Pathogen subset + MegaBLAST index, E16s single node"),
    ]
    
    for test_id, db_name, db_blob_dir, vm, nodes, num_shards, use_index, notes in tests:
        if db_blob_dir:
            db_url = f"{DB_BASE}/{db_blob_dir}/{db_name}"
        else:
            db_url = f"{DB_BASE}/{db_name}/{db_name}"
        
        shard_section = ""
        if num_shards > 0:
            shard_prefix = f"{DB_BASE}/pathogen_{num_shards}shards/core_nt_pathogen_shard_"
            shard_section = f"""db-partitions = {num_shards}
db-partition-prefix = {shard_prefix}"""
        
        index_note = ""
        if use_index:
            index_note = """
# NOTE: Uses custom blast-run script with -use_index.
# See: benchmark/scripts/blast-run-indexed-aks.sh"""
        
        ini = f"""{COMMON_CP}
# {test_id}: {notes}
# Combined Strategy{index_note}

[cluster]
name = elb-v3-combined
machine-type = {vm}
num-nodes = {nodes}
exp-use-local-ssd = true

[blast]
program = blastn
db = {db_url}
queries = {QUERIES_BASE}/{DEFAULT_QUERY}
results = {RESULTS_BASE}/combined/{test_id}
options = {BLAST_OPTIONS}
batch-len = {DEFAULT_BATCH_LEN}
mem-limit = 4G
{shard_section}

[timeouts]
init-pv = 30
"""
        write_ini(f"{OUT_DIR}/combined/{test_id}.ini", ini, dry_run)


# ═══════════════════════════════════════════════════════════
# Reference (control) test
# ═══════════════════════════════════════════════════════════

def gen_reference(dry_run=False):
    """Reference test — same as v2 A1-E64-1N for comparison."""
    print("\n=== REF: Reference Control ===")
    
    ini = f"""{COMMON_CP}
# REF-E64-1N: v2 baseline reproduction for v3 comparison
# Full core_nt (269 GB), E64s_v3 single node, 10 queries
# Expected: ~57 min BLAST time (matches v2 A1-E64-10)

[cluster]
name = elb-v3-ref
machine-type = Standard_E64s_v3
num-nodes = 1
exp-use-local-ssd = true

[blast]
program = blastn
db = {DB_BASE}/core_nt/core_nt
queries = {QUERIES_BASE}/{DEFAULT_QUERY}
results = {RESULTS_BASE}/reference/REF-E64-1N
options = {BLAST_OPTIONS}
batch-len = {DEFAULT_BATCH_LEN}
mem-limit = 4G

[timeouts]
init-pv = 90
"""
    write_ini(f"{OUT_DIR}/reference/REF-E64-1N.ini", ini, dry_run)


def main():
    dry_run = "--dry-run" in sys.argv
    
    print("=" * 60)
    print(" ElasticBLAST v3 — INI Config Generator")
    print(" DB Optimization Benchmark")
    print("=" * 60)
    
    gen_reference(dry_run)
    gen_b1_shard(dry_run)
    gen_b2_subset(dry_run)
    gen_b3_index(dry_run)
    gen_combined(dry_run)
    
    # Count configs
    total = 0
    for root, dirs, files in os.walk(OUT_DIR):
        total += sum(1 for f in files if f.endswith('.ini'))
    
    print(f"\n{'=' * 60}")
    print(f" Generated {total} INI configs in {OUT_DIR}/")
    print(f"{'=' * 60}")
    
    if dry_run:
        print("\n[DRY-RUN mode — no files written]")


if __name__ == '__main__':
    main()
