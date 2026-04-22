# ElasticBLAST Azure Benchmark Plan v3 — DB Sharding & Indexing

> **Created**: 2026-04-22
> **Author**: Moon Hyuk Choi (moonchoi@microsoft.com)
> **Baseline**: [v2 report](results/v2/report.md) — core_nt 269 GB, E64s_v3
> **Budget**: $150 (3 axes, ~20 tests)
> **Region**: Korea Central
> **Goal**: **10x BLAST time reduction** (57 min → <6 min) without changing ElasticBLAST or BLAST+

---

## 0. Problem Statement

v2 established a single overwhelming fact:

$$T_{BLAST} \approx T_{DB\_scan} = 57 \text{ min (E64s\_v3, 1N, 269 GB core\_nt)}$$

Query count (10 vs 300) has **zero impact**. The entire 269 GB is scanned regardless. Multi-node scale-out (2-3N) reduced this to ~10 min via memory pressure relief, but each node still downloads and scans the **full 269 GB**.

**The bottleneck is not CPU or queries — it's how much DB each node must scan.**

This plan attacks that bottleneck through three complementary axes:

| Axis      | Strategy           | Mechanism                            | Expected Speedup |
| --------- | ------------------ | ------------------------------------ | ---------------- |
| **B1**    | DB Sharding        | Each node scans 1/N of DB            | 5-10x            |
| **B2**    | Taxonomy Subset    | Scan only relevant organisms         | 10-50x           |
| **B3**    | MegaBLAST Indexing | Skip full scan via word-lookup index | 2-10x            |
| **B1+B2** | Shard × Subset     | Combine both                         | 50-100x          |

All strategies use **stock BLAST+ 2.17.0 tools** (`blastdb_aliastool`, `blastdbcmd`, `makeblastdb`, `makembindex`) and run within the existing ElasticBLAST AKS framework.

---

## 1. Axis B1: DB Sharding — Per-Node Partial DB

### 1.1 Concept

Current (v2): Every node downloads and scans the **full 269 GB** DB.

```
Node 0: [core_nt 269 GB] ← scan all → 57 min
Node 1: [core_nt 269 GB] ← scan all → 57 min  (no speedup per node)
```

Proposed: Each node downloads and scans only **1/N of the DB** (alias-based sharding).

```
Node 0: [shard_00  27 GB] ← scan 1/10 → ~5.7 min
Node 1: [shard_01  27 GB] ← scan 1/10 → ~5.7 min
...
Node 9: [shard_09  27 GB] ← scan 1/10 → ~5.7 min
→ All shards finish in parallel → merge results → ~6 min total
```

### 1.2 How It Works (BLAST+ native tools, no code changes)

**Step 1: Pre-shard** (one-time, on a prep VM)

```bash
# core_nt has 83 volumes (.00 through .82)
# Group into N shards using blastdb_aliastool
# Example: 10 shards, ~8-9 volumes each

# Shard 0: volumes 00-07 → alias file
blastdb_aliastool -dblist "core_nt.00 core_nt.01 ... core_nt.07" \
    -dbtype nucl -out core_nt_shard_00 -title "core_nt shard 0"

# Shard 1: volumes 08-15 → alias file
blastdb_aliastool -dblist "core_nt.08 core_nt.09 ... core_nt.15" \
    -dbtype nucl -out core_nt_shard_01 -title "core_nt shard 1"
# ... etc.
```

Each shard is a tiny `.nal` alias file (~1 KB) pointing to a subset of the original volume files. **No data duplication** during sharding — volume files stay as-is.

**Step 2: Upload shards** to Blob Storage with per-shard directory structure:

```
blast-db/core_nt_shards/
├── shard_00/   (core_nt.00.nsq, .00.nhr, .00.nin, ..., .07.*)  ~27 GB
├── shard_01/   (~27 GB)
├── ...
└── shard_09/   (~27 GB)
```

**Step 3: Download per-node** — each AKS node downloads only its shard

```bash
# Node 0: downloads only shard_00 (27 GB instead of 269 GB)
azcopy cp "https://stgelb.blob.core.windows.net/blast-db/core_nt_shards/shard_00/*" \
    /blast/blastdb/ --recursive

# Node 1: downloads shard_01, etc.
```

**Step 4: BLAST with `-dbsize` correction**

When searching a shard (partial DB), E-values must be corrected to reflect the full DB size:

```bash
blastn -db core_nt_shard_00 -query input.fa \
    -dbsize 228862015061 \    # total letters in full core_nt
    -outfmt 7 -out results.out
```

The `-dbsize` parameter ensures E-values are comparable across shards and to full-DB results.

**Step 5: Merge results** — concatenate per-shard results, sort by E-value

```bash
# Simple concatenation (outfmt 7 with headers)
cat shard_*/results.out | grep -v '^#' | sort -k11,11g > merged_results.out
```

### 1.3 Why This Should Give 5-10x Speedup

| Factor                 | Full DB (v2)  | 10 Shards   | Improvement      |
| ---------------------- | ------------- | ----------- | ---------------- |
| DB scanned per node    | 269 GB        | 27 GB       | **10x less**     |
| RAM fit                | 269/432 = 62% | 27/432 = 6% | No page eviction |
| DB download/node       | 28 min        | ~3 min      | **9x faster**    |
| BLAST time/node        | 57 min        | ~5-7 min†   | **8-10x faster** |
| Total BLAST (parallel) | 57 min        | ~5-7 min    | **8-10x**        |
| Cold wall clock        | 85 min        | ~25 min‡    | **3.4x**         |

† BLAST scan time is approximately linear with DB size for fixed queries.
‡ AKS create (15 min) + download (3 min) + BLAST (7 min).

**Key question to validate**: Is BLAST scan time truly linear with DB size? If sub-linear (e.g., index lookups), speedup will be less. If super-linear (memory pressure), speedup will be more.

### 1.4 Test Matrix

| Test ID    | Shards | Nodes | VM      | DB/Node | Download Est. | BLAST Est. | Measures                         |
| ---------- | ------ | ----- | ------- | ------- | ------------- | ---------- | -------------------------------- |
| B1-S5-5N   | 5      | 5     | E64s_v3 | 54 GB   | ~6 min        | ~11 min    | Shard scaling baseline           |
| B1-S10-10N | 10     | 10    | E64s_v3 | 27 GB   | ~3 min        | ~6 min     | Optimal shard count              |
| B1-S10-5N  | 10     | 5     | E64s_v3 | 54 GB‡  | ~6 min        | ~11 min    | 2 shards/node                    |
| B1-S20-10N | 20     | 10    | E64s_v3 | 13.5 GB | ~1.5 min      | ~3 min     | Diminishing returns?             |
| B1-S83-10N | 83     | 10    | E64s_v3 | 27 GB   | ~3 min        | ~6 min     | Max shards (1 vol each)          |
| B1-S10-E32 | 10     | 10    | E32s_v3 | 27 GB   | ~3 min        | ~6 min     | Cheaper VM (27 GB << 256 GB RAM) |

‡ 10 shards on 5 nodes = 2 sequential shard searches per node

**Total**: 6 tests

### 1.5 Critical Validation: E-value Correctness

DB sharding changes how BLAST computes E-values. We must validate:

```bash
# Reference: full DB search
blastn -db core_nt -query pathogen-10.fa -outfmt "6 std" -out ref.out

# Sharded: merge 10 shard results with -dbsize
for s in $(seq 0 9); do
    blastn -db core_nt_shard_${s} -query pathogen-10.fa \
        -dbsize 228862015061 -outfmt "6 std" -out shard_${s}.out
done
cat shard_*.out | sort -k11,11g > merged.out

# Compare: top 500 hits should match (identical set, same E-value order)
head -500 ref.out > ref_top500.out
head -500 merged.out > merged_top500.out
diff ref_top500.out merged_top500.out
```

**Acceptance criteria**: Top-500 hits per query match ≥95% between full-DB and sharded results. E-values within 1% tolerance.

### 1.6 Implementation (Existing Code)

The codebase already has most of this:

- **`benchmark/strategies/db_prep.py`**: `shard_db()` creates alias shards via `blastdb_aliastool`
- **`benchmark/strategies/blast_strategies.py`**: `DBShardStrategy` generates job specs with `-dbsize` correction
- **`src/elastic_blast/templates/scripts/init-db-partitioned-aks.sh`**: downloads N partitions from Blob
- **`src/elastic_blast/templates/job-init-pv-partitioned-aks.yaml.template`**: K8s job template

**What's missing**:

1. **Shard prep script**: Run `shard_db()` on a prep VM, upload to Blob — ~30 min one-time
2. **Result merger**: `benchmark/strategies/merger.py` exists but needs E-value re-ranking validation
3. **INI config for sharded mode**: `db-partitions = 10` in `[blast]` section (config key already defined)
4. **Benchmark runner integration**: Wire `DBShardStrategy` into `run_benchmark.py`

### 1.7 Cost Estimate

| Item                    | Nodes | Duration | $/hr/node | Cost     |
| ----------------------- | ----- | -------- | --------- | -------- |
| Shard prep VM (D32s_v3) | 1     | 1 hr     | $1.53     | $1.53    |
| B1-S5-5N                | 5     | 30 min   | $4.03     | $16.79   |
| B1-S10-10N              | 10    | 25 min   | $4.03     | $16.79   |
| B1-S10-5N               | 5     | 30 min   | $4.03     | $8.40    |
| B1-S20-10N              | 10    | 20 min   | $4.03     | $13.43   |
| B1-S83-10N              | 10    | 25 min   | $4.03     | $16.79   |
| B1-S10-E32              | 10    | 25 min   | $2.02     | $8.40    |
| **Axis B1 total**       |       |          |           | **~$82** |

> Note: 10-node tests require ESv3 quota ≥ 640 vCPU (10×64). Current quota = 200. **Need quota increase request** or use E32s_v3 (10×32=320, still over). Alternative: E16s_v3 (10×16=160, fits!) for shard tests where DB is small enough (27 GB << 128 GB RAM).

**Quota mitigation**: Use **E16s_v3** ($1.008/hr, 16 vCPU, 128 GB RAM) for 10-node shard tests. 27 GB DB fits easily in 128 GB RAM. Cost drops to ~$4.20/test.

---

## 2. Axis B2: Taxonomy Subset — Search Only Relevant Organisms

### 2.1 Concept

The customer searches for 3 pathogens:

- SARS-CoV-2 (taxid 2697049, within Virus 10239)
- Monkeypox (taxid 10242, Orthopoxvirus, within Virus 10239)
- P. falciparum (taxid 5820, Plasmodium)

In `core_nt` (269 GB, ~100 billion nt), the vast majority of sequences are from organisms irrelevant to pathogen detection. A **taxonomy-filtered subset** dramatically reduces the search space.

### 2.2 Estimated Subset Sizes

| Subset         | Taxa Included                     | Est. Size   | Est. % of core_nt | BLAST Time Est. |
| -------------- | --------------------------------- | ----------- | ----------------- | --------------- |
| **pathogen**   | Virus (10239) + Plasmodium (5820) | ~5-15 GB    | 2-6%              | **1-3 min**     |
| **virus_only** | Virus (10239)                     | ~3-10 GB    | 1-4%              | **<2 min**      |
| **broad**      | Virus + Bacteria + Plasmodium     | ~50-80 GB   | 19-30%            | **10-17 min**   |
| **no_human**   | Everything except Human (9606)    | ~240-260 GB | 89-97%            | **50-55 min**   |

> These are **estimates** based on NCBI taxonomy distributions. Actual sizes must be measured by extracting subsets.

### 2.3 How It Works

**Step 1: Extract subset on prep VM** (one-time)

```bash
# Create taxid file
echo -e "10239\n5820" > pathogen_taxids.txt

# Extract sequences
blastdbcmd -db core_nt -taxidlist pathogen_taxids.txt -out pathogen_subset.fa

# Build new DB (with taxonomy metadata for future subset-of-subset)
makeblastdb -in pathogen_subset.fa -dbtype nucl \
    -out core_nt_pathogen -title "core_nt pathogen subset" \
    -parse_seqids -blastdb_version 5

# Verify
blastdbcmd -db core_nt_pathogen -info
```

**Step 2: Upload to Blob**

```bash
azcopy cp "core_nt_pathogen*" \
    "https://stgelb.blob.core.windows.net/blast-db/core_nt_pathogen/"
```

**Step 3: Run ElasticBLAST with subset DB** — no code changes, just change INI config

```ini
[blast]
db = https://stgelb.blob.core.windows.net/blast-db/core_nt_pathogen/core_nt_pathogen
```

### 2.4 Trade-offs

| Aspect         | Full core_nt               | Pathogen Subset                               |
| -------------- | -------------------------- | --------------------------------------------- |
| Sensitivity    | Detects all organisms      | Only Virus + Plasmodium                       |
| Novel pathogen | Can discover unexpected    | **Misses non-virus/plasmodium**               |
| DB size        | 269 GB                     | ~5-15 GB                                      |
| BLAST time     | 57 min                     | **~1-3 min**                                  |
| E-values       | Reference standard         | Different (smaller DB = different statistics) |
| Maintenance    | NCBI updates automatically | Must rebuild subset periodically              |

**Customer decision required**: Is missing non-virus/non-plasmodium hits acceptable? For a dedicated pathogen detection panel, likely yes. For general metagenomics screening, no.

### 2.5 Test Matrix

| Test ID         | Subset                        | Est. Size | Nodes | VM      | Measures                |
| --------------- | ----------------------------- | --------- | ----- | ------- | ----------------------- |
| B2-pathogen-1N  | Virus + Plasmodium            | ~5-15 GB  | 1     | E64s_v3 | Subset speedup          |
| B2-virus-1N     | Virus only                    | ~3-10 GB  | 1     | E64s_v3 | Minimum subset          |
| B2-broad-1N     | Virus + Bacteria + Plasmodium | ~50-80 GB | 1     | E64s_v3 | Broader coverage        |
| B2-pathogen-E16 | Virus + Plasmodium            | ~5-15 GB  | 1     | E16s_v3 | Cheaper VM (small DB)   |
| **B2-val**      | **Correctness validation**    | —         | 1     | —       | Compare vs full core_nt |

**Total**: 5 tests (including correctness validation)

### 2.6 Correctness Validation

```bash
# Full DB reference
blastn -db core_nt -query pathogen-10.fa -outfmt "6 std" \
    -max_target_seqs 500 -out full_results.out

# Subset
blastn -db core_nt_pathogen -query pathogen-10.fa -outfmt "6 std" \
    -max_target_seqs 500 -out subset_results.out

# Validation: subset hits must be a SUBSET of full-DB hits
# All subset hits should appear in full results (possibly with different E-values)
comm -23 <(cut -f2 subset_results.out | sort -u) \
         <(cut -f2 full_results.out | sort -u)
# Expected: empty (no extra hits in subset)
```

### 2.7 Cost Estimate

| Item                     | Duration               | Cost    |
| ------------------------ | ---------------------- | ------- |
| Subset prep VM (D32s_v3) | 2 hr (extract + build) | $3.06   |
| B2-pathogen-1N           | 20 min                 | $1.34   |
| B2-virus-1N              | 20 min                 | $1.34   |
| B2-broad-1N              | 30 min                 | $2.02   |
| B2-pathogen-E16          | 15 min                 | $0.25   |
| B2-val (local)           | 10 min                 | $0.00   |
| **Axis B2 total**        |                        | **~$8** |

---

## 3. Axis B3: MegaBLAST Indexing (`makembindex`)

### 3.1 Concept

BLAST+ includes `makembindex`, which creates a compressed word-lookup index for MegaBLAST. Instead of scanning every position in the DB, MegaBLAST can jump directly to seed match locations.

```
Without index: scan all 100B nucleotides → O(DB_size)     → 57 min
With index:    lookup seed positions      → O(num_hits)    → ???
```

### 3.2 How It Works

**Step 1: Build index** (one-time, on prep VM)

```bash
# Build MegaBLAST index for core_nt
# -iformat blastdb: input is an existing BLAST DB
# -old_style_index false: use new compressed format (smaller)
makembindex -input core_nt -iformat blastdb -old_style_index false

# Creates core_nt.00.idx, core_nt.01.idx, ... (one per volume)
# Index size: typically 60-80% of DB size
```

**Step 2: Use index in BLAST search**

```bash
blastn -db core_nt -query pathogen-10.fa \
    -use_index true -index_name core_nt \
    -outfmt 7 -out results.out
```

### 3.3 ElasticBLAST Constraint

Currently, `-use_index` is in the `UNSUPPORTED_OPTIONS` list in `elb_config.py:542`:

```python
UNSUPPORTED_OPTIONS = {
    '-remote',
    '-use_index',     # ← blocks MegaBLAST indexing
    '-index_name',    # ← blocks specifying index location
    ...
}
```

**Workaround for benchmarking** (no production code change needed):

1. Pass `-use_index true -index_name core_nt` via a modified `blast-run-aks.sh` script that injects the flag
2. Or: temporarily remove from UNSUPPORTED_OPTIONS for Azure CSP only

For **production** use: Add a config option like `exp-use-megablast-index = true` that enables `-use_index` + `-index_name` injection.

### 3.4 Constraints and Risks

| Constraint           | Impact                                                                                                                                                 |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **MegaBLAST only**   | Only works with `blastn` using default megablast task. Does NOT work with `blastn -task blastn`, `blastn -task dc-megablast`, `blastx`, `blastp`, etc. |
| **Index size**       | ~60-80% of DB size → core_nt index ≈ 160-215 GB additional storage                                                                                     |
| **Index build time** | Hours for large DBs (one-time cost)                                                                                                                    |
| **Memory**           | Index must fit in memory alongside DB → larger RAM requirement                                                                                         |
| **Effectiveness**    | Most benefit for short queries against large DBs (our exact scenario)                                                                                  |
| **Upstream support** | `makembindex` is stable in BLAST+ 2.17.0 but not widely documented                                                                                     |

### 3.5 Expected Speedup

Literature and BLAST+ documentation suggest:

- Short queries (< 10 KB) against large DBs (> 10 GB): **2-10x speedup**
- Our scenario (37 KB queries, 269 GB DB): likely **3-5x speedup** (from 57 min to ~12-19 min)

Combined with sharding (10 shards + index): potentially **20-50x** (from 57 min to ~1-3 min).

### 3.6 Test Matrix

| Test ID          | DB               | Index | Nodes | VM      | Measures                       |
| ---------------- | ---------------- | ----- | ----- | ------- | ------------------------------ |
| B3-idx-1N        | core_nt (full)   | Yes   | 1     | E64s_v3 | Index speedup baseline         |
| B3-noidx-1N      | core_nt (full)   | No    | 1     | E64s_v3 | Control (same as v2 A1-E64-10) |
| B3-idx-pathogen  | core_nt_pathogen | Yes   | 1     | E64s_v3 | Index + subset combo           |
| B3-idx-shard-10N | 10 shards        | Yes   | 10    | E16s_v3 | Index + shard combo            |
| **B3-val**       | Correctness      | —     | 1     | —       | Compare vs non-indexed results |

**Total**: 5 tests (including control + validation)

### 3.7 Cost Estimate

| Item                     | Duration                     | Cost     |
| ------------------------ | ---------------------------- | -------- |
| Index build VM (D64s_v3) | 4 hr (core_nt index build)   | $9.80    |
| B3-idx-1N                | 40 min                       | $2.69    |
| B3-noidx-1N              | skip (use v2 A1-E64-10 data) | $0       |
| B3-idx-pathogen          | 15 min                       | $1.01    |
| B3-idx-shard-10N         | 20 min                       | $3.36    |
| B3-val (local)           | 10 min                       | $0       |
| **Axis B3 total**        |                              | **~$17** |

---

## 4. Combined Strategies — The Breakthrough Scenarios

### 4.1 Strategy Comparison (Projected)

| Strategy                            | DB/Node | Download | BLAST      | Cold Wall | Warm Wall  | Cost/Run |
| ----------------------------------- | ------- | -------- | ---------- | --------- | ---------- | -------- |
| **v2 baseline** (full DB, 2N)       | 269 GB  | 28 min   | 10 min     | 46 min    | ~10 min    | $5.40    |
| **B1: 10-shard** (10×E16s)          | 27 GB   | 3 min    | ~6 min     | 24 min    | ~6 min     | ~$2.69   |
| **B2: Taxonomy subset** (1N)        | ~10 GB  | 1 min    | ~2 min     | 18 min    | ~2 min     | ~$1.21   |
| **B3: MegaBLAST index** (1N)        | 430 GB† | 40 min   | ~15 min    | 70 min    | ~15 min    | ~$4.70   |
| **B1+B2: Shard × Subset** (5×E16s)  | 2 GB    | <1 min   | <1 min     | 17 min    | **<1 min** | ~$0.28   |
| **B2+B3: Subset + Index** (1N)      | ~15 GB  | 2 min    | **<1 min** | 19 min    | **<1 min** | ~$0.34   |
| **B1+B2+B3: All combined** (3×E16s) | ~5 GB   | <1 min   | **<30s**   | 17 min    | **<30s**   | ~$0.14   |

† DB (269 GB) + index (~160 GB)

### 4.2 Breakthrough Target

For the pathogen detection service, the **B1+B2 combination** is the most practical breakthrough:

| Metric          | v2 Best (E64s×2N warm) | Target (Subset+Shard warm)    | Improvement |
| --------------- | ---------------------- | ----------------------------- | ----------- |
| BLAST time      | 10 min                 | **<1 min**                    | **>10x**    |
| DB download     | 28 min/node            | <1 min/node                   | **>28x**    |
| Cold wall clock | 46 min                 | 17 min (AKS create dominates) | **2.7x**    |
| Warm wall clock | ~10 min                | **<1 min**                    | **>10x**    |
| Cost/run (warm) | $1.34                  | **$0.14-0.28**                | **5-10x**   |
| Node cost/hr    | $8.06 (2×E64s)         | $1.51-5.04 (3-5×E16s)         | **2-5x**    |

### 4.3 Combined Test Matrix

| Test ID          | Strategy | DB              | Shards | Nodes | VM      | Priority |
| ---------------- | -------- | --------------- | ------ | ----- | ------- | -------- |
| C1-subset-shard5 | B1+B2    | pathogen subset | 5      | 5     | E16s_v3 | **P1**   |
| C2-subset-shard3 | B1+B2    | pathogen subset | 3      | 3     | E16s_v3 | **P1**   |
| C3-subset-idx    | B2+B3    | pathogen subset | —      | 1     | E16s_v3 | P2       |
| C4-all           | B1+B2+B3 | pathogen subset | 3      | 3     | E16s_v3 | P3       |

---

## 5. Execution Plan

### Phase 0: DB Preparation (Day 0, ~4 hours)

| #    | Task                                      | Tool                                   | Output                      | Duration |
| ---- | ----------------------------------------- | -------------------------------------- | --------------------------- | -------- |
| 0.1  | Measure core_nt taxonomy distribution     | `blastdbcmd -db core_nt -tax_info`     | actual sizes per taxon      | 30 min   |
| 0.2  | Create pathogen subset                    | `blastdbcmd` + `makeblastdb`           | core_nt_pathogen (~5-15 GB) | 1 hr     |
| 0.3  | Create virus-only subset                  | same                                   | core_nt_virus (~3-10 GB)    | 1 hr     |
| 0.4  | Create broad subset                       | same                                   | core_nt_broad (~50-80 GB)   | 1 hr     |
| 0.5  | Shard core_nt into 5/10/20 shards         | `blastdb_aliastool` (via `db_prep.py`) | alias DBs                   | 15 min   |
| 0.6  | Shard pathogen subset into 3/5 shards     | same                                   | alias DBs                   | 5 min    |
| 0.7  | Build MegaBLAST index for core_nt         | `makembindex`                          | \*.idx files (~160 GB)      | 2-4 hr   |
| 0.8  | Build MegaBLAST index for pathogen subset | `makembindex`                          | \*.idx files (~5 GB)        | 15 min   |
| 0.9  | Upload all to Blob Storage                | `azcopy cp`                            | blob containers             | 30 min   |
| 0.10 | Correctness validation (local)            | `blastn` + `diff`                      | validation report           | 30 min   |

**Prep VM**: Standard_D64s_v3 ($3.07/hr) or Standard_E64s_v3 ($4.03/hr) — needs enough RAM for `makeblastdb` on core_nt.

### Phase 1: Axis B2 — Taxonomy Subset (Day 1 morning, ~2 hours)

**Rationale**: Cheapest, fastest to run, highest expected ROI. If subset gives 10-50x, sharding may be unnecessary.

| #   | Test ID         | Config                       | Duration | Cost  |
| --- | --------------- | ---------------------------- | -------- | ----- |
| 1.1 | B2-pathogen-1N  | core_nt_pathogen, E64s_v3×1N | 20 min   | $1.34 |
| 1.2 | B2-virus-1N     | core_nt_virus, E64s_v3×1N    | 20 min   | $1.34 |
| 1.3 | B2-broad-1N     | core_nt_broad, E64s_v3×1N    | 30 min   | $2.02 |
| 1.4 | B2-pathogen-E16 | core_nt_pathogen, E16s_v3×1N | 15 min   | $0.25 |

**Gate**: If B2-pathogen-1N BLAST time < 3 min → **proceed to combined tests (Phase 3)**. Skip Phase 2 (full DB sharding is redundant if subset is sufficient).

### Phase 2: Axis B1 — DB Sharding (Day 1 afternoon, ~3 hours)

Run only if B2 alone is insufficient or customer needs full core_nt coverage.

| #   | Test ID    | Config                  | Duration | Cost  |
| --- | ---------- | ----------------------- | -------- | ----- |
| 2.1 | B1-S5-5N   | 5 shards on 5×E16s_v3   | 30 min   | $4.20 |
| 2.2 | B1-S10-10N | 10 shards on 10×E16s_v3 | 25 min   | $4.20 |
| 2.3 | B1-S10-5N  | 10 shards on 5×E16s_v3  | 30 min   | $4.20 |
| 2.4 | B1-S10-E32 | 10 shards on 10×E32s_v3 | 25 min   | $8.40 |

### Phase 3: Combined Strategies (Day 2, ~2 hours)

| #   | Test ID          | Strategy                                    | Duration | Cost  |
| --- | ---------------- | ------------------------------------------- | -------- | ----- |
| 3.1 | C1-subset-shard5 | Subset (pathogen) + 5 shards on 5×E16s      | 20 min   | $1.68 |
| 3.2 | C2-subset-shard3 | Subset (pathogen) + 3 shards on 3×E16s      | 20 min   | $1.01 |
| 3.3 | C3-subset-idx    | Subset (pathogen) + MegaBLAST index, 1×E16s | 15 min   | $0.25 |

### Phase 4: Axis B3 — MegaBLAST Index (Day 2, ~2 hours)

| #   | Test ID         | Config                              | Duration | Cost  |
| --- | --------------- | ----------------------------------- | -------- | ----- |
| 4.1 | B3-idx-1N       | core_nt + index, E64s_v3×1N         | 40 min   | $2.69 |
| 4.2 | B3-idx-pathogen | pathogen subset + index, E16s_v3×1N | 15 min   | $0.25 |

---

## 6. Quota Requirements

| VM Family | Current Quota | Needed (max) | Test           | Action                      |
| --------- | ------------- | ------------ | -------------- | --------------------------- |
| ESv3      | 200 vCPU      | 640 (10×E64) | B1-S10-10N     | **Use E16s instead**        |
| ESv3      | 200 vCPU      | 160 (10×E16) | B1-S10-10N-E16 | ✓ fits                      |
| ESv3      | 200 vCPU      | 320 (10×E32) | B1-S10-E32     | **Request increase to 320** |
| DSv3      | 100 vCPU      | 64 (1×D64)   | Prep VM        | ✓ fits                      |

**Recommendation**: Run all multi-node shard tests on **E16s_v3** (128 GB RAM). A 27 GB shard easily fits. No quota increase needed.

---

## 7. Budget Summary

| Phase                     | Tests              | Est. Cost |
| ------------------------- | ------------------ | --------- |
| Phase 0 (prep)            | DB builds, uploads | ~$15      |
| Phase 1 (taxonomy subset) | 4 tests            | ~$5       |
| Phase 2 (DB sharding)     | 4 tests            | ~$21      |
| Phase 3 (combined)        | 3 tests            | ~$3       |
| Phase 4 (MegaBLAST index) | 2 tests            | ~$3       |
| Cluster overhead          | —                  | ~$10      |
| **Total**                 | **~17 tests**      | **~$57**  |

Well within $150 budget, leaving room for reruns and additional experiments.

---

## 8. Expected Deliverables

### 8.1 Benchmark Report (`results/v3/report.md`)

1. **Taxonomy subset sizing**: Actual sizes of pathogen/virus/broad subsets
2. **Sharding linearity**: Is BLAST time linear with DB size? Plot DB_size vs BLAST_time
3. **MegaBLAST index speedup**: With/without index comparison
4. **Combined strategy performance**: Best achievable wall clock and cost/run
5. **E-value correctness**: Validation of sharded and subset results vs full DB reference
6. **Customer recommendation**: Updated config recommendation table

### 8.2 Code Artifacts

1. **`benchmark/prep_db_v3.sh`**: One-shot script to create all subset/shard/index DBs
2. **`benchmark/configs/v3/*.ini`**: INI configs for each test
3. **`benchmark/strategies/merger.py`**: Validated result merger for sharded runs
4. **Updated `azure_optimizer.py`**: `recommend_config()` with DB optimization strategy selection

### 8.3 Updated Customer Recommendation

| Scenario            | Strategy               | Config                   | BLAST Time | Cost/Run   |
| ------------------- | ---------------------- | ------------------------ | ---------- | ---------- |
| Known pathogens     | Taxonomy subset (warm) | E16s×1N, pathogen DB     | **<2 min** | **<$0.10** |
| Broad screening     | 10-shard (warm)        | E16s×10N, full DB shards | **<6 min** | **<$2.00** |
| Maximum sensitivity | Full DB (warm)         | E64s×2N, full core_nt    | ~10 min    | ~$1.34     |

---

## 9. Risks and Mitigations

| Risk                                            | Impact                               | Mitigation                                                      |
| ----------------------------------------------- | ------------------------------------ | --------------------------------------------------------------- |
| BLAST time NOT linear with DB size              | Sharding speedup less than projected | Measure with 2+ data points, update model                       |
| Taxonomy subset too small (under-detection)     | Customer rejects approach            | Offer "broad" subset (Virus+Bacteria+Plasmodium) as alternative |
| `-dbsize` E-value correction insufficient       | Incorrect ranking in sharded results | Validate with reference full-DB search                          |
| `makembindex` fails on core_nt 83 volumes       | No index-based tests                 | Try on pathogen subset first (fewer volumes)                    |
| `makembindex` index too large for node RAM      | Cannot use index effectively         | Use E64s or skip B3, focus on B1+B2                             |
| AKS quota blocks 10-node tests                  | Cannot run B1-S10 tests              | Use E16s_v3 (160 vCPU), or 5-node tests only                    |
| `-use_index` blocked by ElasticBLAST validation | Cannot run B3 through ElasticBLAST   | Direct BLAST execution in custom pod script                     |

---

## 10. Decision Tree

```
START
│
├─ Customer needs full core_nt coverage?
│  ├─ YES → Axis B1 (DB Sharding)
│  │         ├─ 10-shard on 10×E16s → ~6 min BLAST, ~$2/run
│  │         └─ Add B3 (index) if still too slow → ~3 min BLAST
│  │
│  └─ NO (known pathogens only) → Axis B2 (Taxonomy Subset)
│           ├─ pathogen subset on 1×E16s → ~1-2 min BLAST, ~$0.10/run
│           ├─ Too slow? → B1+B2 (shard the subset) → <1 min
│           └─ Need broader coverage? → "broad" subset (Virus+Bacteria+Plasmodium)
│
└─ Both? → Tiered approach
         ├─ Fast screen: pathogen subset (~1 min)
         └─ If ambiguous → full core_nt sharded search (~6 min)
```

---

## Appendix A: BLAST+ Tool Reference

| Tool                | Purpose                                     | Used In                 |
| ------------------- | ------------------------------------------- | ----------------------- |
| `blastdb_aliastool` | Create virtual shard DBs from volume groups | B1 (sharding)           |
| `blastdbcmd`        | Extract sequences by taxid, get DB metadata | B2 (subset creation)    |
| `makeblastdb`       | Build new BLAST DB from FASTA               | B2 (subset DB build)    |
| `makembindex`       | Build MegaBLAST word-lookup index           | B3 (indexing)           |
| `blastn -dbsize`    | Override DB size for E-value correction     | B1 (shard result merge) |
| `blastn -use_index` | Enable indexed MegaBLAST search             | B3 (indexed search)     |

All tools are part of **BLAST+ 2.17.0** (included in `ncbi/elb:1.4.0` container image).

## Appendix B: Relationship to Existing Code

| Component                    | File                                       | Status                              |
| ---------------------------- | ------------------------------------------ | ----------------------------------- |
| `shard_db()`                 | `benchmark/strategies/db_prep.py`          | ✅ Implemented, never run           |
| `create_taxonomy_subset()`   | `benchmark/strategies/db_prep.py`          | ✅ Implemented, never run           |
| `DBShardStrategy`            | `benchmark/strategies/blast_strategies.py` | ✅ Implemented, never run           |
| `TaxonomyStrategy`           | `benchmark/strategies/blast_strategies.py` | ✅ Implemented, never run           |
| `HybridStrategy`             | `benchmark/strategies/blast_strategies.py` | ✅ Implemented, never run           |
| `PreloadedStrategy`          | `benchmark/strategies/blast_strategies.py` | ✅ Implemented, never run           |
| `init-db-partitioned-aks.sh` | `templates/scripts/`                       | ✅ Implemented, never run           |
| `db-partitions` config       | `constants.py`, `elb_config.py`            | ✅ Config keys defined              |
| `merger.py`                  | `benchmark/strategies/merger.py`           | ⚠️ Exists, needs E-value validation |
| `-use_index` support         | `elb_config.py`                            | ❌ Blocked (UNSUPPORTED_OPTIONS)    |
| `makembindex` integration    | —                                          | ❌ Not implemented                  |
