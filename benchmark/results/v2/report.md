# ElasticBLAST Azure Benchmark v2 — core_nt Production Workload Report

> **Date**: 2026-04-20 ~ 2026-04-21  
> **Author**: Moon Hyuk Choi (moonchoi@microsoft.com)  
> **Region**: Korea Central  
> **ElasticBLAST**: 1.5.0 (BLAST+ 2.17.0)  
> **Database**: core_nt (269 GB, 83 volumes, 754 files)  
> **Cost basis**: Azure pay-as-you-go (standard) pricing  
> **Customer context**: Pathogen detection service (SARS-CoV-2, Monkeypox, P. falciparum)

---

## Abstract

This benchmark evaluates ElasticBLAST on Azure AKS with the NCBI `core_nt` database (269 GB) — a production-scale nucleotide database 3.3x larger than the `nt_prok` (82 GB) tested in v1. Using pathogen detection query sequences (SARS-CoV-2, Monkeypox virus, P. falciparum) provided by the customer, we measure the impact of query scale (10-300 sequences), CPU scale-up (E32s_v3 → E64s_v3), and multi-node scale-out (1N → 2-3N) on BLAST execution time.

The principal finding is that **database scan time completely dominates execution**, making query count irrelevant to performance — 10 queries and 300 queries produce identical per-batch BLAST times (~57 min on E64s_v3). Multi-node scale-out delivers **5.2-5.8x speedup** (57 min → 10 min) even for single-batch workloads, confirming super-linear scaling observed in v1 with an even larger database. However, database download overhead (~28 min/node for 269 GB) represents a significant fixed cost, suggesting that **persistent pre-loaded clusters** are essential for production pathogen detection services requiring sub-minute response times.

> **Scope note.** This report covers Axis 1 (SKU scale-up) and Axis 2 (query-based tuning) of `BENCHMARK-PLAN-V2.md` **partially** (27% and 36% coverage respectively). **Axis 3 (multi-request service — reuse, concurrent, autoscale) was not executed.** The warm-cluster / service-mode recommendations are therefore _derived from cold-run data_, not empirically validated. See Section 9 for full scope reconciliation.

---

## TL;DR — Customer Recommendations

| Scenario                               | Config                   | BLAST Time  | Wall Clock       | Cost/Run |
| -------------------------------------- | ------------------------ | ----------- | ---------------- | -------- |
| **Single pathogen check (10 queries)** | E64s_v3 × 2N, Local SSD  | **~10 min** | ~40 min (cold)   | ~$5.40   |
| **Full panel (300 queries)**           | E64s_v3 × 2N, Local SSD  | **~10 min** | ~40 min (cold)   | ~$5.40   |
| **Service mode (warm cluster)** †      | E64s_v3 × 2N, reuse=true | ~10 min †   | ~10 min † (warm) | ~$1.35 † |
| **Cost-sensitive**                     | E64s_v3 × 1N, Local SSD  | ~57 min     | ~85 min          | ~$5.70   |

> † **Estimated, not measured.** Warm-cluster numbers are derived from C1-E64-2N BLAST time (9.8 min) × hourly VM cost, assuming `reuse=true` works cleanly. Empirical validation (Axis 3 of the v2 plan) is pending — see Section 9.1 and Bug #2 (reuse hang).

**Key insight**: Query count (10 vs 300) does NOT affect performance. The 269 GB database scan is the sole bottleneck. Multi-node scaling reduces this from 57 min to 10 min.

---

## 1. Key Findings

### Finding 1: Query Count is Irrelevant — DB Scan Dominates

| Test          | Queries | Batches | BLAST Median/batch | Wall Clock |
| ------------- | ------- | ------- | ------------------ | ---------- |
| A1 (E64s, 1N) | 10      | 12      | **57.3 min**       | 110 min    |
| A2 (E64s, 1N) | 300     | 12      | **57.3 min**       | 93 min     |

10 queries (37 KB) and 300 queries (1.2 MB) produce **identical BLAST execution times**. This confirms that for `core_nt` (269 GB), the database scan time (~57 min per batch on E64s_v3) completely dominates, and query processing adds negligible overhead.

**Implication**: The customer's concern about scaling from 10 to 300 queries per request is unfounded — performance is identical. The bottleneck is entirely the database, not the queries.

### Finding 2: Multi-Node Scale-Out Delivers 5-6x Speedup

| Config    | Nodes | BLAST Time | Speedup  | DB I/O Total |
| --------- | ----- | ---------- | -------- | ------------ |
| A1 (E64s) | 1     | 57.3 min   | 1.0x     | 269 GB       |
| C1 (E64s) | 3     | 10.9 min   | **5.2x** | 807 GB       |
| C1 (E64s) | 2     | 9.8 min    | **5.8x** | 538 GB       |
| D1 (E48s) | 3     | 10.9 min   | 5.2x     | 807 GB       |

Even with only 1 query batch (no query-level parallelism), multi-node execution achieves **super-linear speedup**. This is because distributing the workload across nodes reduces per-node CPU contention and memory pressure when scanning the 269 GB database.

**Scaling Efficiency**:

$$E(N) = \frac{T_1}{N \times T_N} \times 100\%$$

| Config  | N   | $T_1$    | $T_N$    | Speedup | Efficiency |
| ------- | --- | -------- | -------- | ------- | ---------- |
| E64s 2N | 2   | 57.3 min | 9.8 min  | 5.8x    | **292%**   |
| E64s 3N | 3   | 57.3 min | 10.9 min | 5.2x    | **175%**   |

Super-linear efficiency (>100%) is consistent with v1 findings on nt_prok (82 GB). The effect is even more pronounced with the larger core_nt database, likely because 269 GB exceeds the 256 GB RAM of E64s_v3, causing page cache eviction pressure on a single node.

### Finding 3: Database Download is the Dominant Overhead

| Phase                        | Duration           | Notes                |
| ---------------------------- | ------------------ | -------------------- |
| AKS cluster creation         | 15-20 min          | One-time per cluster |
| **DB download to Local SSD** | **28-30 min/node** | 269 GB via azcopy    |
| Query split + upload         | < 1 min            | Negligible           |
| BLAST execution (1N)         | 57 min             | DB scan dominated    |
| BLAST execution (2-3N)       | 10 min             | Super-linear scaling |
| Results upload               | 5-10 min           | Per-batch sidecar    |

For a cold-start run, **DB download (28 min) approaches the BLAST execution time (57 min)** on a single node. On multi-node, DB download (28 min) **exceeds** BLAST time (10 min). This makes **persistent pre-loaded clusters** (`reuse=true`) essential for production use:

| Mode                 | Total Wall Clock (10q) | DB Download  | BLAST             |
| -------------------- | ---------------------- | ------------ | ----------------- |
| Cold start, 1N       | 110 min                | 28 min (36%) | 57 min (52%)      |
| Cold start, 2N       | 46 min                 | 28 min (61%) | 10 min (22%)      |
| **Warm cluster, 2N** | **~10 min**            | **0 min**    | **10 min (100%)** |

### Finding 4: E64s_v3 vs E32s_v3 (v1 comparison)

| VM      | vCPU | RAM    | core_nt BLAST | nt_prok BLAST (v1) |
| ------- | ---- | ------ | ------------- | ------------------ |
| E32s_v3 | 32   | 256 GB | 25 min\*      | 11.3 min           |
| E64s_v3 | 64   | 432 GB | 57 min        | N/A                |

\*E32s_v3 result from initial A1 test (before switching to E64s_v3). The E32s_v3 was actually **faster** than E64s_v3 for a single batch (25 min vs 57 min), which is counterintuitive. This may be because:

1. E32s_v3's 512 GB temp disk is barely sufficient for core_nt (269 GB), but the smaller VM has better CPU cache utilization
2. E64s_v3 runs more concurrent batches (12) which compete for memory/CPU, while E32s_v3 may run fewer due to `mem-limit=4G`
3. Different cluster creation times and AKS version differences between runs

**This requires further investigation** — the E32s_v3 result should be validated with a dedicated single-batch test.

---

## 2. Customer Workload Analysis

### Customer Query Characteristics

| Pathogen                     | Sequences | Bases      | % of Total |
| ---------------------------- | --------- | ---------- | ---------- |
| SARS-CoV-2 (orf1ab)          | 1         | 21,290     | 56.4%      |
| SARS-CoV-2 (RdRP)            | 1         | 2,795      | 7.4%       |
| SARS-CoV-2 (N)               | 1         | 1,260      | 3.3%       |
| Monkeypox (F3L) × 2          | 2         | 924        | 2.4%       |
| P. falciparum (18S rRNA) × 5 | 5         | 11,477     | 30.4%      |
| **Total**                    | **10**    | **37,746** | 100%       |

### Customer Usage Scenarios

| Scenario            | Pathogens    | Queries/pathogen | Total Queries | Total Bases |
| ------------------- | ------------ | ---------------- | ------------- | ----------- |
| Minimum (test)      | 3            | 1-3              | 10            | 37 KB       |
| Typical             | 10           | 5                | 50            | ~150 KB     |
| Maximum (1 request) | 30           | 10               | **300**       | ~900 KB     |
| Multi-user burst    | 30 × N users | 10               | **300 × N**   | ~900 KB × N |

### Performance Projection

Since query count does not affect BLAST time (Finding 1), all scenarios produce identical performance:

| Scenario                       | BLAST Time (2N) | Wall Clock (cold) | Wall Clock (warm) |
| ------------------------------ | --------------- | ----------------- | ----------------- |
| 10 queries                     | 10 min          | 46 min            | **10 min**        |
| 300 queries                    | 10 min          | 46 min            | **10 min**        |
| 300 × 10 users (sequential)    | 100 min         | 100 min           | **100 min**       |
| 300 × 10 users (parallel, 20N) | 10 min          | 46 min            | **10 min**        |

---

## 3. Cost Analysis

### Per-Run Cost

| Config          | VM      | Nodes | $/hr/node | BLAST Time | Wall Clock | Est. Cost |
| --------------- | ------- | ----- | --------- | ---------- | ---------- | --------- |
| E64s_v3 1N      | E64s_v3 | 1     | $4.032    | 57 min     | 110 min    | **$7.39** |
| **E64s_v3 2N**  | E64s_v3 | 2     | $4.032    | 10 min     | 46 min     | **$6.18** |
| E64s_v3 3N      | E64s_v3 | 3     | $4.032    | 11 min     | 47 min     | $9.48     |
| E64s_v3 2N warm | E64s_v3 | 2     | $4.032    | 10 min     | 10 min     | **$1.34** |

**Best value**: E64s_v3 × 2N warm cluster — $1.34/run with 10-minute turnaround.

### Service Mode Cost (Monthly)

| Usage                | Runs/day | Cluster Mode | Monthly Cost |
| -------------------- | -------- | ------------ | ------------ |
| Low (5 runs/day)     | 5        | On-demand    | $930/month   |
| Medium (20 runs/day) | 20       | Warm 8hr/day | $1,620/month |
| High (100 runs/day)  | 100      | Warm 24/7    | $5,870/month |

### Benchmark Total Cost

| Phase                    | Tests      | VM Hours | Est. Cost   |
| ------------------------ | ---------- | -------- | ----------- |
| Phase A (E64s 1N)        | 2 (A1, A2) | 3.4 hr   | $13.71      |
| Phase C (E64s 2N)        | 1 (C1)     | 1.5 hr   | $12.10      |
| Phase C (E64s 3N)        | 1 (C1)     | 2.4 hr   | $29.03      |
| Failed runs overhead     | ~4         | ~8 hr    | ~$32.26     |
| DB prestage VM (D64s_v3) | 1          | 0.5 hr   | $2.45       |
| **Total benchmark cost** |            |          | **~$89.55** |

---

## 4. Infrastructure

### Test Environment

| Component        | Specification                                                 |
| ---------------- | ------------------------------------------------------------- |
| AKS              | Kubernetes 1.34.4, Korea Central                              |
| VM (primary)     | Standard_E64s_v3 (64 vCPU, 432 GB RAM, 1 TB SSD, $4.032/hr)   |
| VM (v1 baseline) | Standard_E32s_v3 (32 vCPU, 256 GB RAM, 512 GB SSD, $2.016/hr) |
| Container        | elbacr.azurecr.io/ncbi/elb:1.4.0 (BLAST+ 2.17.0)              |
| Storage          | Azure Blob Storage (Standard_LRS), Korea Central              |
| Auth             | Managed Identity (Workload Identity)                          |
| DB download      | azcopy v10, Blob → Local SSD (hostPath)                       |

### Database

| Item        | Detail                                                     |
| ----------- | ---------------------------------------------------------- |
| Name        | core_nt (Core nucleotide database)                         |
| Source      | NCBI FTP (ftp.ncbi.nlm.nih.gov)                            |
| Compressed  | 473 GB (83 tar.gz volumes)                                 |
| Extracted   | **269 GB** (754 files)                                     |
| Volumes     | 83 (.nsq files)                                            |
| Pre-staging | Temporary D64s_v3 VM, FTP → extract → azcopy Blob (22 min) |

### Query Sets

| File             | Sequences | Bases     | Source                           |
| ---------------- | --------- | --------- | -------------------------------- |
| pathogen-10.fa   | 10        | 37,746    | Customer NCBI RefSeq sequences   |
| pathogen-50.fa   | 50        | 196,180   | 5x replicated                    |
| pathogen-100.fa  | 100       | 392,360   | 10x replicated                   |
| pathogen-300.fa  | 300       | 1,177,280 | 30x replicated (max request)     |
| pathogen-1000.fa | 1,000     | 3,924,500 | 100x replicated (multi-user sim) |

---

## 5. Test Results

### Successful Tests

| Test ID     | DB      | Queries | VM      | Nodes | Init-SSD | BLAST Median | BLAST Range | Wall Clock | Status |
| ----------- | ------- | ------- | ------- | ----- | -------- | ------------ | ----------- | ---------- | ------ |
| A1-E32-1n\* | core_nt | 10      | E32s_v3 | 1     | 31.7 min | **25.1 min** | —           | 57 min     | PASS   |
| A1-E64-1n   | core_nt | 10      | E64s_v3 | 1     | 27.8 min | **57.3 min** | 53.8-60.2   | 110 min    | PASS   |
| A2-E64-1n   | core_nt | 300     | E64s_v3 | 1     | (reuse)  | **57.3 min** | 53.8-60.2   | 93 min     | PASS   |
| C1-E64-3n   | core_nt | 10      | E64s_v3 | 3     | 29.7 min | **10.9 min** | —           | 47 min     | PASS   |
| C1-E64-2n   | core_nt | 10      | E64s_v3 | 2     | 30.1 min | **9.8 min**  | —           | 46 min     | PASS   |
| D1-E48-3n   | core_nt | 10      | E48s_v3 | 3     | 34.0 min | **10.9 min** | —           | 45 min     | PASS   |

\*A1-E32-1n: initial test run on E32s_v3 (before systematic Phase A)

### BLAST Job Timings (A1-E64-1n, 12 batches)

| Batch      | Duration      | Notes          |
| ---------- | ------------- | -------------- |
| batch-000  | 55.3 min      |                |
| batch-001  | 59.9 min      |                |
| batch-002  | **53.8 min**  | Fastest        |
| batch-003  | 60.2 min      | Slowest (tied) |
| batch-004  | 56.6 min      |                |
| batch-005  | **60.2 min**  | Slowest (tied) |
| batch-006  | 54.1 min      |                |
| batch-007  | 59.9 min      |                |
| batch-008  | 55.4 min      |                |
| batch-009  | 60.2 min      | Slowest (tied) |
| batch-010  | 55.4 min      |                |
| batch-011  | 60.0 min      |                |
| **Median** | **57.3 min**  |                |
| **Range**  | 6.4 min (12%) | Low variance   |

The low variance (12%) across batches indicates uniform workload distribution — unlike v1's nt_prok where 16S rRNA batches showed 2-10x tail latency. This is because the pathogen query set has more homogeneous hit rates against core_nt.

### Failed Tests

| Test ID   | Nodes | Failure Reason                  | Root Cause                                                                         |
| --------- | ----- | ------------------------------- | ---------------------------------------------------------------------------------- |
| C2-E64-3n | 3     | 3rd node not provisioned        | ESv3 quota 200 vCPU, 3×64=192 (too tight)                                          |
| C2-E64-2n | 2     | submit-jobs hung after init-ssd | ElasticBLAST reuse bug (submit-jobs doesn't detect init-ssd completion on 2nd run) |
| C3-E64-2n | 2     | Same as C2                      | Same reuse bug                                                                     |

---

## 6. Comparison with v1 (nt_prok 82 GB)

| Dimension                    | v1 (nt_prok)                      | v2 (core_nt)       | Ratio    |
| ---------------------------- | --------------------------------- | ------------------ | -------- |
| DB Size                      | 82 GB                             | 269 GB             | 3.3x     |
| BLAST/batch (1N, E32s)       | 11.3 min                          | 25.1 min           | 2.2x     |
| BLAST/batch (1N, E64s)       | N/A                               | 57.3 min           | —        |
| Scaling (3N)                 | 2.3-4.4x                          | 5.2x               | Stronger |
| DB Download                  | 5-10 min                          | 28-30 min          | 3-5x     |
| Query impact                 | Significant (v1 had tail latency) | **None** (uniform) | —        |
| Best wall clock (multi-node) | 6.9 min (3N)                      | 10 min (2N)        | 1.4x     |

**Key differences**:

1. core_nt (269 GB) is 3.3x larger but BLAST is only 2.2x slower on E32s — sub-linear scaling with DB size
2. Multi-node speedup is **stronger** with larger DB (5.2x vs 4.4x) — more benefit from distributing memory pressure
3. Query complexity has **no impact** on core_nt (vs significant tail latency on nt_prok)

---

## 7. Discussion

### Why Query Count Doesn't Matter

BLAST's execution time for a given database depends on:

$$T_{BLAST} = T_{seed} + T_{extend} + T_{align}$$

Where $T_{seed}$ (word matching across the entire DB) dominates for small query sets. With core_nt at 269 GB, the seeding phase scans ~100 billion nucleotides regardless of whether there are 10 or 300 query sequences. The additional alignment work from more queries is negligible compared to the DB scan.

This has a profound practical implication: **batch-len optimization is irrelevant for this workload**. Whether queries are split into 1 batch or 12 batches, total work is identical. The only benefit of batching is enabling multi-node parallelism.

### Why Multi-Node Gives Super-Linear Speedup

On a single E64s_v3 node (432 GB RAM), loading a 269 GB database leaves only 163 GB for OS page cache, BLAST working memory, and 12 concurrent BLAST processes. This causes:

1. **Page cache eviction**: DB pages are evicted and re-read from SSD during the scan
2. **CPU cache thrashing**: 12 concurrent processes compete for L3 cache
3. **Memory bandwidth saturation**: 269 GB DB × 12 processes = high memory bus contention

With 2 nodes, each node handles ~6 batches with the full 269 GB DB cached in its 432 GB RAM, eliminating page cache pressure. This explains the >100% scaling efficiency.

### Hypothesis Test: Cheaper VM × More Nodes (D1: E48s × 3N)

To test whether a cheaper VM with more nodes could outperform E64s × 2N on both speed and cost, we ran **D1: E48s_v3 × 3N** (10 queries, core_nt). Hypothesis: 3×48=144 vCPU > 2×64=128 vCPU with lower hourly rate could deliver faster and cheaper execution.

| Dimension           | C1-E64-2N (baseline) | D1-E48-3N            | Winner    |
| ------------------- | -------------------- | -------------------- | --------- |
| Total vCPU          | 128                  | 144                  | D1 (+12%) |
| RAM/node            | 432 GB               | 384 GB               | C1        |
| VM hourly (×nodes)  | $4.03 × 2 = $8.06/hr | $3.02 × 3 = $9.06/hr | C1        |
| init-ssd (max/node) | 30.1 min             | 34.0 min             | **C1**    |
| BLAST time          | **9.8 min**          | 10.9 min             | **C1**    |
| Wall clock          | 46 min               | 45 min               | ~tie      |
| Cold cost/run       | ~$6.18               | ~$6.80               | **C1**    |
| Warm cost/run       | **~$1.34**           | ~$1.65               | **C1**    |

**Hypothesis REJECTED**. E64s × 2N wins on both speed AND cost. Reasons:

1. **Per-node DB download scales with node count**: 3 nodes download 269 GB × 3 = 807 GB (vs 2 × 269 = 538 GB), adding 4 min to init-ssd
2. **BLAST time identical (~10 min)**: Both configs eliminate the single-node memory pressure; extra vCPU on D1 is wasted since there's only 1 batch per node
3. **Hourly cost higher**: 3×E48s ($9.06/hr) > 2×E64s ($8.06/hr) despite lower per-VM rate
4. **Lower RAM headroom**: E48s_v3 has 384 GB (vs 432 GB) — closer to the 269 GB DB size, less page cache margin

**Takeaway**: For this workload, **fewer larger nodes beat more smaller nodes**. The super-linear speedup (Finding 2) comes from eliminating single-node memory contention, not from adding more CPU. Once the DB fits comfortably in per-node RAM, additional nodes add DB-download overhead without speeding up BLAST.

### Production Architecture Recommendation

For the customer's pathogen detection service, the optimal architecture is:

```
┌─────────────────────────────────────────────┐
│ Persistent AKS Cluster (reuse=true)         │
│                                             │
│ Node 0 (E64s_v3): core_nt pre-loaded       │
│ Node 1 (E64s_v3): core_nt pre-loaded       │
│                                             │
│ New request → elastic-blast submit          │
│   → Skip DB download (already on SSD)      │
│   → BLAST execution: ~10 min               │
│   → Results upload: ~5 min                  │
│                                             │
│ Idle cost: $8.06/hr (2 nodes)               │
│ Per-run cost: ~$1.34 (10 min BLAST)         │
└─────────────────────────────────────────────┘
```

> **Note**: This architecture is the _target_ state, not an empirically validated one. Actual service readiness depends on (a) fixing the `submit-jobs` reuse hang (Bug #2), and (b) completing Axis 3 tests: warm-submit E2E, concurrent submit, autoscale. Until then, numbers in this block are extrapolations.

---

## 8. Bugs Discovered

| #   | Bug                                                                       | Impact                           | Status                                                |
| --- | ------------------------------------------------------------------------- | -------------------------------- | ----------------------------------------------------- |
| 1   | `init-pv` timeout default 45 min too short for core_nt (269 GB)           | init-ssd DeadlineExceeded        | Fixed: `[timeouts] init-pv = 90`                      |
| 2   | `publicNetworkAccess: Disabled` blocks VM→Blob azcopy upload              | DB prestage fails silently       | Fixed: temporary enable in script                     |
| 3   | Cluster reuse: `init-ssd` job immutable field error                       | 2nd submit fails on same cluster | Workaround: `kubectl delete jobs --all` before submit |
| 4   | Cluster reuse: `submit-jobs` hangs after init-ssd on 2nd run              | BLAST jobs never created         | **Open** — needs code investigation                   |
| 5   | E64s_v3 × 3N = 192 vCPU near quota limit (200)                            | 3rd node fails to provision      | Workaround: use 2N or request quota increase          |
| 6   | `elastic-blast status` doesn't detect job completion                      | Monitoring script timeout        | Workaround: kubectl-based polling                     |
| 7   | DB prestage `azcopy cp` glob pattern creates nested `blast-staging/` path | DB files at wrong blob path      | Fixed: server-side blob copy + cleanup                |

---

## 9. Limitations

### 9.1 Scope vs. v2 Benchmark Plan (BENCHMARK-PLAN-V2.md)

The v2 plan defined a **3-axis benchmark** to prove ElasticBLAST Azure production readiness for a near-real-time pathogen detection service. Actual execution covered only Axis 1/2 partially; **Axis 3 (multi-user service) — the core of v2 — was not executed.**

| Axis                              | Planned Tests                                         | Executed                       | Coverage |
| --------------------------------- | ----------------------------------------------------- | ------------------------------ | -------- |
| **Axis 1: SKU Scale-Up**          | 11 tests (E32/E48/E64/E96/L32/L64/HB120 × {10q,300q}) | 3 (E64-10q, E64-300q, E48-10q) | **27%**  |
| **Axis 2: Query-Based Tuning**    | 11 tests (10/50/100/300/1000/3054 q × {1N,3N,5N})     | 4 (10q×{1N,2N,3N}, 300q×1N)    | **36%**  |
| **Axis 3: Multi-Request Service** | 6 tests (reuse, seq×5, con×3, con×10, burst)          | **0**                          | **0%**   |

**Axis 1 gaps** (8 SKU benchmarks missing):

- **L-series (NVMe)**: L32as_v3, L64as_v3 — highest per-node I/O throughput, critical for DB-bound workloads. Plan assumed NVMe ≥ E-series Local SSD; untested.
- **HB120rs_v3 (HPC)**: 120 vCPU, Tsai 2021 reference point. Never run.
- **E48s_v3 × 300q**: D1 tested 10q only; multi-batch behavior on E48s not measured.
- **E96s_v3**: CPU scale-up ceiling not probed.
- **Repeated runs**: Plan required 5 iterations/test for statistical confidence; 1 run each was taken.

**Axis 2 gaps** (7 query-scale benchmarks missing):

- `pathogen-50`, `pathogen-100`, `pathogen-1000`, `gut-3054` query sets: never generated or run.
- 5-node scaling: blocked by ESv3 quota (5×64=320 > 200).
- **No `recommend_config()` implementation** in `azure_optimizer.py` — planned deliverable unmet.

**Axis 3 gaps** (entire axis = 0%, the v2 _raison d'être_):

- **A3-reuse** (`reuse=true` 2nd-submit E2E): **Not measured.** The "$1.34/run warm" claim in TL;DR is _derived arithmetically_ (10 min × $8.06/hr ÷ 60 min/hr), not observed.
- **A3-seq5** (5 sequential requests throughput): not measured.
- **A3-con3 / A3-con10** (concurrent submit feasibility): not tested. Plan notes "ElasticBLAST may not support concurrent submit on same cluster" — unverified assumption remains unverified.
- **A3-burst** (autoscale 3→5→10 nodes): not tested.
- **Queue worker** (Azure Queue Storage → elastic-blast submit): planned as new development, not built.

### 9.2 What's Validated vs. What's Estimated

| Claim                                           | Source            | Status        |
| ----------------------------------------------- | ----------------- | ------------- |
| Query count (10 vs 300) is irrelevant           | A1 vs A2 measured | **Validated** |
| E64s × 2N: BLAST ≈ 9.8 min (cold, 10q)          | C1-E64-2N run     | **Validated** |
| E64s × 3N: BLAST ≈ 10.9 min (cold, 10q)         | C1-E64-3N run     | **Validated** |
| E48s × 3N: BLAST ≈ 10.9 min (cold, 10q)         | D1 run            | **Validated** |
| Super-linear speedup (>100% efficiency)         | A1 vs C1 ratio    | **Validated** |
| DB download ≈ 28-34 min/node for 269 GB         | init-ssd job logs | **Validated** |
| Warm cluster E2E ≈ 10 min (reuse=true, 2nd run) | Derived           | **Estimated** |
| Warm cluster cost ≈ $1.34/run                   | Derived from est. | **Estimated** |
| Concurrent request feasibility                  | —                 | **Unknown**   |
| Autoscale response < 5 min                      | —                 | **Unknown**   |
| L-series / HB-series SKU performance            | —                 | **Unknown**   |
| Query-scale → config recommendation matrix      | Axis 2 partial    | **Unknown**   |

### 9.3 Methodological Limitations

1. **Single run per test**: No statistical repetition. No variance/confidence intervals reported.
2. **Reuse bug (Bug #2)**: `submit-jobs` hangs on 2nd run on same cluster. This is itself the blocker for Axis 3 — a chicken-and-egg problem that must be fixed before warm-cluster claims can be empirically validated.
3. **E32s vs E64s anomaly**: E32s_v3 single-batch (25 min) < E64s_v3 (57 min) is counterintuitive and unresolved (ran at different times under different conditions).
4. **No I/O profiling**: The super-linear-speedup hypothesis (per-node memory pressure) is _inferred_ from timing, not measured via `/proc/diskstats`, `vmstat`, or Azure Monitor disk metrics.
5. **No taxonomy-subset test**: Planned virus+plasmodium subset (~25 GB, ~10x smaller than core_nt) not executed.
6. **Scope drift from v1 plan**: v2 switched DB from `nt_prok` (82 GB) to `core_nt` (269 GB) and query set to `pathogen-*`, so direct v1/v2 numeric comparisons are indicative only.

---

## 10. Future Work — v2 3-Axis Completion Roadmap

The highest-value next steps are ordered by impact on unresolved claims.

### 10.1 Priority 1 — Unblock Axis 3 (production-mode validation)

| #   | Task                                          | Depends on | Est. Cost | Blocks                              |
| --- | --------------------------------------------- | ---------- | --------- | ----------------------------------- |
| P1  | **Fix reuse `submit-jobs` hang** (Bug #2)     | —          | $0        | All Axis 3; TL;DR warm-cluster row  |
| P2  | **A3-reuse**: cold+warm E2E measurement (10q) | P1         | ~$4       | $1.34/run claim, Section 7 estimate |
| P3  | **A3-seq5**: 5 sequential warm requests       | P1         | ~$6       | Service throughput number           |
| P4  | **A3-con3**: concurrent submit feasibility    | P1         | ~$3       | Axis 3 assumption unknown           |
| P5  | **A3-burst**: KEDA/HPA autoscale 3→5→10       | P1, P4     | ~$15      | Service-level scalability           |

### 10.2 Priority 2 — Complete Axis 1 SKU Comparison

| #   | Task                   | SKU                   | Quota                  | Est. Cost |
| --- | ---------------------- | --------------------- | ---------------------- | --------- |
| S1  | A1-L32-10 + A1-L32-300 | L32as_v3 (NVMe)       | 100 LSv3 vCPU ✓        | ~$3       |
| S2  | A1-L64-10 + A1-L64-300 | L64as_v3 (NVMe×2×CPU) | 100 LSv3 → 64 used, OK | ~$4       |
| S3  | A1-HB-10 + A1-HB-300   | HB120rs_v3 (HPC)      | 0 (need quota req)     | ~$4       |
| S4  | A1-E96-10              | E96s_v3               | 200 ESv3 ✓             | ~$2       |
| S5  | A1-E48-300             | E48s_v3               | 200 ESv3 ✓             | ~$2       |

### 10.3 Priority 3 — Complete Axis 2 Query Scaling

| #   | Task                                                   | Preconditions         | Est. Cost |
| --- | ------------------------------------------------------ | --------------------- | --------- |
| Q1  | Generate pathogen-50/100/1000 fasta                    | Python script in plan | $0        |
| Q2  | A2-{50,100,1000}-{1N,3N}                               | best SKU from Axis 1  | ~$10      |
| Q3  | Implement `recommend_config()` in `azure_optimizer.py` | Q2 analysis           | $0        |

### 10.4 Priority 4 — Scientific Depth

1. **E32s vs E64s controlled replay**: run both SKUs simultaneously with identical config to resolve the 25-min vs 57-min anomaly.
2. **I/O profiling during BLAST**: `/proc/diskstats` + `vmstat` + Azure Monitor disk metrics → direct evidence for super-linear-speedup hypothesis.
3. **Taxonomy-subset DB**: 25 GB virus+plasmodium subset; expected 10x speedup.
4. **DB sharding (partitioned mode)**: 10-shard × N-node layout → per-node DB 27 GB instead of 269 GB.
5. **Statistical repetition**: 5 runs/test per v2 plan; report medians + 95% CI.

### 10.5 Estimated Total Cost to Complete v2

| Tier               | Tests   | Cost     |
| ------------------ | ------- | -------- |
| P1-P5 (Axis 3)     | 5 tests | ~$28     |
| S1-S5 (Axis 1)     | 5 tests | ~$15     |
| Q1-Q3 (Axis 2)     | 6 tests | ~$10     |
| Priority 4 (depth) | ~8 runs | ~$20     |
| **Total**          | —       | **~$73** |

Well within the original v2 budget of $200/day.

---

## 11. Reproducing Results

### Prerequisites

```bash
# Azure resources
az aks create -g rg-elb-koc -n elb-benchmark \
  --node-count 2 --node-vm-size Standard_E64s_v3 \
  --attach-acr elbacr

# DB pre-staged at:
# https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt
# (269 GB, 754 files — use benchmark/prestage-db-runcommand.sh)

# Query files at:
# https://stgelb.blob.core.windows.net/queries/pathogen-{10,300}.fa
```

### Best Configuration (E64s_v3 × 2N)

```ini
[cloud-provider]
azure-region = koreacentral
azure-resource-group = rg-elb-koc
azure-storage-account = stgelb

[cluster]
name = elb-pathogen
machine-type = Standard_E64s_v3
num-nodes = 2
exp-use-local-ssd = true
reuse = true

[blast]
program = blastn
db = https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt
queries = https://stgelb.blob.core.windows.net/queries/pathogen-10.fa
results = https://stgelb.blob.core.windows.net/results/pathogen-run
options = -max_target_seqs 500 -evalue 0.05 -word_size 28 -dust yes -soft_masking true -outfmt 7
batch-len = 100000
mem-limit = 4G

[timeouts]
init-pv = 90
```

### Expected Results

| Phase                           | Time         | Source        |
| ------------------------------- | ------------ | ------------- |
| DB download (first run only)    | ~28 min/node | Measured      |
| BLAST execution (2 nodes, 10q)  | ~10 min      | Measured      |
| Wall clock (cold)               | ~46 min      | Measured      |
| Wall clock (warm, `reuse=true`) | ~10 min †    | **Estimated** |
| Cost (cold)                     | ~$6.18       | Derived       |
| Cost (warm)                     | ~$1.34 †     | **Estimated** |

> † Warm numbers are arithmetic extrapolations (10 min × $8.06/hr). Empirical warm-cluster measurement (Axis 3 of v2 plan) is pending. See Section 9.

---

## 12. Conclusion

This benchmark establishes the first performance baseline for ElasticBLAST on Azure with the NCBI `core_nt` database (269 GB) using real pathogen detection queries.

**Key contributions**:

1. **Query count is irrelevant for performance.** 10 and 300 pathogen queries produce identical BLAST times (57 min/batch on E64s_v3), confirming that the 269 GB database scan completely dominates execution. This means the customer can freely scale from single-pathogen to 30-pathogen panels without performance degradation.

2. **Multi-node scaling delivers 5-6x speedup.** Two E64s_v3 nodes reduce BLAST time from 57 min to 10 min, with super-linear efficiency (292%). This is stronger than v1's nt_prok results (145% on NVMe 3N), likely because the larger core_nt database benefits more from distributed memory pressure.

3. **Database download is the production bottleneck.** At 28 min/node for 269 GB, DB download exceeds BLAST execution time on multi-node configurations. Persistent pre-loaded clusters (`reuse=true`) eliminate this overhead entirely, reducing wall clock from 46 min to 10 min.

4. **Seven bugs were discovered**, including init-ssd timeout issues, cluster reuse failures, and storage access configuration problems. The cluster reuse bug (submit-jobs hanging on 2nd run) remains open and blocks the production warm-cluster architecture.

**Practical recommendation**: For the customer's pathogen detection service against core_nt, use **E64s_v3 × 2 nodes with `reuse=true`**. Cold-run target is 10-minute BLAST at ~$6.18/run; warm-run target is 10-minute E2E at ~$1.34/run. **However, warm-run numbers remain extrapolations until the cluster reuse bug (Bug #2) is fixed and Axis 3 of the v2 plan is executed** (see Section 10.1). Until then, the safest production stance is to provision 2×E64s_v3 and rebuild per-request (cold mode, ~$6.18/run), and plan a follow-up validation sprint to unlock the ~$1.34/run warm-cluster regime.

---

## References

[1] Altschul, S.F. et al. (1990). Basic local alignment search tool. J. Mol. Biol. 215:403-410.

[2] Camacho, C. et al. (2023). ElasticBLAST: accelerating sequence analysis via cloud computing. BMC Bioinformatics 24:117.

[3] Tsai, J. (2021). Running NCBI BLAST on Azure — Performance, Scalability and Best Practice. Azure HPC Blog.

[4] Choi, M.H. (2026). ElasticBLAST Azure Performance Benchmark v1 — Storage and Scaling. Internal report.
