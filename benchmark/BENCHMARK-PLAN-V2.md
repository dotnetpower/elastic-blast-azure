# ElasticBLAST Azure Benchmark Plan v2 — Production Readiness

> **Created**: 2026-04-20
> **Last updated**: 2026-04-22
> **Author**: Moon Hyuk Choi (moonchoi@microsoft.com)
> **Budget**: $200/day max
> **Region**: Korea Central
> **Baseline**: [Benchmark v1 Report](results/2026-04-18/report.md) (storage + scaling, completed 2026-04-18)
> **Customer context**: Pathogen detection service — 3 pathogens (SARS-CoV-2, MPXV, P. falciparum), 1-300 queries/request, multi-user

---

## 0. Execution Status (2026-04-22)

> This section tracks real progress against the plan. See [results/v2/report.md §9](results/v2/report.md#9-limitations) for full scope reconciliation.

### Axis Coverage

| Axis                              | Planned | Executed | Coverage | Status       |
| --------------------------------- | ------- | -------- | -------- | ------------ |
| **Axis 1: SKU Scale-Up**          | 11      | 3        | **27%**  | Partial      |
| **Axis 2: Query-Based Tuning**    | 11      | 4        | **36%**  | Partial      |
| **Axis 3: Multi-Request Service** | 6       | **0**    | **0%**   | **Not done** |

### Executed Tests

| Test ID    | Axis | VM      | Nodes | Queries | BLAST Time | Date       | Result    |
| ---------- | ---- | ------- | ----- | ------- | ---------- | ---------- | --------- |
| A1-E32-10  | 1    | E32s_v3 | 1     | 10      | 25.1 min   | 2026-04-20 | PASS      |
| A1-E64-10  | 1    | E64s_v3 | 1     | 10      | 57.3 min   | 2026-04-20 | PASS      |
| A2-E64-300 | 2    | E64s_v3 | 1     | 300     | 57.3 min   | 2026-04-20 | PASS      |
| C1-E64-3N  | 2    | E64s_v3 | 3     | 10      | 10.9 min   | 2026-04-20 | PASS      |
| C1-E64-2N  | 2    | E64s_v3 | 2     | 10      | 9.8 min    | 2026-04-20 | PASS      |
| D1-E48-3N  | 1    | E48s_v3 | 3     | 10      | 10.9 min   | 2026-04-21 | PASS      |
| C2-E64-\*  | 2    | E64s_v3 | 2,3   | 300     | —          | 2026-04-20 | FAIL (#2) |
| C3-E64-\*  | 2    | E64s_v3 | 2,3   | 1000    | —          | 2026-04-20 | FAIL (#2) |

### Critical Gaps (Axis 3 — the v2 _raison d'être_)

| Test ID  | Purpose                                     | Blocker                |
| -------- | ------------------------------------------- | ---------------------- |
| A3-reuse | warm-cluster E2E, validates $1.34/run claim | Bug #2 (reuse hang)    |
| A3-seq5  | sustained throughput                        | Bug #2                 |
| A3-con3  | concurrent submit feasibility               | Unverified + Bug #2    |
| A3-con10 | max concurrent load                         | Bug #2                 |
| A3-burst | autoscale 3→5→10 node reaction              | Bug #2 + no KEDA setup |

**Bug #2 — reuse hang**: `submit-jobs` pod hangs on the 2nd `elastic-blast submit` to a cluster where init-ssd already completed. This is the single blocker for all Axis 3 tests. **Until fixed, the warm-cluster production mode is an unvalidated extrapolation.**

### Axis 1 Gaps (SKU comparison)

| SKU        | Status  | Reason                                                 |
| ---------- | ------- | ------------------------------------------------------ |
| L32as_v3   | Not run | LSv3 quota 100 vCPU available ✓, needs new cluster     |
| L64as_v3   | Not run | LSv3 quota 100 vCPU (exactly fits 1×64), needs cluster |
| HB120rs_v3 | Not run | HBrsv3 quota = 0 (SKU request required)                |
| E96s_v3    | Not run | ESv3 quota OK, not prioritized                         |
| E48s-300q  | Not run | D1 covered 10q only                                    |

### Repetition / Statistical Confidence

Plan called for **5 runs per test**. Every executed test ran **1 time only**. No variance or confidence intervals can be reported.

---

## 1. Background and Motivation

### v1 Summary (Completed)

Benchmark v1 established:

- **Local SSD is 2.8x faster** than Blob NFS (82 GB DB, storage I/O path bottleneck)
- **3-node scaling** achieves 2.3-4.4x speedup at $0.70/run (best value)
- **mem-limit=4G** is 19% faster than default 254G
- 7 production bugs discovered and fixed

### Why v2?

A customer requested ElasticBLAST for a **pathogen detection service** against `core_nt` (~300 GB). Their workload differs from v1:

| Dimension           | v1 (completed)     | v2 (customer scenario)             |
| ------------------- | ------------------ | ---------------------------------- |
| Database            | nt_prok (82 GB)    | **core_nt (~300 GB)**              |
| Query scale         | 3,054 seqs (fixed) | **10-300 seqs/request** (variable) |
| Usage model         | Single run         | **Multi-user service**             |
| Latency requirement | None               | **Near real-time**                 |

### Research Questions

| ID  | Question                                                                  | Success Criteria                                   |
| --- | ------------------------------------------------------------------------- | -------------------------------------------------- |
| RQ1 | Which VM SKU provides the best single-node BLAST performance for core_nt? | Identify optimal $/performance SKU                 |
| RQ2 | How does query count (10-300) affect optimal node/pod configuration?      | Produce query-size → config mapping                |
| RQ3 | Can ElasticBLAST serve near real-time multi-user workloads?               | E2E latency < 5 min for 10 queries on warm cluster |

---

## 2. Prerequisites

### 2.1 core_nt Database Pre-staging

```bash
# Estimate: ~300 GB, ~30 min transfer at 800 MB/s
./benchmark/prestage-db.sh core_nt

# Verify
AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy list \
  "https://stgelb.blob.core.windows.net/blast-db/core_nt/" --properties Size \
  | tail -5
```

**Blocking**: All v2 tests depend on core_nt being available in Azure Blob. Start this first.

### 2.2 Query Preparation

Customer provided 10 NCBI RefSeq sequences (3 pathogens). All are public data:

| Pathogen        | Accession   | Gene     | Bases      |
| --------------- | ----------- | -------- | ---------- |
| SARS-CoV-2      | NC_045512.2 | orf1ab   | 21,290     |
| SARS-CoV-2      | NC_045512.2 | RdRP     | 2,795      |
| SARS-CoV-2      | NC_045512.2 | N        | 1,260      |
| Monkeypox virus | NC_003310.1 | F3L      | 462        |
| Monkeypox virus | NC_063383.1 | F3L      | 462        |
| P. falciparum   | NC_004325.2 | 18S rRNA | 2,149      |
| P. falciparum   | NC_004326.2 | 18S rRNA | 2,092      |
| P. falciparum   | NC_004328.3 | 18S rRNA | 2,505      |
| P. falciparum   | NC_004331.3 | 18S rRNA | 2,151      |
| P. falciparum   | NC_037282.1 | 18S rRNA | 2,580      |
| **Total**       |             |          | **37,746** |

Source files: `benchmark/private/260420_elastic_blast_test_fasta_file_10ea/`

#### Query Sets to Prepare

| Query Set          | Seqs  | Bases   | How                                   | Purpose            |
| ------------------ | ----- | ------- | ------------------------------------- | ------------------ |
| `pathogen-10.fa`   | 10    | 37 KB   | Merge customer 10 files               | Baseline (1 batch) |
| `pathogen-50.fa`   | 50    | ~150 KB | 5x duplicate with mutated headers     | ~2 batches         |
| `pathogen-100.fa`  | 100   | ~300 KB | 10x duplicate                         | ~3 batches         |
| `pathogen-300.fa`  | 300   | ~900 KB | 30x duplicate (simulates max request) | ~9 batches         |
| `pathogen-1000.fa` | 1,000 | ~3 MB   | 100x duplicate                        | ~30 batches        |
| `gut-3054.fa`      | 3,054 | ~3.1 MB | v1 query (for comparison)             | ~31 batches        |

```bash
# Generate merged query from customer files
cat benchmark/private/260420_elastic_blast_test_fasta_file_10ea/*.fa \
    benchmark/private/260420_elastic_blast_test_fasta_file_10ea/*.fasta \
    > benchmark/queries/pathogen-10.fa

# Generate scaled query sets (add sequence index to headers to avoid duplicates)
python3 -c "
from Bio import SeqIO
import sys
seqs = list(SeqIO.parse('benchmark/queries/pathogen-10.fa', 'fasta'))
for target in [50, 100, 300, 1000]:
    with open(f'benchmark/queries/pathogen-{target}.fa', 'w') as f:
        for i in range(target):
            s = seqs[i % len(seqs)]
            f.write(f'>{s.id}_rep{i//len(seqs)}\n{str(s.seq)}\n')
    print(f'pathogen-{target}.fa: {target} seqs')
"
```

### 2.3 Upload Queries to Blob

```bash
for f in benchmark/queries/pathogen-*.fa; do
    AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy cp "$f" \
        "https://stgelb.blob.core.windows.net/queries/$(basename $f)"
done
```

---

## 3. Test Axes

### Overview

```
Axis 1: SKU Scale-Up ──────── "Which VM is the fastest?"
    │
    ▼ (Optimal SKU determined)
Axis 2: Query-Based Tuning ── "What is the optimal config per query scale?"
    │
    ▼ (Recommendation mapping finalized)
Axis 3: Multi-Request Service ─ "Can it handle multiple users concurrently?"
```

---

## 4. Axis 1: SKU Scale-Up — Identifying the Optimal Single-Node VM

### 4.1 Objective

Identify the **VM SKU with the best single-node performance** for core_nt (~300 GB).
Based on the v1 conclusion (Local SSD > NFS), only Local SSD mode is tested.

### 4.2 VM Candidates

| SKU                 | vCPU | RAM    | Temp Disk   | $/hr   | Selection Reason                     |
| ------------------- | ---- | ------ | ----------- | ------ | ------------------------------------ |
| Standard_E32s_v3    | 32   | 256 GB | 512 GB      | $2.016 | v1 baseline, **temp disk < core_nt** |
| Standard_E48s_v3    | 48   | 384 GB | 768 GB      | $3.024 | CPU 1.5x, temp disk fits?            |
| Standard_E64s_v3    | 64   | 432 GB | 1,024 GB    | $4.032 | CPU 2x, **core_nt fits easily**      |
| Standard_E96s_v3    | 96   | 672 GB | 1,344 GB    | $6.048 | CPU 3x, top E-series                 |
| Standard_L32as_v3   | 32   | 256 GB | 3.8 TB NVMe | $2.496 | v1 NVMe baseline                     |
| Standard_L64as_v3   | 64   | 512 GB | 7.6 TB NVMe | $4.992 | NVMe + CPU 2x                        |
| Standard_HB120rs_v3 | 120  | 480 GB | 2×960 GB    | $3.600 | HPC, Tsai 2021 comparison            |

> **Important**: E32s_v3 temp disk = 512 GB while core_nt ≈ 300 GB. It fits but with little margin.
> Considering space for queries/results, E48s_v3+ or L-series is safer.

### 4.3 Pre-check: DB Size Verification

The exact size of core_nt must be verified first. Check volume count before downloading from NCBI:

```bash
# Check core_nt volume list on S3
aws s3 ls s3://ncbi-blast-databases/ --no-sign-request | grep "core_nt\." | head -20

# Or check on NCBI FTP
curl -s https://ftp.ncbi.nlm.nih.gov/blast/db/ | grep "core_nt\." | head -20
```

If core_nt > 500 GB, exclude E32s_v3 and test only E64s_v3+ / L-series.

### 4.4 Test Matrix

**DB**: core_nt, Local SSD mode (`exp-use-local-ssd = true`)
**Queries**: pathogen-10.fa (1 batch) + pathogen-300.fa (9 batches)

| Test ID    | SKU        | Nodes | Query        | batches | Measurement Goal          |
| ---------- | ---------- | ----- | ------------ | ------- | ------------------------- |
| A1-E32-10  | E32s_v3    | 1     | pathogen-10  | 1       | E32 baseline (if DB fits) |
| A1-E48-10  | E48s_v3    | 1     | pathogen-10  | 1       | CPU 1.5x effect           |
| A1-E64-10  | E64s_v3    | 1     | pathogen-10  | 1       | CPU 2x effect             |
| A1-E96-10  | E96s_v3    | 1     | pathogen-10  | 1       | CPU 3x effect             |
| A1-L32-10  | L32as_v3   | 1     | pathogen-10  | 1       | NVMe baseline             |
| A1-L64-10  | L64as_v3   | 1     | pathogen-10  | 1       | NVMe + CPU 2x             |
| A1-HB-10   | HB120rs_v3 | 1     | pathogen-10  | 1       | HPC (Tsai comparison)     |
| A1-E32-300 | E32s_v3    | 1     | pathogen-300 | 9       | multi-batch single node   |
| A1-E64-300 | E64s_v3    | 1     | pathogen-300 | 9       | multi-batch scale-up      |
| A1-L32-300 | L32as_v3   | 1     | pathogen-300 | 9       | multi-batch NVMe          |
| A1-HB-300  | HB120rs_v3 | 1     | pathogen-300 | 9       | multi-batch HPC           |

**Total**: 11 tests

### 4.5 Measurement

| Metric                    | Method                                    |
| ------------------------- | ----------------------------------------- |
| DB download time          | `azcopy` job start → completion timestamp |
| BLAST execution per batch | K8s job startTime → completionTime        |
| Total wall clock          | submit → last job complete                |
| CPU utilization           | `kubectl top nodes` (snapshot every 30s)  |
| Cost                      | nodes × duration × $/hr                   |

### 4.6 Expected Output

1. **SKU Performance Chart**: bar chart — median per-batch time per SKU
2. **SKU Cost-Efficiency Chart**: scatter — cost/run vs median time
3. **CPU Scaling Chart**: line chart — vCPU count vs speedup (vs linear)
4. **Optimal SKU recommendation table**

### 4.7 Cost Estimate

| Tests            | Avg duration | Avg $/hr | Est. cost |
| ---------------- | ------------ | -------- | --------- |
| 7 × 10-query     | ~15 min each | ~$3.50   | $6.13     |
| 4 × 300-query    | ~30 min each | ~$3.50   | $7.00     |
| Cluster overhead | ~2 hr total  | ~$3.00   | $6.00     |
| **Axis 1 total** |              |          | **~$20**  |

### 4.8 Execution Procedure

```bash
# Step 1: Verify core_nt is pre-staged
AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy list \
  "https://stgelb.blob.core.windows.net/blast-db/core_nt/" \
  --properties Size | tail -3

# Step 2: Check SKU availability in Korea Central
az vm list-skus --location koreacentral --resource-type virtualMachines \
  --query "[?name=='Standard_E64s_v3' || name=='Standard_E96s_v3' || name=='Standard_HB120rs_v3' || name=='Standard_L64as_v3'].{Name:name, Available:restrictions}" \
  -o table

# Step 3: Run Axis 1 tests sequentially (share cluster where possible)
# E-series tests: create 1 cluster, scale node pool across SKUs
# L-series tests: separate cluster (different VM family)
# HB-series tests: separate cluster

# Step 4: Collect results
kubectl get jobs -l app=blast -o json > results/v2/axis1-<test-id>.json
```

### 4.9 INI Template

```ini
# benchmark/configs/v2/axis1-template.ini
[cloud-provider]
azure-region = koreacentral
azure-acr-resource-group = rg-elbacr
azure-acr-name = elbacr
azure-resource-group = rg-elb-koc
azure-storage-account = stgelb
azure-storage-account-container = blast-db

[cluster]
name = elb-v2-axis1
machine-type = ${SKU}           # Variable: E32s_v3, E64s_v3, etc.
num-nodes = 1
exp-use-local-ssd = true

[blast]
program = blastn
db = https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt
queries = https://stgelb.blob.core.windows.net/queries/${QUERY_FILE}
results = https://stgelb.blob.core.windows.net/results/v2/${TEST_ID}
options = -max_target_seqs 500 -evalue 0.05 -word_size 28 -dust yes -soft_masking true -outfmt 7
batch-len = 100000
mem-limit = 4G
```

---

## 5. Axis 2: Query-Based Tuning — Optimal Config per Query Scale

### 5.1 Objective

Derive the optimal combination of **(SKU, node count, batch-len, mem-limit)** based on query count (10-3,000),
and implement auto-recommendation logic in `azure_optimizer.py`.

### 5.2 Dependencies

- Proceed after **2-3 optimal SKUs** are determined from Axis 1
- Assumption: `E64s_v3` (or similar) is selected from Axis 1 results

### 5.3 Test Matrix

**DB**: core_nt, Local SSD mode
**SKU**: Axis 1 optimal SKU (e.g., E64s_v3) + v1 baseline (E32s_v3)

| Test ID    | Query         | Bases  | batches | SKU      | Nodes | Purpose                                 |
| ---------- | ------------- | ------ | ------- | -------- | ----- | --------------------------------------- |
| A2-10-1N   | pathogen-10   | 37 KB  | 1       | best_sku | 1     | Min query baseline                      |
| A2-50-1N   | pathogen-50   | 150 KB | 2       | best_sku | 1     | Small scale                             |
| A2-100-1N  | pathogen-100  | 300 KB | 3       | best_sku | 1     | Medium scale, 1N                        |
| A2-100-3N  | pathogen-100  | 300 KB | 3       | best_sku | 3     | Medium scale, 3N (distribution effect?) |
| A2-300-1N  | pathogen-300  | 900 KB | 9       | best_sku | 1     | Max single request, 1N                  |
| A2-300-3N  | pathogen-300  | 900 KB | 9       | best_sku | 3     | Max single request, 3N                  |
| A2-300-5N  | pathogen-300  | 900 KB | 9       | best_sku | 5     | Max single request, 5N                  |
| A2-1000-1N | pathogen-1000 | 3 MB   | 30      | best_sku | 1     | Multi-user simulation                   |
| A2-1000-3N | pathogen-1000 | 3 MB   | 30      | best_sku | 3     | Multi-user, 3N                          |
| A2-1000-5N | pathogen-1000 | 3 MB   | 30      | best_sku | 5     | Multi-user, 5N                          |
| A2-3054-3N | gut-3054      | 3.1 MB | 31      | best_sku | 3     | v1 comparison (DB only)                 |

**Total**: 11 tests

### 5.4 Measurement

Same as v1 + additions:

| Metric             | Method                                            |
| ------------------ | ------------------------------------------------- |
| Batch count        | ElasticBLAST submit log                           |
| Pod scheduling lag | pod creationTimestamp → startTime                 |
| Idle node time     | (batch count < node × pod/node) some nodes unused |

### 5.5 Expected Output

1. **Query-Scale Heatmap**: (query count × node count) → wall-clock time
2. **Cost-Efficiency Table**: cost/run for each combination
3. **Recommendation Matrix**:

```
| Queries | Rec. Nodes | Rec. SKU | Est. Time | Est. Cost |
|---------|----------|---------|----------|----------|
| 1-50    | 1        | E64s    | X min    | $Y       |
| 50-100  | 1-3      | E64s    | X min    | $Y       |
| 100-300 | 3        | E64s    | X min    | $Y       |
| 300+    | 3-5      | E64s    | X min    | $Y       |
```

4. **`azure_optimizer.py` code**: implement `recommend_config()` function

### 5.6 Cost Estimate

| Tests             | Avg duration | Avg $/hr | Est. cost |
| ----------------- | ------------ | -------- | --------- |
| 6 × single-node   | ~20 min      | ~$4.00   | $8.00     |
| 4 × multi-node    | ~15 min      | ~$12.00  | $12.00    |
| 1 × v1-comparison | ~10 min      | ~$12.00  | $2.00     |
| Cluster overhead  | ~1.5 hr      | ~$4.00   | $6.00     |
| **Axis 2 total**  |              |          | **~$28**  |

---

## 6. Axis 3: Near Real-Time Multi-Request Service

### 6.1 Objective

Validate the **service scenario** where multiple users send BLAST requests concurrently.
Key: **Queue-based multi-request processing** + **auto-scaling** on a persistent `reuse = true` cluster.

### 6.2 Dependencies

- Axis 1, 2 complete → optimal SKU and node config finalized
- `reuse = true` functionality verified
- Queue-integrated Worker implementation (new development)

### 6.3 Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ User A      │     │ Azure Queue      │     │ AKS Cluster         │
│ User B      │────▶│ Storage          │────▶│ (reuse=true, warm)  │
│ User C      │     │                  │     │                     │
└─────────────┘     │ Message format:  │     │ Worker Pod:         │
                    │ {                │     │  1. Dequeue message  │
                    │   "query_url":   │     │  2. elastic-blast    │
                    │     "blob://...",│     │     submit           │
                    │   "options":     │     │  3. Wait completion  │
                    │     "-evalue..", │     │  4. Notify result    │
                    │   "callback_url":│     │                     │
                    │     "https://..."│     │ Auto-scale:          │
                    │ }                │     │  3 → 5 → 10 nodes   │
                    └──────────────────┘     └─────────────────────┘
```

### 6.4 Implementation Steps

#### Step 1: `reuse = true` validation (existing feature)

```bash
# First submit — cluster creation (5-15 min)
elastic-blast submit --cfg bench-v2-reuse.ini

# Second submit — cluster reuse (starts immediately)
elastic-blast submit --cfg bench-v2-reuse-2nd.ini

# Measure: first submit E2E vs second submit E2E
```

#### Step 2: Sequential multi-request script

```bash
# benchmark/run_sequential_requests.sh
#!/bin/bash
for i in $(seq 1 $NUM_REQUESTS); do
    echo "[$(date)] Request $i start"
    elastic-blast submit --cfg "configs/v2/request-${i}.ini"
    elastic-blast status --cfg "configs/v2/request-${i}.ini" --wait
    echo "[$(date)] Request $i complete"
done
```

#### Step 3: Concurrent multi-request (parallel submit)

```bash
# benchmark/run_concurrent_requests.sh
#!/bin/bash
for i in $(seq 1 $NUM_REQUESTS); do
    (
        elastic-blast submit --cfg "configs/v2/request-${i}.ini"
        elastic-blast status --cfg "configs/v2/request-${i}.ini" --wait
    ) &
done
wait
```

> **Known limitation**: ElasticBLAST may not support concurrent submit on the same cluster.
> In that case, Job namespace isolation or queue-based sequential processing is required.

#### Step 4: Queue Worker (new development — if needed)

```python
# src/elastic_blast/queue_worker.py (future implementation)
# Polls messages from Azure Queue Storage and
# sequentially executes elastic-blast submit
```

### 6.5 Test Matrix

| Test ID     | Scenario         | Requests | Queries/req | Concurrency | Cluster               | Measurement       |
| ----------- | ---------------- | -------- | ----------- | ----------- | --------------------- | ----------------- |
| A3-reuse    | reuse validation | 2 seq    | 10          | 1           | new→reuse             | Reuse E2E latency |
| A3-seq5     | seq 5 requests   | 5 seq    | 10          | 1           | persistent            | Avg E2E           |
| A3-seq5-300 | seq 5 × 300q     | 5 seq    | 300         | 1           | persistent            | throughput        |
| A3-con3     | concurrent 3 req | 3 conc   | 100         | 3           | persistent, 3N        | Interference      |
| A3-con10    | concurrent 10    | 10 conc  | 100         | 10          | persistent, 5N        | Max load          |
| A3-burst    | Burst load       | 20/min   | 10          | burst       | persistent, autoscale | Scale response    |

**Total**: 6 tests

### 6.6 Key Metrics

| Metric                                  | Target               | Method                               |
| --------------------------------------- | -------------------- | ------------------------------------ |
| E2E Latency (10 queries, warm cluster)  | **< 5 min**          | submit → results available           |
| E2E Latency (300 queries, warm cluster) | **< 15 min**         | submit → results available           |
| Throughput (sustained)                  | ≥ 5 req/hr           | Sequential requests, 10 queries each |
| Concurrent requests                     | ≥ 3 without failure  | Parallel submit                      |
| Auto-scale response                     | < 5 min to add nodes | KEDA or HPA trigger time             |
| Idle cost                               | < $2/hr (1 node min) | Min node pool during low traffic     |

### 6.7 Cost Estimate

| Tests            | Duration | Nodes | Avg $/hr | Est. cost |
| ---------------- | -------- | ----- | -------- | --------- |
| A3-reuse         | 30 min   | 1-3   | $6.00    | $3.00     |
| A3-seq5          | 1 hr     | 3     | $6.05    | $6.05     |
| A3-seq5-300      | 2 hr     | 3     | $6.05    | $12.10    |
| A3-con3          | 30 min   | 3     | $6.05    | $3.03     |
| A3-con10         | 1 hr     | 5     | $10.08   | $10.08    |
| A3-burst         | 1 hr     | 3-10  | $15.00   | $15.00    |
| Cluster idle     | 2 hr     | 3     | $6.05    | $12.10    |
| **Axis 3 total** |          |       |          | **~$61**  |

---

## 7. Execution Schedule

### Day 0: Preparation (2-3 hours)

| #   | Task                      | Command                              | Est. Time |
| --- | ------------------------- | ------------------------------------ | --------- |
| 0.1 | core_nt DB pre-stage      | `./benchmark/prestage-db.sh core_nt` | 30-60 min |
| 0.2 | Verify DB size            | `azcopy list`                        | 5 min     |
| 0.3 | Generate query sets       | Python script (Section 2.2)          | 5 min     |
| 0.4 | Upload queries            | `azcopy cp`                          | 5 min     |
| 0.5 | Check SKU availability    | `az vm list-skus`                    | 5 min     |
| 0.6 | Create INI configs        | Generate from template               | 15 min    |
| 0.7 | Verify ElasticBLAST build | `pytest tests/azure/ -v`             | 10 min    |

### Day 1: Axis 1 — SKU Scale-Up (6-8 hours)

| #   | Task                         | Tests                        | Cluster Strategy          | Est. Time |
| --- | ---------------------------- | ---------------------------- | ------------------------- | --------- |
| 1.1 | E-series single batch (10q)  | A1-E32-10, E48, E64, E96     | 1 cluster, node pool swap | 2 hr      |
| 1.2 | L-series single batch (10q)  | A1-L32-10, L64               | Separate cluster          | 1 hr      |
| 1.3 | HB-series single batch (10q) | A1-HB-10                     | Separate cluster          | 30 min    |
| 1.4 | Multi-batch (300q)           | A1-E32/E64/L32/HB-300        | Reuse clusters            | 2 hr      |
| 1.5 | Data collection + cleanup    | Export JSON, delete clusters | 30 min                    |
| 1.6 | Preliminary analysis         | Charts, SKU ranking          | 1 hr                      |

**Gate**: Select 2-3 optimal SKUs from Axis 1 results → proceed to Axis 2

### Day 2: Axis 2 — Query-Based Tuning (4-6 hours)

| #   | Task                      | Tests                              | Est. Time |
| --- | ------------------------- | ---------------------------------- | --------- |
| 2.1 | Single-node query scaling | A2-10/50/100/300/1000-1N           | 2 hr      |
| 2.2 | Multi-node query scaling  | A2-100/300/1000-3N, A2-300/1000-5N | 2 hr      |
| 2.3 | v1 comparison (core_nt)   | A2-3054-3N                         | 30 min    |
| 2.4 | Data analysis             | Heatmap, recommendation table      | 1 hr      |

**Gate**: Recommendation matrix complete → proceed to Axis 3

### Day 3-4: Axis 3 — Multi-Request Service (6-10 hours)

| #   | Task                     | Tests                      | Est. Time |
| --- | ------------------------ | -------------------------- | --------- |
| 3.1 | `reuse=true` validation  | A3-reuse                   | 30 min    |
| 3.2 | Sequential multi-request | A3-seq5, A3-seq5-300       | 3 hr      |
| 3.3 | Concurrent multi-request | A3-con3, A3-con10          | 2 hr      |
| 3.4 | Burst/autoscale test     | A3-burst                   | 1.5 hr    |
| 3.5 | Data analysis            | Latency charts, throughput | 1 hr      |

### Day 5: Report (3-4 hours)

| #   | Task                                                | Est. Time |
| --- | --------------------------------------------------- | --------- |
| 5.1 | v2 report draft                                     | 2 hr      |
| 5.2 | Charts generation (`create_charts_v2.py` extension) | 1 hr      |
| 5.3 | Customer-facing summary (1-pager)                   | 30 min    |
| 5.4 | `azure_optimizer.py` code update                    | 30 min    |

---

## 8. Budget Summary

| Axis       | Tests        | Est. Cost | Notes                          |
| ---------- | ------------ | --------- | ------------------------------ |
| Prep       | DB transfer  | ~$1       | S3→Blob, storage cost          |
| **Axis 1** | 11           | **~$20**  | 7 SKUs, 2 query sets           |
| **Axis 2** | 11           | **~$28**  | 6 query scales, 3 node configs |
| **Axis 3** | 6            | **~$61**  | Sustained cluster time         |
| Report     | —            | $0        | Analysis only                  |
| **Total**  | **28 tests** | **~$110** | Within $200/day budget         |

---

## 9. Risk and Mitigation

| Risk                                        | Impact            | Mitigation                                      |
| ------------------------------------------- | ----------------- | ----------------------------------------------- |
| core_nt > E32s_v3 temp disk (512 GB)        | E32 tests fail    | Skip E32, start from E48/E64                    |
| HB120rs_v3 not available in Korea Central   | 1 test skipped    | Use East US 2 or skip HPC comparison            |
| L64as_v3 quota insufficient                 | 1 test skipped    | Use L32as_v3 only                               |
| Concurrent submit conflicts on same cluster | A3-con tests fail | Use Job namespace isolation or sequential queue |
| core_nt download takes >1 hour              | Day 0 delayed     | Start prestage overnight before Day 1           |
| Budget overrun                              | —                 | Stop after Axis 1 if >$80 spent                 |

---

## 10. Deliverables

| #   | Deliverable                 | Location                                   |
| --- | --------------------------- | ------------------------------------------ |
| 1   | v2 Benchmark Report         | `benchmark/results/v2/report.md`           |
| 2   | Raw data (JSON)             | `benchmark/results/v2/data/`               |
| 3   | Charts (PNG + PDF)          | `benchmark/results/v2/charts/`             |
| 4   | SKU Recommendation Table    | In report, Section 2                       |
| 5   | Query→Config Mapping Table  | In report, Section 3                       |
| 6   | `azure_optimizer.py` update | `src/elastic_blast/azure_optimizer.py`     |
| 7   | INI configs for all tests   | `benchmark/configs/v2/`                    |
| 8   | Customer 1-pager Summary    | `benchmark/results/v2/customer-summary.md` |

---

## 11. Success Criteria

| Criterion                          | Target                                       |
| ---------------------------------- | -------------------------------------------- |
| Optimal SKU identified for core_nt | Cost-efficiency ranking with data            |
| Query→Config recommendation        | Covers 10-3,000 query range                  |
| Near real-time latency             | < 5 min for 10 queries on warm cluster       |
| Multi-request throughput           | ≥ 3 concurrent requests without failure      |
| Report quality                     | Academic structure with charts (v1 standard) |
| Code artifact                      | `azure_optimizer.py` auto-recommend function |
