# ElasticBLAST on Azure Performance Benchmark Plan

> Created: 2026-04-17 (Updated: 2026-04-18)
> Budget: $100/day max
> Region: Korea Central
> Objective: Prove storage optimization and multi-node scaling advantages
> Limitation: LSv3 (NVMe) quota 100 vCPU — NVMe scaling limited to 3 nodes. ANF Ultra requires manual capacity pool provisioning (~$3/hr).
> Cost basis: All cost estimates use Azure pay-as-you-go (standard) pricing for Korea Central region.

---

## 1. Objectives and Research Questions

### Goal

Demonstrate that ElasticBLAST Azure delivers **measurably superior performance and cost efficiency** for production-scale BLAST searches through three key architectural advantages:

1. **Storage optimization** — Compare 5 Azure storage backends (Blob NFS, Local SSD, NVMe, ANF Ultra, RAM cache) to identify the fastest I/O path for 82 GB BLAST databases
2. **Horizontal scaling** — Prove that multi-node AKS clusters achieve super-linear speedup for embarrassingly parallel BLAST workloads
3. **Auto-tuning** — Validate the optimization profiles (cost/balanced/performance) that automatically select VM type, pod resources, and node count based on workload characteristics

### Research Questions

| ID  | Question                                                                  | Success Criteria                      |
| --- | ------------------------------------------------------------------------- | ------------------------------------- |
| RQ1 | How much does storage backend affect BLAST performance across 5 backends? | Local SSD ≥ 30% faster than Blob NFS  |
| RQ2 | Does vmtouch RAM caching eliminate storage I/O as a bottleneck?           | Warm run ≥ 50% faster than cold run   |
| RQ3 | Does multi-node execution scale linearly with node count?                 | 5 nodes ≥ 8x speedup over 1 node      |
| RQ4 | What is the cost-per-query improvement of scaling out vs. scaling up?     | Multi-node cost ≤ 1.5x single-node    |
| RQ5 | Which storage backend offers the best cost-performance ratio?             | Identify optimal $/query-hour backend |

### Key Hypotheses

- **H1**: Storage I/O is the dominant bottleneck for large DBs. Eliminating it (Local SSD + vmtouch) shifts the workload from I/O-bound to CPU-bound.
- **H2**: Once I/O-bound → CPU-bound, adding more nodes provides near-linear speedup because BLAST jobs are embarrassingly parallel (independent query batches).
- **H3**: Multi-node cost is comparable to single-node because wall-clock time decreases proportionally (N nodes × T/N time ≈ same total node-hours).
- **H4**: Pod-level resource tuning (threads, memory limit) significantly impacts per-node throughput. Current default `mem-limit=254G` causes K8s to schedule only 1-2 pods/node, leaving CPU underutilized. Reducing `mem-limit` and `num-threads` per pod increases pod density and CPU utilization.

---

## 2. Test Datasets

### Pre-staged in stgelb/blast-db

| DB                       | Size  | Type       | Program | Use Case                 |
| ------------------------ | ----- | ---------- | ------- | ------------------------ |
| wolf18/RNAvirome.S2.RDRP | 3 MB  | protein    | blastx  | Pipeline validation only |
| nt_prok                  | 82 GB | nucleotide | blastn  | Storage + scaling tests  |

### Queries

| File                     | Size   | Description                                | Est. Batches (batch_len=100K) |
| ------------------------ | ------ | ------------------------------------------ | ----------------------------- |
| small.fa                 | 1.7 KB | Tiny (blastx, pipeline test)               | 1                             |
| gut_bacteria_query.fa.gz | 956 KB | Gut bacteria 16S + E.coli contigs (blastn) | ~31                           |

**Query composition**: 3,054 sequences (3.1 MB total bases)

- 60 real 16S rRNA sequences (12 gut bacteria species × 5 reps): _B. breve, F. prausnitzii, A. muciniphila, L. rhamnosus, C. perfringens, R. bromii, E. coli, E. faecalis_ etc.
- 2,994 E. coli K12 genome fragments (1 KB contigs)
- High hit rate against nt_prok → exercises full BLAST alignment/scoring path

**Batch configuration**: `batch-len = 100000` → ~31 batches → sufficient for 5-node distribution.

---

## 3. Cost Analysis

### VM Costs (Korea Central, On-Demand)

| VM                   | vCPU | RAM    | Local Disk  | $/hr       |
| -------------------- | ---- | ------ | ----------- | ---------- |
| **Standard_E32s_v3** | 32   | 256 GB | Temp SSD    | **$2.016** |
| Standard_L32s_v3     | 32   | 256 GB | 3.8 TB NVMe | $2.496     |

### Per-Test Cost Estimate (single run)

| Phase              | Test ID    | VM      | Storage   | Nodes | Est. Duration | Est. Cost  |
| ------------------ | ---------- | ------- | --------- | ----- | ------------- | ---------- |
| Phase 1            | S1-nfs     | E32s_v3 | Blob NFS  | 1     | ~50 min       | $1.68      |
| Phase 1            | S1-ssd     | E32s_v3 | Local SSD | 1     | ~40 min       | $1.34      |
| Phase 1            | S1-nvme    | L32s_v3 | NVMe SSD  | 1     | ~35 min       | $1.46      |
| Phase 1            | S1-anf     | E32s_v3 | ANF Ultra | 1     | ~45 min       | $1.51      |
| Phase 1            | S1-warm    | E32s_v3 | Warm RAM  | 1     | ~20 min       | $0.67      |
| Phase 2            | M1-ssd-1n  | E32s_v3 | Local SSD | 1     | ~40 min       | $1.34      |
| Phase 2            | M1-ssd-3n  | E32s_v3 | Local SSD | 3     | ~20 min       | $2.02      |
| Phase 2            | M1-ssd-5n  | E32s_v3 | Local SSD | 5     | ~15 min       | $2.52      |
| Phase 2            | M1-nvme-1n | L32s_v3 | NVMe SSD  | 1     | ~35 min       | $1.46      |
| Phase 2            | M1-nvme-3n | L32s_v3 | NVMe SSD  | 3     | ~18 min       | $2.25      |
| Phase 2            | M1-anf-1n  | E32s_v3 | ANF Ultra | 1     | ~45 min       | $1.51      |
| Phase 2            | M1-anf-3n  | E32s_v3 | ANF Ultra | 3     | ~25 min       | $2.52      |
| Phase 2            | M1-anf-5n  | E32s_v3 | ANF Ultra | 5     | ~18 min       | $3.02      |
| —                  | Idle/wait  | E32s_v3 | —         | 1     | ~30 min       | $1.01      |
| **Total (1 run)**  |            |         |           |       |               | **$22.80** |
| **Total (5 runs)** |            |         |           |       |               | **~$114**  |

**Budget**: $100/day. ANF Ultra adds ~$3/hr capacity pool cost (provisioned only during test).

### Estimated Wall-Clock Time Per Run

**Cluster reuse strategy**: Instead of create/delete per test, keep clusters warm and reuse across 5 iterations + node scaling.

| Phase     | Tests  | Clusters Created | Cluster Reuse Strategy                                               | Est. Time x5  |
| --------- | ------ | ---------------- | -------------------------------------------------------------------- | ------------- |
| Phase 1   | 5      | 3                | NFS: create→5 runs+warm→delete; SSD: 5 runs; NVMe: 5 runs            | ~7 hours      |
| Phase 2   | 9      | 3                | Per storage: create 5N→5 runs→scale 3N→5 runs→scale 1N→5 runs→delete | ~8 hours      |
| **Total** | **14** | **6**            | **reuse eliminates ~15 create/delete cycles**                        | **~15 hours** |

**Overhead reduction**: 32.5h → ~15h (54% saved) by reusing clusters and nodepool scaling.

### AKS Overhead Costs

| Item                               | Cost              |
| ---------------------------------- | ----------------- |
| AKS control plane (free tier)      | $0                |
| Blob Storage (~85GB, Standard_LRS) | ~$1.50/month      |
| AKS idle (VM running, no jobs)     | $2.02/hr per node |

---

## 4. Test Matrix

### Phase 1: Storage Performance (RQ1, RQ2) — Day 1

**Goal**: Quantify storage backend impact on BLAST performance using a single node.

Uses **nt_prok (82GB)** with **1 node** and **gut_bacteria_query** — isolates storage variable.

| Test ID | DB             | Storage    | VM      | Nodes | Cluster Reuse | Description                       |
| ------- | -------------- | ---------- | ------- | ----- | ------------- | --------------------------------- |
| S1-nfs  | nt_prok (82GB) | Blob NFS   | E32s_v3 | 1     | No (cold)     | Baseline: shared NFS PVC          |
| S1-ssd  | nt_prok (82GB) | Local SSD  | E32s_v3 | 1     | New cluster   | Temp disk via hostPath            |
| S1-nvme | nt_prok (82GB) | Local NVMe | L32s_v3 | 1     | New cluster   | Dedicated NVMe via hostPath       |
| S1-anf  | nt_prok (82GB) | ANF Ultra  | E32s_v3 | 1     | New cluster   | Azure NetApp Files Ultra tier PVC |
| S1-warm | nt_prok (82GB) | Warm (RAM) | E32s_v3 | 1     | Reuse S1-nfs  | vmtouch cached, skip DB init      |

**Storage backends explained**:

| Backend    | Technology               | I/O Path            | Provisioning           |
| ---------- | ------------------------ | ------------------- | ---------------------- |
| Blob NFS   | Azure Blob NFS Premium   | Network → NFS mount | PVC (azureblob-nfs)    |
| Local SSD  | VM temp disk (managed)   | hostPath            | Per-node init-ssd job  |
| Local NVMe | L-series dedicated NVMe  | hostPath            | Per-node init-ssd job  |
| ANF Ultra  | Azure NetApp Files Ultra | Network → NFS mount | PVC (azure-netapp)     |
| Warm (RAM) | OS page cache (vmtouch)  | RAM                 | Reuse existing cluster |

**What we measure**:

- DB download time (azcopy → PV vs azcopy → local disk)
- BLAST execution time (NFS I/O vs NVMe I/O vs RAM)
- Disk IOPS, latency, throughput per backend
- Total elapsed time breakdown

**Expected outcome**:

- S1-ssd ≥ 30% faster than S1-nfs (faster local I/O)
- S1-nvme ≥ 40% faster than S1-nfs (dedicated NVMe, highest IOPS)
- S1-anf ≥ 20% faster than S1-nfs (lower latency NFS, dedicated bandwidth)
- S1-warm ≥ 50% faster than S1-nfs (DB in RAM, no I/O wait)

**Prerequisites for NVMe and ANF tests**:

- **S1-nvme**: Requires L-series VM quota (`az vm list-usage -l koreacentral | grep LSv3`). Use `machine-type = Standard_L32s_v3`.
- **S1-anf**: Requires ANF capacity pool + volume. Create before test, delete immediately after.
  ```bash
  # Create ANF resources (one-time, ~$3/hr while active)
  az netappfiles account create -g rg-elb-koc -n anfaccount -l koreacentral
  az netappfiles pool create -g rg-elb-koc -a anfaccount -n anfpool \
    --size 4 --service-level Ultra
  az netappfiles volume create -g rg-elb-koc -a anfaccount -p anfpool \
    -n anfvol --file-path anfvol --usage-threshold 4096 \
    --vnet vnet-elb --subnet subnet-anf --protocol-types NFSv3
  # Delete after test!
  az netappfiles account delete -g rg-elb-koc -n anfaccount --yes
  ```

### Phase 2: Multi-Node Scaling (RQ3, RQ4, RQ5)

**Goal**: Prove horizontal scaling delivers near-linear speedup across multiple storage backends.

Tests 3 storage backends × 2-3 node counts = 8 scaling tests.

| Test ID    | DB             | Storage   | VM      | Nodes | batch-len | Description                   |
| ---------- | -------------- | --------- | ------- | ----- | --------- | ----------------------------- |
| M1-ssd-1n  | nt_prok (82GB) | Local SSD | E32s_v3 | 1     | 100000    | SSD single-node baseline      |
| M1-ssd-3n  | nt_prok (82GB) | Local SSD | E32s_v3 | 3     | 100000    | SSD 3-node scale              |
| M1-ssd-5n  | nt_prok (82GB) | Local SSD | E32s_v3 | 5     | 100000    | SSD 5-node scale              |
| M1-nvme-1n | nt_prok (82GB) | NVMe SSD  | L32s_v3 | 1     | 100000    | NVMe single-node baseline     |
| M1-nvme-3n | nt_prok (82GB) | NVMe SSD  | L32s_v3 | 3     | 100000    | NVMe 3-node scale (quota max) |
| M1-anf-1n  | nt_prok (82GB) | ANF Ultra | E32s_v3 | 1     | 100000    | ANF single-node baseline      |
| M1-anf-3n  | nt_prok (82GB) | ANF Ultra | E32s_v3 | 3     | 100000    | ANF 3-node (shared NFS)       |
| M1-anf-5n  | nt_prok (82GB) | ANF Ultra | E32s_v3 | 5     | 100000    | ANF 5-node (shared NFS)       |

**Why 3 storage backends in scaling tests**:

- **Local SSD**: Per-node download, no shared I/O — tests pure compute scaling
- **NVMe**: Same architecture as SSD but faster I/O — tests if NVMe advantage persists at scale
- **ANF Ultra**: Shared NFS PVC — tests whether I/O contention degrades with more nodes

**Prerequisites**:

- **NVMe 3-node**: 3 × 32 = 96 vCPU (LSv3 quota 100, OK). 5-node requires support request.
- **ANF**: Capacity pool must be active during all ANF tests. Run ANF scaling tests together to minimize ANF cost.

**What we measure**:

- BLAST execution time (job start → last job complete)
- Per-node job distribution (how many batches per node)
- Total elapsed time (including fixed overhead)
- Cost: nodes × hours × $/hr
- **Storage-specific**: ANF I/O contention at 1/3/5 nodes; NVMe at 1/3 nodes

**Expected outcome**:

- SSD/NVMe: super-linear speedup (3N ~5-6×) due to CPU contention reduction; SSD extends to 5N (~8-9×)
- ANF: sub-linear speedup at 5N due to shared NFS bandwidth saturation
- NVMe > SSD at all node counts (faster per-node I/O)

**Scaling efficiency formula**:
$$\text{Scaling Efficiency} = \frac{T_1}{N \times T_N} \times 100\%$$

Where $T_1$ = single-node BLAST time, $T_N$ = N-node BLAST time. Target: ≥ 75%.

### Phase 3: Pod Resource Optimization (Future Work)

Phase 3 is reserved for testing different pod resource configurations (mem-limit, num-threads, batch-len) to find the optimal CPU/memory/pod density tradeoff. This will be run after Phase 1 and Phase 2 results are analyzed.

See `azure_optimizer.py` for the three optimization profiles (cost/balanced/performance) that auto-tune pod resources.

---

## 5. INI Config Templates

### Phase 1: Storage — Blob NFS (S1-nfs)

```ini
[cloud-provider]
azure-region = koreacentral
azure-acr-resource-group = rg-elbacr
azure-acr-name = elbacr
azure-resource-group = rg-elb-koc
azure-storage-account = stgelb
azure-storage-account-container = blast-db

[cluster]
name = elb-bench-s1
machine-type = Standard_E32s_v3
num-nodes = 1
reuse = false

[blast]
program = blastn
db = https://stgelb.blob.core.windows.net/blast-db/nt_prok/nt_prok
queries = https://stgelb.blob.core.windows.net/queries/gut_bacteria_query.fa.gz
results = https://stgelb.blob.core.windows.net/results/bench-s1-nfs
options = -evalue 0.01 -outfmt 7
batch-len = 100000
```

### Phase 1: Storage — Local SSD (S1-ssd)

```ini
[cluster]
name = elb-bench-s1-ssd
machine-type = Standard_E32s_v3
num-nodes = 1
exp-use-local-ssd = true
reuse = false
# (rest same as S1-nfs)
```

### Phase 1: Storage — Local NVMe (S1-nvme)

```ini
[cluster]
name = elb-bench-s1-nvme
machine-type = Standard_L32s_v3
num-nodes = 1
exp-use-local-ssd = true
reuse = false
# (rest same as S1-nfs)
```

### Phase 1: Storage — ANF Ultra (S1-anf)

```ini
[cluster]
name = elb-bench-s1-anf
machine-type = Standard_E32s_v3
num-nodes = 1
reuse = false
# ANF requires setting ELB_STORAGE_CLASS env var before submit:
# ELB_STORAGE_CLASS=azure-netapp-ultra
# (rest same as S1-nfs)
```

### Phase 2: Scaling — 1/3/5 nodes (M1-Nnode)

```ini
[cluster]
name = elb-bench-m1
machine-type = Standard_E32s_v3
num-nodes = 1   # Change to 3 or 5
reuse = false
exp-use-local-ssd = true

[blast]
program = blastn
db = https://stgelb.blob.core.windows.net/blast-db/nt_prok/nt_prok
queries = https://stgelb.blob.core.windows.net/queries/gut_bacteria_query.fa.gz
results = https://stgelb.blob.core.windows.net/results/bench-m1-1node
options = -evalue 0.01 -outfmt 7
batch-len = 100000
```

---

## 6. Metrics Collection

### Per Test — Required

| Metric                  | Source                          | Unit    |
| ----------------------- | ------------------------------- | ------- |
| Total elapsed time      | Wall clock (submit → complete)  | seconds |
| Cluster create time     | AKS provisioning                | seconds |
| DB download time        | init-pv / init-ssd job duration | seconds |
| Job submit time         | submit-jobs pod duration        | seconds |
| BLAST execution time    | BLAST jobs (start → complete)   | seconds |
| Results upload time     | results-export sidecar          | seconds |
| Pods succeeded / failed | kubectl get jobs                | count   |
| Num query batches       | ElasticBLAST log                | count   |
| Estimated cost          | elapsed_hr × $/hr × nodes       | USD     |

### Per Test — If Available (kubectl exec into pods)

| Metric               | Source                        | Unit     |
| -------------------- | ----------------------------- | -------- |
| Disk read IOPS       | /proc/diskstats               | IOPS     |
| Disk read throughput | /proc/diskstats               | MB/s     |
| Disk latency         | iostat                        | ms       |
| CPU utilization      | /proc/stat or top             | %        |
| Memory used          | /proc/meminfo                 | GB       |
| BLAST \time output   | blast-run-aks.sh \time stderr | real/sys |

### Derived Metrics

| Metric              | Formula                                    |
| ------------------- | ------------------------------------------ |
| Storage speedup     | T(Blob NFS) / T(Local SSD)                 |
| Warm cache speedup  | T(cold) / T(warm)                          |
| Scaling efficiency  | T(1 node) / (N × T(N nodes)) × 100%        |
| Cost per query-hour | cost_usd / (query_size_MB × blast_time_hr) |
| Overhead ratio      | (total - blast_time) / total × 100%        |

---

## 7. Execution Procedure

### Day 1: Phase 1 (Storage) + Phase 2 (Scaling)

Each test is run **5 times** with **cluster reuse** to minimize overhead.

**Key principle**: Create cluster once → run 5 iterations → delete. Between iterations, only clean BLAST jobs (not the cluster or DB).

```bash
# ── Step 0: Prerequisites ──
cd /home/moonchoi/dev/elastic-blast-azure
source venv/bin/activate
az storage account update -n stgelb -g rg-elb-koc --public-network-access Enabled -o none

# Upload gut bacteria query to blob storage (one-time)
AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy cp \
  benchmark/queries/gut_bacteria_query.fa.gz \
  "https://stgelb.blob.core.windows.net/queries/gut_bacteria_query.fa.gz"

# ── Phase 1: Storage tests (1 node, 5 backends × 5 runs) ──

# --- S1-nfs + S1-warm (1 cluster, 5 cold + 5 warm runs) ---
for run in 1 2 3 4 5; do
  PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
    python bin/elastic-blast submit --cfg benchmark/configs/bench-s1-nfs.ini
  # Collect metrics, then clean jobs only (keep cluster + DB)
  kubectl delete jobs -l app=blast --ignore-not-found
  kubectl delete job submit-jobs elb-finalizer --ignore-not-found

  # Warm run (DB cached in RAM via vmtouch)
  PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
    python bin/elastic-blast submit --cfg benchmark/configs/bench-s1-warm.ini
  kubectl delete jobs -l app=blast --ignore-not-found
  kubectl delete job submit-jobs elb-finalizer --ignore-not-found
done
PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast delete \
  --cfg benchmark/configs/bench-s1-nfs.ini

# --- S1-ssd (1 cluster, 5 runs) ---
for run in 1 2 3 4 5; do
  PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
    python bin/elastic-blast submit --cfg benchmark/configs/bench-s1-ssd.ini
  kubectl delete jobs -l app=blast --ignore-not-found
  kubectl delete job submit-jobs elb-finalizer --ignore-not-found
done
PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast delete \
  --cfg benchmark/configs/bench-s1-ssd.ini

# --- S1-nvme (1 cluster L32s_v3, 5 runs) ---
# (same pattern as SSD)

# --- S1-anf (1 cluster, 5 runs — provision ANF first!) ---
# (same pattern, ANF pool active during all 5 runs)

# ── Phase 2: Scaling tests (3 backends × 3 node counts × 5 runs) ──
# Create 5N cluster once, run 5x at 5N, scale to 3N, run 5x, scale to 1N, run 5x, delete.

# --- SSD scaling ---
# Create 5-node cluster
PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
  python bin/elastic-blast submit --cfg benchmark/configs/bench-m1-ssd-5n.ini
for run in 1 2 3 4 5; do
  kubectl delete jobs -l app=blast --ignore-not-found
  kubectl delete job submit-jobs elb-finalizer --ignore-not-found
  PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
    python bin/elastic-blast submit --cfg benchmark/configs/bench-m1-ssd-5n.ini
done
# Scale to 3 nodes
az aks nodepool scale -g rg-elb-koc --cluster-name elb-bench-m1-ssd \
  --name nodepool1 --node-count 3
for run in 1 2 3 4 5; do
  kubectl delete jobs -l app=blast --ignore-not-found
  kubectl delete job submit-jobs elb-finalizer --ignore-not-found
  PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
    python bin/elastic-blast submit --cfg benchmark/configs/bench-m1-ssd-3n.ini
done
# Scale to 1 node
az aks nodepool scale -g rg-elb-koc --cluster-name elb-bench-m1-ssd \
  --name nodepool1 --node-count 1
for run in 1 2 3 4 5; do
  kubectl delete jobs -l app=blast --ignore-not-found
  kubectl delete job submit-jobs elb-finalizer --ignore-not-found
  PYTHONPATH=src:$PYTHONPATH AZCOPY_AUTO_LOGIN_TYPE=AZCLI \
    python bin/elastic-blast submit --cfg benchmark/configs/bench-m1-ssd-1n.ini
done
PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast delete \
  --cfg benchmark/configs/bench-m1-ssd-5n.ini

# --- NVMe scaling (same pattern with L32s_v3) ---
# --- ANF scaling (same pattern with ANF pool) ---

# ── Cleanup ──
az storage account update -n stgelb -g rg-elb-koc --public-network-access Disabled -o none
```

---

## 8. Cost Safety Rules

1. **Always delete clusters** after each test — never leave nodes running overnight
2. **AKS idle cost**: $2.02/hr per E32s_v3 node ($10.08/hr for 5 nodes)
3. **Budget guard**: Check `az consumption usage list` before each day
4. **Emergency stop**: `az aks delete -g rg-elb-koc -n <name> --no-wait --yes`
5. **Storage public access**: Disable when not testing (`--public-network-access Disabled`)
6. **Cluster reuse**: Keep cluster warm across 5 iterations; only clean BLAST jobs between runs\n7. **Nodepool scaling**: In Phase 2, scale nodepool 5→3→1 instead of create/delete per node count", "oldString": "6. **Cluster reuse**: Within Phase 1, reuse S1-nfs cluster for S1-warm test
7. **Max runtime**: Kill any single test exceeding 2 hours
8. **ANF cleanup**: Delete ANF account immediately after S1-anf test (~$3/hr)
9. **Daily budget**: $100/day hard stop

---

## 9. Expected Results

### Phase 1: Storage Performance

| Test ID | Storage   | VM      | Expected Total | Expected BLAST | Key Bottleneck          |
| ------- | --------- | ------- | -------------- | -------------- | ----------------------- |
| S1-nfs  | Blob NFS  | E32s_v3 | ~40-60 min     | ~15-25 min     | NFS network latency     |
| S1-ssd  | Local SSD | E32s_v3 | ~30-45 min     | ~10-18 min     | Temp disk I/O           |
| S1-nvme | NVMe SSD  | L32s_v3 | ~25-40 min     | ~8-15 min      | azcopy download         |
| S1-anf  | ANF Ultra | E32s_v3 | ~35-50 min     | ~12-20 min     | ANF network throughput  |
| S1-warm | Warm RAM  | E32s_v3 | ~15-25 min     | ~8-12 min      | CPU-bound (no I/O wait) |

**Expected ranking**: Warm (RAM) > NVMe > Local SSD > ANF Ultra > Blob NFS

### Phase 2: Multi-Node Scaling

**Local SSD scaling** (per-node independent I/O):

| Test ID   | Nodes | Expected BLAST | Expected Scaling | Cost  |
| --------- | ----- | -------------- | ---------------- | ----- |
| M1-ssd-1n | 1     | ~15-25 min     | 1.0x (baseline)  | $1.34 |
| M1-ssd-3n | 3     | ~3-5 min       | ~5-6x            | $2.02 |
| M1-ssd-5n | 5     | ~1-3 min       | ~8-9x            | $2.52 |

**NVMe scaling** (per-node independent I/O, faster disk — max 3N due to quota):

| Test ID    | Nodes | Expected BLAST | Expected Scaling | Cost  |
| ---------- | ----- | -------------- | ---------------- | ----- |
| M1-nvme-1n | 1     | ~10-18 min     | 1.0x (baseline)  | $1.46 |
| M1-nvme-3n | 3     | ~2-4 min       | ~5-6x            | $2.25 |

**ANF Ultra scaling** (shared NFS, I/O contention expected):

| Test ID   | Nodes | Expected BLAST | Expected Scaling | Cost  |
| --------- | ----- | -------------- | ---------------- | ----- |
| M1-anf-1n | 1     | ~12-20 min     | 1.0x (baseline)  | $1.51 |
| M1-anf-3n | 3     | ~5-8 min       | ~2-3x            | $2.52 |
| M1-anf-5n | 5     | ~3-6 min       | ~3-4x            | $3.02 |

**Expected conclusion**: SSD/NVMe scale super-linearly (CPU-bound). ANF scales sub-linearly at 5N due to shared NFS bandwidth saturation. NVMe consistently faster than SSD at all node counts.

---

## 10. Report Structure

The final report (`benchmark/results/report-final.md`) follows the project's academic paper structure:

### Required Charts

1. **Storage comparison bar chart** — S1-nfs vs S1-ssd vs S1-nvme vs S1-anf vs S1-warm (total time, BLAST time)
2. **Multi-node scaling chart** — 1/3/5 nodes BLAST time + ideal linear scaling line
3. **Scaling efficiency chart** — actual vs ideal speedup
4. **Cost-performance scatter** — cost vs BLAST time for all configurations
5. **Per-job time distribution** — box plot by node count
6. **Storage I/O metrics** — IOPS, throughput, latency comparison across backends

### Key Tables

1. Research question answers (RQ1-RQ5 with data)
2. Phase timing breakdown (per-test, all phases)
3. Scaling efficiency (actual vs theoretical)
4. Cost-per-query comparison across all 5 storage backends
5. Storage I/O metrics (IOPS, throughput, latency) per backend

### Success Criteria Summary

| RQ  | Criterion                            | Metric               | Target |
| --- | ------------------------------------ | -------------------- | ------ |
| RQ1 | Local SSD faster than Blob NFS       | BLAST time ratio     | ≥ 1.3x |
| RQ2 | Warm cache eliminates I/O bottleneck | Cold/warm time ratio | ≥ 1.5x |
| RQ3 | Multi-node scales effectively        | 5N speedup vs 1N     | ≥ 8x   |
| RQ4 | Cost-efficient scaling               | Cost ratio (5n/1n)   | ≤ 1.5x |
| RQ5 | Optimal storage backend identified   | $/query-hour ranking | Report |

---

## 11. Risks and Mitigations

| Risk                                 | Impact | Mitigation                                              | Status    |
| ------------------------------------ | ------ | ------------------------------------------------------- | --------- |
| Query file generates too few batches | RQ3    | batch-len=100K gives ~31 batches (verified)             | OK        |
| AKS cluster creation timeout         | All    | Retry once; if fails, use existing cluster              | —         |
| 5-node E32s_v3 quota exceeded        | RQ3    | Quota increased to 200 vCPU ESv3, 500 regional          | **Fixed** |
| L32s_v3 quota not available          | RQ1    | LSv3 quota 100 vCPU available (verified)                | **OK**    |
| ANF capacity pool cost overrun       | RQ1    | Create pool immediately before test, delete right after | —         |
| ANF VNet/subnet not configured       | RQ1    | Pre-create delegated subnet for ANF before benchmark    | —         |
| Blob NFS performance variability     | RQ1    | Run each test 5x, report median                         | —         |
| Budget overrun                       | All    | Hard stop at $100/day; single run ~$26, 5 runs ~$130    | —         |
| init-ssd get-blastdb container stuck | All    | **Fixed**: pkill azcopy added to script                 | **Fixed** |
| CPU request > limit for >4 nodes     | RQ3    | **Fixed**: cpu_req capped at num_cpus - 2               | **Fixed** |
| PV race condition (.azDownload)      | All    | **Fixed**: subPath: queries in init templates           | **Fixed** |
| blast-run azcopy login delay         | All    | **Fixed**: deferred to failure handler only             | **Fixed** |
| L32s_v3 quota insufficient for 5N    | RQ3    | NVMe scaling limited to 3N (96/100 vCPU); SSD covers 5N | OK        |

---

## 12. Comparison with Upstream ElasticBLAST

Direct comparison with GCP ElasticBLAST is out of scope (requires GCP environment). Instead:

### Published Reference Points

| Source                  | Workload           | Time    | Cost   | Platform |
| ----------------------- | ------------------ | ------- | ------ | -------- |
| Camacho et al. 2023 [2] | nr DB, 10K queries | ~2-4 hr | ~$5-15 | GCP      |
| Tsai 2021 [3]           | nt DB, HPC cluster | ~1-3 hr | ~$10+  | Azure VM |

### Azure ElasticBLAST Advantages (to validate)

| Feature       | Upstream (GCP)        | Azure ElasticBLAST    | Expected Benefit      |
| ------------- | --------------------- | --------------------- | --------------------- |
| Storage       | GCS + Persistent Disk | Blob NFS / Local SSD  | Lower I/O latency     |
| DB caching    | None                  | vmtouch RAM caching   | 50%+ faster warm runs |
| Download tool | gsutil                | azcopy (optimized)    | Faster DB transfer    |
| Auto-tuning   | Manual config         | Optimization profiles | Right-sized resources |
| Auth          | Service Account keys  | Managed Identity      | More secure           |

The benchmark results will be presented alongside these reference points to contextualize Azure performance.
