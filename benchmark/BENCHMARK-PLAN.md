# ElasticBLAST Azure Storage Benchmark Plan

> Created: 2026-04-03
> Budget: $30/day max
> Region: Korea Central

---

## 1. Objective

Quantify the storage I/O impact on BLAST execution time across 3 storage backends:

| Backend        | Description                                  | Status     |
| -------------- | -------------------------------------------- | ---------- |
| **Blob NFS**   | Azure Blob NFS Premium (default PVC mode)    | Baseline   |
| **Local NVMe** | Node-local SSD via hostPath                  | Comparison |
| **Warm (RAM)** | Re-run on existing cluster, DB cached in RAM | Best-case  |

ANF (Azure NetApp Files) is excluded from this round — provisioning requires a separate capacity pool (~$1,000/TB/month minimum), which exceeds the budget.

---

## 2. Test Datasets

### DB Sources

NCBI publishes pre-formatted BLAST databases on **AWS S3** (`s3://ncbi-blast-databases`, public, no auth required).  
azcopy can transfer S3 → Azure Blob directly at **~860 MB/s** — much faster than NCBI FTP.

### Already Staged (stgelb/blast-db)

| DB                       | Size  | Type       | Program |
| ------------------------ | ----- | ---------- | ------- |
| wolf18/RNAvirome.S2.RDRP | 3 MB  | protein    | blastx  |
| 260_part_aa              | 2 GB  | nucleotide | blastn  |
| nt_prok                  | 82 GB | nucleotide | blastn  |

### To Pre-stage from S3

| DB        | Size on S3 | Purpose                          | Transfer Time |
| --------- | ---------- | -------------------------------- | ------------- |
| swissprot | ~340 MB    | Small protein DB, fast iteration | ~1 sec        |
| env_nt    | ~340 MB    | Small nucleotide DB              | ~1 sec        |

Pre-staging command:

```bash
# S3 → Azure Blob (free egress from AWS S3 public bucket)
azcopy cp \
  "https://ncbi-blast-databases.s3.amazonaws.com/2025-09-16-01-05-02/swissprot*" \
  "https://stgelb.blob.core.windows.net/blast-db/swissprot/" \
  --block-size-mb=256
```

### Queries

| File                 | Size   | Description                      |
| -------------------- | ------ | -------------------------------- |
| small.fa             | 1.7 KB | Tiny test query (blastx)         |
| JAIJZY01.1.fsa_nt.gz | 1 MB   | Medium nucleotide query (blastn) |

---

## 3. Cost Analysis

### VM Costs (Korea Central)

| VM                   | vCPU | RAM    | $/hr       | Max hours at $30/day |
| -------------------- | ---- | ------ | ---------- | -------------------- |
| Standard_D8s_v3      | 8    | 32 GB  | $0.384     | 78h                  |
| Standard_E16s_v3     | 16   | 128 GB | $1.008     | 29.7h                |
| **Standard_E32s_v3** | 32   | 256 GB | **$2.016** | **14.8h**            |

### Storage Costs (negligible for benchmark)

| Item                               | Cost                 |
| ---------------------------------- | -------------------- |
| Blob Storage (Standard_LRS, ~85GB) | ~$1.50/month         |
| Egress from S3 to Azure            | Free (public bucket) |
| AKS control plane (free tier)      | $0                   |

### Per-Test Cost Estimate

| Test                 | VM      | Nodes | Max Duration | Est. Cost |
| -------------------- | ------- | ----- | ------------ | --------- |
| Small DB (3MB) cold  | E32s_v3 | 1     | ~20 min      | ~$0.67    |
| Small DB warm        | E32s_v3 | 1     | ~5 min       | ~$0.17    |
| Medium DB (2GB) cold | E32s_v3 | 1     | ~30 min      | ~$1.00    |
| Medium DB NVMe       | E32s_v3 | 1     | ~30 min      | ~$1.00    |
| Large DB (82GB) cold | E32s_v3 | 1     | ~60 min      | ~$2.02    |
| Large DB warm        | E32s_v3 | 1     | ~15 min      | ~$0.50    |
| **AKS cluster idle** | E32s_v3 | 1     | per hour     | **$2.02** |

**Critical**: AKS cluster charges even when idle (VM running). Always `az aks stop` when not actively testing.

### Daily Budget Plan

| Phase                                    | Tests           | Est. AKS Time | Est. Cost  |
| ---------------------------------------- | --------------- | ------------- | ---------- |
| Pre-stage DBs                            | 0 (azcopy only) | 0             | ~$0.01     |
| Phase S1: Small DB (Blob NFS cold/warm)  | 2               | ~25 min       | ~$0.84     |
| Phase S2: Medium DB (Blob NFS vs NVMe)   | 2               | ~60 min       | ~$2.00     |
| Phase S3: Large DB (Blob NFS cold, warm) | 2               | ~75 min       | ~$2.52     |
| Phase S4: Large DB (NVMe)                | 1               | ~60 min       | ~$2.02     |
| Cluster idle time buffer                 | -               | ~30 min       | ~$1.00     |
| **Total Day 1**                          | **7 tests**     | **~4h**       | **~$8.39** |

Well within the $30/day budget. Buffer for retries: ~$21 remaining.

---

## 4. Test Matrix

### Phase S1: Baseline (Small DB, 3MB)

Purpose: Validate benchmark pipeline, measure overhead (cluster create, job submit).

| ID      | DB           | Storage  | VM      | Nodes | Run  |
| ------- | ------------ | -------- | ------- | ----- | ---- |
| S1-cold | wolf18 (3MB) | blob_nfs | E32s_v3 | 1     | Cold |
| S1-warm | wolf18 (3MB) | warm     | E32s_v3 | 1     | Warm |

### Phase S2: Storage Comparison (Medium DB, 2GB)

Purpose: Measure storage I/O impact with a real-world sized DB.

| ID      | DB                | Storage  | VM      | Nodes | Run  |
| ------- | ----------------- | -------- | ------- | ----- | ---- |
| S2-blob | 260_part_aa (2GB) | blob_nfs | E32s_v3 | 1     | Cold |
| S2-nvme | 260_part_aa (2GB) | nvme     | E32s_v3 | 1     | Cold |

### Phase S3: Large DB (82GB) — Blob NFS

Purpose: Stress-test Blob NFS with production-scale DB.

| ID      | DB             | Storage  | VM      | Nodes | Run  |
| ------- | -------------- | -------- | ------- | ----- | ---- |
| S3-cold | nt_prok (82GB) | blob_nfs | E32s_v3 | 1     | Cold |
| S3-warm | nt_prok (82GB) | warm     | E32s_v3 | 1     | Warm |

### Phase S4: Large DB (82GB) — Local NVMe

Purpose: Compare NVMe vs Blob NFS for large DB.

| ID      | DB             | Storage | VM      | Nodes | Run  |
| ------- | -------------- | ------- | ------- | ----- | ---- |
| S4-nvme | nt_prok (82GB) | nvme    | E32s_v3 | 1     | Cold |

---

## 5. Metrics to Collect

Per test:

- **Total elapsed time** (wall clock)
- **Per-phase timing**: cluster create, DB download, job submit, BLAST execution, results upload
- **BLAST phase separation**: Phase 1 (DB read), Phase 2 (alignment), Phase 3 (write)
- **K8s job status**: succeeded/failed/active counts, per-job timestamps
- **Azure Monitor**: CPU%, Memory, Disk IOPS, Network (time-series, 1-min intervals)
- **Pod metrics**: /proc/diskstats, /proc/meminfo (via kubectl exec)
- **Cost**: elapsed_hours × vm_cost_hr × num_nodes

---

## 6. Execution Steps

```bash
# 1. Pre-stage swissprot from S3 (optional, for future use)
AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy cp \
  "https://ncbi-blast-databases.s3.amazonaws.com/2025-09-16-01-05-02/swissprot*" \
  "https://stgelb.blob.core.windows.net/blast-db/swissprot/" \
  --block-size-mb=256

# 2. Run benchmark
cd /home/moonchoi/dev/elastic-blast-azure
source venv/bin/activate
PYTHONPATH=src:$PYTHONPATH python benchmark/run_benchmark.py --phase B

# 3. Stop cluster when done (critical for cost!)
az aks stop -g rg-elb-koc -n <cluster-name> --no-wait

# 4. Disable public access when done
az storage account update -n stgelb -g rg-elb-koc --public-network-access Disabled -o none
```

---

## 7. Cost Safety Rules

1. **Always stop AKS clusters** after each test phase (`az aks stop`)
2. **Never leave E32s_v3 running overnight** — that's $48/day for 1 node
3. **Check cost before starting**: `az consumption usage list` or Azure Cost Management
4. **Set budget alert**: $30/day on the subscription
5. **Prefer smaller VMs** for debugging: D8s_v3 ($0.384/hr) instead of E32s_v3 ($2.016/hr)
6. **Reuse clusters** wherever possible to avoid 10-15 min create overhead

---

## 8. Expected Results

| Scenario                       | Expected Time | Why                                       |
| ------------------------------ | ------------- | ----------------------------------------- |
| Small DB, Blob NFS cold        | ~15-20 min    | Mostly cluster create + job overhead      |
| Small DB, warm                 | ~3-5 min      | DB trivial, measures pipeline overhead    |
| Medium DB (2GB), Blob NFS      | ~10-15 min    | Moderate I/O, DB fits in RAM easily       |
| Medium DB (2GB), NVMe          | ~8-12 min     | Faster init (local disk), same BLAST time |
| Large DB (82GB), Blob NFS cold | ~30-60 min    | DB download is the bottleneck             |
| Large DB (82GB), warm          | ~10-15 min    | DB in RAM, CPU-bound BLAST                |
| Large DB (82GB), NVMe          | ~25-45 min    | Per-node download, but faster I/O         |

Key hypothesis: **For large DBs, the dominant factor is DB download/caching time, not storage I/O latency during BLAST execution** — because vmtouch caches the DB into RAM.
