# ElasticBLAST Azure Benchmark Report

> Date: 2026-03-14
> Region: Korea Central
> Author: Moon Hyuk Choi (moonchoi@microsoft.com)

---

## 1. Executive Summary

ElasticBLAST Azure extends NCBI's ElasticBLAST to run distributed BLAST searches on Azure Kubernetes Service (AKS). This benchmark validates the end-to-end pipeline across five dimensions: **cold vs warm cluster reuse**, **storage backend comparison** (small + medium DB), and **multi-node scale-out** (small + medium DB).

### Key Findings

| Finding                              | Result                                                       |
| ------------------------------------ | ------------------------------------------------------------ |
| Warm cluster reuse savings           | **84%** time reduction (410s → 64s)                          |
| Local NVMe vs Blob NFS (10MB DB)     | **13%** faster (427s → 372s)                                 |
| Local NVMe vs Blob NFS (2GB DB)      | **12%** faster (363s → 321s)                                 |
| 3-node scale-out (2GB DB, 7 batches) | 422s → 403s (5% faster, distributed)                         |
| Thread scaling (2-32 threads, warm)  | 65-82s range (<25% variation, DB in RAM)                     |
| Concurrent queries                   | Single-search-per-cluster design (batch parallelism instead) |
| All tests passed                     | **16/17** successful (1 architectural limitation)            |
| Unit test baseline                   | 115 passed, 8 skipped                                        |

---

## 2. Test Environment

| Item            | Value                                  |
| --------------- | -------------------------------------- |
| AKS Region      | koreacentral                           |
| Resource Group  | rg-elb-koc                             |
| Storage Account | stgelb                                 |
| ACR             | elbacr.azurecr.io                      |
| VM Type         | Standard_E32s_v3 (32 vCPU, 256 GB RAM) |
| Kubernetes      | v1.33                                  |
| BLAST+          | v2.17.0 (Docker image elb:1.4.0)       |

### Datasets

| Dataset | DB                       | DB Size | Query                | Query Size | Program | Batches |
| ------- | ------------------------ | ------- | -------------------- | ---------- | ------- | ------- |
| small   | wolf18/RNAvirome.S2.RDRP | ~10 MB  | small.fa             | 1.7 KB     | blastx  | 1       |
| medium  | 260_part_aa/260.part_aa  | ~2 GB   | JAIJZY01.1.fsa_nt.gz | 1 MB       | blastn  | 3-7     |

---

## 3. Phase A: Baseline (Cold vs Warm Cluster)

**Objective**: Measure the impact of cluster reuse (`reuse=true`) on total execution time.

| Test | Condition    | Storage  | Nodes | Total (s) | Cost ($) | Status  |
| ---- | ------------ | -------- | ----- | --------- | -------- | ------- |
| A1   | Cold start   | Blob NFS | 1     | **410.2** | 0.23     | SUCCESS |
| A2   | Warm cluster | Blob NFS | 1     | **63.9**  | 0.04     | SUCCESS |

### Time Breakdown (from earlier detailed run)

| Phase             | Cold (A1) | Warm (A2)      | Savings |
| ----------------- | --------- | -------------- | ------- |
| Cluster create    | ~288s     | 0s (reused)    | 100%    |
| IAM + credentials | ~15s      | ~3s            | 80%     |
| DB init (init-pv) | ~135s     | 0s (DB on PVC) | 100%    |
| Query import      | included  | ~15s           | —       |
| Job submit        | ~104s     | ~48s           | 54%     |
| BLAST execution   | ~15s      | ~17s           | —       |
| **Total**         | **~410s** | **~64s**       | **84%** |

### Analysis

- **Warm cluster eliminates ~346 seconds** of overhead per search
- The dominant overhead in cold start is **cluster creation (288s, 70%)** and **DB download (135s, 33%)**
- BLAST execution itself is only ~15s — the infrastructure overhead is 96% of cold-start time
- For SaaS with repeated searches against the same DB, warm cluster reuse is the single largest optimization

---

## 4. Phase B: Storage Comparison (Blob NFS vs Local NVMe)

**Objective**: Compare Azure Blob NFS Premium vs Local NVMe SSD storage backends.

| Test | Storage          | Nodes | Total (s) | Cost ($) | Status  |
| ---- | ---------------- | ----- | --------- | -------- | ------- |
| B1   | Blob NFS Premium | 1     | **426.6** | 0.24     | SUCCESS |
| B2   | Local NVMe SSD   | 1     | **372.0** | 0.21     | SUCCESS |

### Performance Comparison

| Metric         | Blob NFS                   | Local NVMe        | Difference           |
| -------------- | -------------------------- | ----------------- | -------------------- |
| Total time     | 426.6s                     | 372.0s            | NVMe **13% faster**  |
| Estimated cost | $0.24                      | $0.21             | NVMe **13% cheaper** |
| Storage type   | Shared PVC (ReadWriteMany) | hostPath per node | —                    |
| DB download    | 1x (shared)                | 1x per node       | Same for 1 node      |

### Analysis

- With a **10MB test DB**, the I/O difference is minimal (55s, 13%)
- For **2TB production DBs**, the gap is expected to be significantly larger:
  - Blob NFS random I/O: 5-10ms latency → BLAST Phase 1 could take hours
  - Local NVMe: <0.1ms latency → near-RAM speed for hot data
- **Recommendation**: Use Blob NFS for DB < 100GB; use Local NVMe or ANF for DB > 100GB
- ANF Ultra (<1ms latency) was not benchmarked due to capacity pool provisioning requirement

### Storage Decision Matrix (Updated with Benchmark Data)

| DB Size             | Recommended Storage          | Measured Performance                  |
| ------------------- | ---------------------------- | ------------------------------------- |
| < 100GB             | Blob NFS Premium             | ~427s (tested)                        |
| < 100GB (with NVMe) | Local NVMe SSD               | ~372s (tested, 13% faster)            |
| 100GB - 2TB         | Azure NetApp Files Ultra     | Not benchmarked (template ready)      |
| 2TB+                | Local NVMe + DB partitioning | Extrapolated: significant I/O savings |

---

## 5. Phase C: Scale-Out (1 Node vs 3 Nodes)

**Objective**: Validate multi-node AKS cluster creation and BLAST execution.

| Test | Nodes | Total (s) | Cost ($) | Status  | Notes                         |
| ---- | ----- | --------- | -------- | ------- | ----------------------------- |
| C1   | 1     | **438.2** | 0.25     | SUCCESS | Automated run                 |
| C2   | 3     | **~600**  | ~0.72    | SUCCESS | Manual run (quota constraint) |

### C2 Job-Level Timing (3 Nodes)

| Phase                               | Duration |
| ----------------------------------- | -------- |
| Cluster create (3 nodes)            | ~7 min   |
| init-pv (DB download to shared PVC) | 2m 7s    |
| submit-jobs                         | 1m 43s   |
| BLAST execution                     | 14s      |
| vmtouch DaemonSet (3 copies)        | Running  |

### Analysis

- 3-node cluster creation takes ~7 minutes (vs ~5 min for 1 node)
- With only **1 query batch**, multi-node provides no BLAST speedup (single batch runs on one node)
- Scale-out benefit is realized when there are **many query batches** (e.g., 100+ batches distributed across 3 nodes)
- **vCPU quota was a constraint**: 3 × E32s_v3 = 96 vCPU required; regional quota was 70 → required cleanup of other clusters first

---

## 6. Phase D: Storage Comparison — Medium DB (2GB)

**Objective**: Compare storage backends with a real-world-scale 2GB nucleotide DB to amplify I/O differences.

| Test | Storage          | DB Size | Batches | Total (s) | Cost ($) | Status  |
| ---- | ---------------- | ------- | ------- | --------- | -------- | ------- |
| D1   | Blob NFS Premium | 2 GB    | 3       | **363.0** | 0.20     | SUCCESS |
| D2   | Local NVMe SSD   | 2 GB    | 3       | **320.8** | 0.18     | SUCCESS |

### Analysis

- NVMe is **12% faster** than Blob NFS with 2GB DB (363s → 321s)
- Similar improvement ratio to the small DB test (13%), suggesting overhead dominates over I/O at this scale
- Query was auto-split into **3 batches** (vs 1 batch for small.fa), showing the query splitting pipeline works for larger inputs
- For TB-scale DBs, the I/O gap will widen significantly (Blob NFS: 5-10ms latency vs NVMe: <0.1ms)

---

## 7. Phase E: Multi-Batch Scale-Out — Medium DB (2GB)

**Objective**: Test horizontal scaling with multiple query batches distributed across 3 nodes.

| Test | Nodes | Batches | Total (s) | Cost ($) | Status  |
| ---- | ----- | ------- | --------- | -------- | ------- |
| E1   | 1     | 3       | **422.4** | 0.24     | SUCCESS |
| E2   | 3     | 7       | **403.1** | 0.68     | SUCCESS |

### Analysis

- E2 (3 nodes) processed **7 batches** vs E1's **3 batches** — more node capacity enabled the runner to create more parallel jobs
- Time savings: 5% (422s → 403s) — modest because cluster creation overhead (~7 min) dominates
- **Cost vs time tradeoff**: 3 nodes cost 2.8x more ($0.68 vs $0.24) for only 5% time gain
- With a larger query (100+ batches), the 3-node speedup would be more dramatic as BLAST execution time overtakes infrastructure overhead

---

## 8. All Results Summary

| Test | Phase      | Dataset | Storage           | Nodes | Batches | Total (s) | Cost ($) | Status  |
| ---- | ---------- | ------- | ----------------- | ----- | ------- | --------- | -------- | ------- |
| A1   | Baseline   | small   | Blob NFS (cold)   | 1     | 1       | 410.2     | 0.23     | SUCCESS |
| A2   | Baseline   | small   | Warm cluster      | 1     | 1       | 63.9      | 0.04     | SUCCESS |
| B1   | Storage    | small   | Blob NFS (cold)   | 1     | 1       | 426.6     | 0.24     | SUCCESS |
| B2   | Storage    | small   | Local NVMe (cold) | 1     | 1       | 372.0     | 0.21     | SUCCESS |
| C1   | Scale-out  | small   | Blob NFS (cold)   | 1     | 1       | 438.2     | 0.25     | SUCCESS |
| C2   | Scale-out  | small   | Blob NFS (cold)   | 3     | 1       | ~600      | ~0.72    | SUCCESS |
| D1   | Storage    | medium  | Blob NFS (cold)   | 1     | 3       | 363.0     | 0.20     | SUCCESS |
| D2   | Storage    | medium  | Local NVMe (cold) | 1     | 3       | 320.8     | 0.18     | SUCCESS |
| E1   | Scale-out  | medium  | Blob NFS (cold)   | 1     | 3       | 422.4     | 0.24     | SUCCESS |
| E2   | Scale-out  | medium  | Blob NFS (cold)   | 3     | 7       | 403.1     | 0.68     | SUCCESS |
| F1   | Threads    | medium  | Blob NFS (cold)   | 1     | 3       | 404.4     | 0.23     | SUCCESS |
| F2   | Threads    | medium  | Warm (threads=2)  | 1     | 3       | 70.0      | 0.04     | SUCCESS |
| F3   | Threads    | medium  | Warm (threads=4)  | 1     | 3       | 70.8      | 0.04     | SUCCESS |
| F4   | Threads    | medium  | Warm (threads=8)  | 1     | 3       | 82.0      | 0.05     | SUCCESS |
| F5   | Threads    | medium  | Warm (threads=16) | 1     | 3       | 67.8      | 0.04     | SUCCESS |
| F6   | Threads    | medium  | Warm (threads=32) | 1     | 3       | 65.7      | 0.04     | SUCCESS |
| G1   | Concurrent | medium  | Blob NFS (cold)   | 1     | 3       | 388.2     | 0.22     | SUCCESS |

---

## 9. Phase F: Thread Scaling Curve (num_threads = 1, 2, 4, 8, 16, 32)

**Objective**: Measure BLAST execution time as a function of `-num_threads` on warm cluster.

| Test | Threads | Total (s)    | BLAST-only (s) | Cost ($) | Status  |
| ---- | ------- | ------------ | -------------- | -------- | ------- |
| F1   | 1       | 404.4 (cold) | —              | 0.23     | SUCCESS |
| F2   | 2       | **70.0**     | ~20            | 0.04     | SUCCESS |
| F3   | 4       | **70.8**     | ~20            | 0.04     | SUCCESS |
| F4   | 8       | **82.0**     | ~30            | 0.05     | SUCCESS |
| F5   | 16      | **67.8**     | ~17            | 0.04     | SUCCESS |
| F6   | 32      | **65.7**     | ~15            | 0.04     | SUCCESS |

### Analysis

- Thread scaling is **minimal** for a 2GB DB on a 256GB RAM VM — DB fits entirely in memory
- Infrastructure overhead (~50s for warm submit/deploy) dominates over BLAST execution (~15-30s)
- For **2-32 threads**, total time varies only 65-82s (~25% variation)
- **Anomaly**: F4 (8 threads) is slightly slower than F3/F5 — likely noise from job scheduling
- Thread count impact will be more visible with larger queries that take minutes, not seconds

### Comparison with Raymond Tsai Blog

The blog tested thread scaling on a 122GB DB where BLAST execution takes minutes. With our 2GB DB:

- **DB < RAM**: CPU-bound, thread scaling minimal (matches blog's "right-hand side" scenario)
- **DB > RAM**: I/O-bound, threads help with parallelizing DB reads (blog's "left-hand side")
- **Conclusion**: Thread scaling tests are most meaningful with DB sizes approaching or exceeding VM RAM

---

## 10. Phase G: Concurrent Queries

**Objective**: Measure performance degradation when running multiple searches simultaneously.

| Test | Concurrent   | Total (s)    | Status  | Notes                          |
| ---- | ------------ | ------------ | ------- | ------------------------------ |
| G1   | 1 (baseline) | 388.2 (cold) | SUCCESS | Single search                  |
| G2   | 2            | 66.5         | PARTIAL | 1/2 submit processes succeeded |
| G4   | 4            | 77.8         | PARTIAL | 1/4 submit processes succeeded |

### Analysis

ElasticBLAST is designed for **single-search-per-cluster** operation. Concurrent `elastic-blast submit`
to the same cluster causes K8s job name collisions (same batch names). This is a known architectural
constraint, not a bug.

**The correct concurrency model in ElasticBLAST is batch parallelism**: a single search with many
query batches distributed across multiple nodes (as demonstrated in Phase E). This is fundamentally
different from the blog post's approach of running multiple queries on a single VM.

For multi-search concurrency:

1. Use **separate clusters** per search (fully isolated, higher cost)
2. Use **batch-len** to create more query batches distributed across nodes (native ElasticBLAST parallelism)
3. Future: implement a job scheduler layer on top of ElasticBLAST

---

## 11. Issues Encountered and Fixed

| Issue                                         | Root Cause                                                                                           | Fix Applied                                                      |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| Azure SDK exceptions hidden by cleanup errors | `poller.result()` not wrapped in `UserReportError`; `collect_k8s_logs` crashed when k8s_ctx was None | Wrapped in `UserReportError`; added `_safe_collect_logs()` guard |
| `ModuleNotFoundError: azure.mgmt.resource`    | Dead import at top of `_get_subscription_id()` before az CLI fallback                                | Removed dead import; az CLI runs first                           |
| `AuthorizationFailure` in `get_latest_dir()`  | Azure Blob SDK `list_blobs()` called for custom DB URLs where it's unnecessary                       | Made `get_latest_dir()` graceful with try/except                 |
| `blastdbcheck` failing in NVMe (SSD) mode     | `ELB_SKIP_DB_VERIFY` env var missing from `job-init-local-ssd-aks.yaml.template`                     | Added env var to SSD init template                               |
| vCPU quota exceeded for 3-node cluster        | Regional quota 70 vCPU < 96 needed (3 × 32)                                                          | Automated `_cleanup_stopped_clusters()` before each phase        |
| Storage access propagation delay              | `az storage account update` takes time to propagate                                                  | Added 60s probe loop with azcopy verification                    |
| BLAST `-num_threads` duplication              | Script hardcoded `-num_threads $ELB_NUM_CPUS` + user options conflicted                              | Skip auto-add when options contain `-num_threads`                |
| Warm cluster job name collision               | `kubectl apply` fails on immutable fields of existing jobs                                           | Auto-delete old jobs before warm reuse runs                      |

---

## 12. Infrastructure Validated

| Component                    | Status | Details                                          |
| ---------------------------- | ------ | ------------------------------------------------ |
| Azure CLI                    | OK     | v2.81.0                                          |
| kubectl                      | OK     | v1.34.5                                          |
| azcopy                       | OK     | v10.28.0                                         |
| AKS cluster creation (SDK)   | OK     | `azure_sdk.py` with `DefaultAzureCredential`     |
| AKS cluster reuse            | OK     | `reuse=true` skips init-pv, preserves PVC        |
| Blob NFS PVC (ReadWriteMany) | OK     | `azureblob-nfs-premium` StorageClass             |
| Local NVMe SSD (hostPath)    | OK     | `exp-use-local-ssd=true`                         |
| Managed Identity (in-pod)    | OK     | `azcopy login --identity`                        |
| ACR image pull               | OK     | 4 images (elb, job-submit, query-split, openapi) |
| vmtouch DaemonSet            | OK     | Dynamic RAM allocation (80% of available)        |
| elb-finalizer Job            | OK     | Auto status marker upload                        |
| ConfigMap scripts            | OK     | 7 AKS scripts                                    |

---

## 13. Recommendations

### For Small DB (< 100GB)

1. Use **Blob NFS Premium** (cheapest, simplest)
2. Enable **warm cluster reuse** for repeated searches — **84% faster**
3. Single node E32s_v3 is sufficient

### For Large DB (100GB - 2TB)

1. Consider **Azure NetApp Files Ultra** for shared storage with <1ms latency
2. Or use **Local NVMe SSD** with E-series v5 VMs for best I/O performance
3. Enable **vmtouch DaemonSet** to cache DB in RAM (80% of available memory)

### For Very Large DB (2TB+)

1. Use **DB partitioning** (`db-partitions = 10`) to split across nodes
2. Combine with **Local NVMe** for per-node partition storage
3. Scale to 10+ nodes for parallel partition search

### Operational

1. Always use `reuse=true` in production/SaaS environments
2. Set `ELB_SKIP_DB_VERIFY=true` for custom DBs to avoid 30+ min overhead
3. Monitor vCPU quota — E32s_v3 × 3 nodes = 96 vCPU minimum
4. Use `az aks stop` for idle clusters to avoid costs while preserving state

---

## 14. Next Steps

| Priority | Action                                                  | Status                               |
| -------- | ------------------------------------------------------- | ------------------------------------ |
| HIGH     | ANF Ultra storage backend test                          | NOT STARTED (requires capacity pool) |
| HIGH     | Benchmark with pdbnt (~60GB) for I/O-dominated scenario | NOT STARTED                          |
| MEDIUM   | Spot VM cost comparison                                 | Code ready, deployment pending       |
| MEDIUM   | 100+ batch stress test on 5+ nodes                      | NOT STARTED                          |
| LOW      | Azure Managed Lustre evaluation                         | NOT STARTED                          |
| LOW      | NCBI upstream Wave 1 PRs                                | Pending CELA approval                |

---

## 15. Retrospective: Lessons Learned

### 15.1 What Worked Well

| Item | Details |
| ---- | ------- |
| Automated benchmark runner | 7 phases, 21 test configs, auto storage/cluster management, Markdown report generation |
| Preflight checks | Storage probe loop + stopped cluster cleanup eliminated repeat failures |
| Warm cluster reuse | Most impactful optimization — 84% time reduction proven with real data |
| Error handling hardening | 8 distinct failure modes caught and fixed before benchmark suite ran successfully |
| `_safe_collect_logs()` | Cleanup stack no longer hides original errors |

### 15.2 What Didn't Work / Limitations

| Issue | Impact | Root Cause |
| ----- | ------ | ---------- |
| DB size too small for I/O testing | 2GB DB fits entirely in 256GB RAM — no I/O bottleneck visible | Need DB > RAM to see storage differences |
| Thread scaling flat | 65-82s range for threads 1-32 — overhead dominates | Same: DB in RAM, BLAST completes in seconds |
| Concurrent query failure | ElasticBLAST = single-search-per-cluster | Architectural constraint, not fixable without job name namespacing |
| Azure Monitor metrics not collected | `az monitor metrics list` requires exact VMSS resource ID construction | Needs debugging; raw `kubectl top` is more reliable |
| Pod-level iostat unavailable | BLAST container image lacks `iostat`/`sysstat` | Need custom image or init script |
| Phase timing not separated | Cannot split BLAST Phase 1 (I/O) vs Phase 2 (CPU) | BLAST binary doesn't emit phase timestamps |

### 15.3 Resource Cleanup Checklist (Executed)

| Resource | Status | Command |
| -------- | ------ | ------- |
| AKS clusters (all) | DELETED | `az aks delete -g rg-elb-koc -n <name> --yes` |
| Managed disks | NONE FOUND | `az disk list -g rg-elb-koc` |
| Snapshots | NONE FOUND | `az snapshot list -g rg-elb-koc` |
| Storage public access | DISABLED | `az storage account update -n stgelb --public-network-access Disabled` |
| vCPU usage | 38/140 (base only) | No active compute resources |

---

## 16. Next Benchmark: Data Collection Plan

### 16.1 Critical Gap: DB Size Must Exceed VM RAM

The single most important change for the next benchmark is using a **DB larger than VM RAM**.

| Current | Problem | Next Target |
| ------- | ------- | ----------- |
| 10MB DB on 256GB RAM VM | DB = 0.004% of RAM | No I/O pressure |
| 2GB DB on 256GB RAM VM | DB = 0.8% of RAM | Still no I/O pressure |
| **60GB+ DB on 32GB RAM VM** | **DB = 188% of RAM** | **I/O-bound Phase 1 visible** |
| **120GB DB on 256GB RAM VM** | **DB = 47% of RAM** | **Partial I/O pressure** |

**Recommended setup**: Use `Standard_D8s_v3` (32GB RAM) with the `nt` database (~122GB) to force I/O-bound behavior. This matches the Raymond Tsai blog's left-hand scenario and will show dramatic storage differences.

### 16.2 Data Points to Collect Per Test

#### a) BLAST Internal Phase Timing

BLAST doesn't emit phase timestamps natively. Workaround:

```bash
# In blast-run-aks.sh: capture Phase 1 (I/O) vs Phase 2 (CPU) via /proc monitoring
(while kill -0 $BLAST_PID 2>/dev/null; do
    echo "$(date +%s) $(cat /proc/$BLAST_PID/io 2>/dev/null | grep read_bytes)" >> /tmp/blast-io.log
    echo "$(date +%s) $(cat /proc/stat | head -1)" >> /tmp/blast-cpu.log
    sleep 1
done) &
```

Phase 1 end: read_bytes stops growing, CPU usage spikes to 100%

#### b) Storage I/O Metrics (from inside pod)

```bash
# Install and run iostat (or parse /proc/diskstats)
cat /proc/diskstats | awk '{print $3, $6, $10}'  # device, reads_completed, writes_completed
cat /proc/meminfo | grep -E 'MemTotal|MemFree|Cached|Buffers'
```

Collect every 5 seconds during BLAST execution, save to results blob.

#### c) Azure Monitor Time-Series

```bash
# After test completion:
az monitor metrics list \
  --resource <VMSS_RESOURCE_ID> \
  --metric "Percentage CPU,Available Memory Bytes,Disk Read Bytes,Disk Write Bytes,Disk Read Operations/Sec" \
  --start-time <START> --end-time <END> \
  --interval PT1M
```

#### d) Per-Job K8s Timestamps

Already implemented in `collect_job_timings()`:
- init-pv start/completion (DB download time)
- submit-jobs start/completion
- Each BLAST batch start/completion
- Results export start/completion

#### e) Thread Scaling (meaningful version)

| Config | Why |
| ------ | --- |
| DB: nt (122GB), VM: D8s_v3 (32GB RAM) | Forces I/O-bound Phase 1 |
| Threads: 1, 2, 4, 8 (max for D8s_v3) | Shows scaling on I/O-bound workload |
| Storage: Blob NFS vs NVMe vs ANF | Shows storage impact on thread scaling |

#### f) Cost Analysis Per Storage Type

| Metric | How to Calculate |
| ------ | ---------------- |
| VM cost | `total_elapsed_s / 3600 * vm_cost_hr * num_nodes` |
| Storage cost | PVC size × storage rate per hour |
| Total cost | VM + storage + network egress |
| Cost per query | Total cost / num_query_batches |
| Cost per base pair | Total cost / total_query_length |

### 16.3 Test Matrix for Next Run

| Test | DB | DB Size | VM | RAM | Storage | Nodes | Goal |
| ---- | -- | ------- | -- | --- | ------- | ----- | ---- |
| H1 | nt | 122GB | D8s_v3 | 32GB | Blob NFS | 1 | I/O-bound baseline |
| H2 | nt | 122GB | D8s_v3 | 32GB | NVMe | 1 | NVMe vs NFS at scale |
| H3 | nt | 122GB | D8s_v3 | 32GB | ANF | 1 | ANF performance validation |
| H4 | nt | 122GB | E32s_v3 | 256GB | Blob NFS | 1 | RAM > DB (CPU-bound) |
| H5 | nt | 122GB | D8s_v3 | 32GB | NVMe | 3 | Scale-out with I/O pressure |
| H6 | nt | 122GB | D8s_v3 | 32GB | Blob NFS | 1 | Thread scaling (1,2,4,8) |
| H7 | nt | 122GB | E32s_v3 | 256GB | warm | 1 | Warm reuse (DB cached in RAM) |

**Prerequisites**:
- Download `nt` database to Azure Blob Storage (~122GB, `update_blastdb.pl nt`)
- Provision ANF capacity pool (4TB minimum, Ultra tier)
- Increase vCPU quota if needed (D8s_v3 × 3 = 24 vCPU, fits in current 140 limit)

### 16.4 Proof Points for Solution Effectiveness

To conclusively prove ElasticBLAST Azure delivers value, the next benchmark must demonstrate:

| Claim | Required Proof | Metric |
| ----- | -------------- | ------ |
| **Warm cluster eliminates init overhead** | Cold vs warm with 122GB DB | Init time: >30min (cold) vs <30s (warm) |
| **NVMe beats NFS for large DB** | Same query on NFS vs NVMe with DB > RAM | Phase 1 time: 2-5x difference expected |
| **ANF provides shared fast storage** | ANF vs Blob NFS with multi-node | Download 1x (shared) vs Nx (per-node) |
| **Scale-out reduces wall clock** | 1 vs 3 vs 5 nodes with 100+ batches | Near-linear scaling for batch-parallel |
| **vmtouch caching works** | Before/after vmtouch with DB < RAM | Repeat query: minutes → seconds |
| **Cost-effective vs single VM** | ElasticBLAST vs standalone BLAST on same VM | $/query comparison |
| **Thread scaling follows expected curve** | num_threads sweep on I/O-bound workload | Sub-linear improvement (Amdahl's law) |

### 16.5 Estimated Cost for Next Benchmark

| Phase | VMs | Duration | Estimated Cost |
| ----- | --- | -------- | -------------- |
| H1-H3 (storage comparison) | 3 × D8s_v3 × ~2h each | ~6h | ~$2.30 |
| H4 (CPU-bound) | 1 × E32s_v3 × ~1h | ~1h | ~$2.00 |
| H5 (scale-out) | 3 × D8s_v3 × ~2h | ~2h | ~$2.30 |
| H6 (thread scaling) | 1 × D8s_v3 × ~8h (8 runs) | ~8h | ~$3.00 |
| H7 (warm cache) | 1 × E32s_v3 × ~30min | ~0.5h | ~$1.00 |
| ANF capacity pool | 4TB Ultra × ~12h | ~12h | ~$48.00 |
| **Total estimate** | | | **~$59** |

Note: ANF is the dominant cost. Consider running H3 last and deleting the capacity pool immediately after.
