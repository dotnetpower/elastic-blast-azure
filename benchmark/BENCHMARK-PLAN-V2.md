# ElasticBLAST Azure Benchmark Plan v2 — Production Readiness

> **Created**: 2026-04-20
> **Author**: Moon Hyuk Choi (moonchoi@microsoft.com)
> **Budget**: $200/day max
> **Region**: Korea Central
> **Baseline**: [Benchmark v1 Report](results/2026-04-18/report.md) (storage + scaling, completed 2026-04-18)
> **Customer context**: Pathogen detection service — 3 pathogens (SARS-CoV-2, MPXV, P. falciparum), 1-300 queries/request, multi-user

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
Axis 1: SKU Scale-Up ──────── "어떤 VM이 가장 빠른가?"
    │
    ▼ (최적 SKU 확정)
Axis 2: Query-Based Tuning ── "쿼리 규모별 최적 설정은?"
    │
    ▼ (추천 매핑 확정)
Axis 3: Multi-Request Service ─ "다수 사용자 동시 처리가 되는가?"
```

---

## 4. Axis 1: SKU Scale-Up — 단일 노드 최적 VM 식별

### 4.1 Objective

core_nt (~300 GB) 에서 **단일 노드 성능이 가장 좋은 VM SKU**를 식별합니다.
v1에서 확인된 결론(Local SSD > NFS)을 전제로, Local SSD 모드만 테스트합니다.

### 4.2 VM Candidates

| SKU                 | vCPU | RAM    | Temp Disk   | $/hr   | 선택 이유                            |
| ------------------- | ---- | ------ | ----------- | ------ | ------------------------------------ |
| Standard_E32s_v3    | 32   | 256 GB | 512 GB      | $2.016 | v1 baseline, **temp disk < core_nt** |
| Standard_E48s_v3    | 48   | 384 GB | 768 GB      | $3.024 | CPU 1.5x, temp disk 수용 가능?       |
| Standard_E64s_v3    | 64   | 432 GB | 1,024 GB    | $4.032 | CPU 2x, **core_nt 확실히 수용**      |
| Standard_E96s_v3    | 96   | 672 GB | 1,344 GB    | $6.048 | CPU 3x, 최고 E-series                |
| Standard_L32as_v3   | 32   | 256 GB | 3.8 TB NVMe | $2.496 | v1 NVMe baseline                     |
| Standard_L64as_v3   | 64   | 512 GB | 7.6 TB NVMe | $4.992 | NVMe + CPU 2x                        |
| Standard_HB120rs_v3 | 120  | 480 GB | 2×960 GB    | $3.600 | HPC, Tsai 2021 비교                  |

> **중요**: E32s_v3 temp disk = 512 GB인데 core_nt ≈ 300 GB. 수용은 되지만 여유가 적음.
> 쿼리/results 공간까지 고려하면 E48s_v3+ 또는 L-series가 안전.

### 4.3 Pre-check: DB Size Verification

core_nt 정확한 크기를 먼저 확인해야 합니다. NCBI에서 다운로드 전 volume 수 확인:

```bash
# S3에서 core_nt volume 목록 확인
aws s3 ls s3://ncbi-blast-databases/ --no-sign-request | grep "core_nt\." | head -20

# 또는 NCBI FTP에서 확인
curl -s https://ftp.ncbi.nlm.nih.gov/blast/db/ | grep "core_nt\." | head -20
```

만약 core_nt > 500 GB이면 E32s_v3는 제외하고 E64s_v3+ / L-series만 테스트합니다.

### 4.4 Test Matrix

**DB**: core_nt, Local SSD mode (`exp-use-local-ssd = true`)
**Queries**: pathogen-10.fa (1 batch) + pathogen-300.fa (9 batches)

| Test ID    | SKU        | Nodes | Query        | batches | 측정 목표                      |
| ---------- | ---------- | ----- | ------------ | ------- | ------------------------------ |
| A1-E32-10  | E32s_v3    | 1     | pathogen-10  | 1       | E32 baseline (DB 수용 가능 시) |
| A1-E48-10  | E48s_v3    | 1     | pathogen-10  | 1       | CPU 1.5x 효과                  |
| A1-E64-10  | E64s_v3    | 1     | pathogen-10  | 1       | CPU 2x 효과                    |
| A1-E96-10  | E96s_v3    | 1     | pathogen-10  | 1       | CPU 3x 효과                    |
| A1-L32-10  | L32as_v3   | 1     | pathogen-10  | 1       | NVMe baseline                  |
| A1-L64-10  | L64as_v3   | 1     | pathogen-10  | 1       | NVMe + CPU 2x                  |
| A1-HB-10   | HB120rs_v3 | 1     | pathogen-10  | 1       | HPC (Tsai 비교)                |
| A1-E32-300 | E32s_v3    | 1     | pathogen-300 | 9       | multi-batch single node        |
| A1-E64-300 | E64s_v3    | 1     | pathogen-300 | 9       | multi-batch scale-up           |
| A1-L32-300 | L32as_v3   | 1     | pathogen-300 | 9       | multi-batch NVMe               |
| A1-HB-300  | HB120rs_v3 | 1     | pathogen-300 | 9       | multi-batch HPC                |

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

1. **SKU Performance Chart**: bar chart — SKU별 median per-batch time
2. **SKU Cost-Efficiency Chart**: scatter — cost/run vs median time
3. **CPU Scaling Chart**: line chart — vCPU count vs speedup (linear 대비)
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

## 5. Axis 2: Query-Based Tuning — 쿼리 규모별 최적 설정

### 5.1 Objective

쿼리 수 (10-3,000)에 따라 **(SKU, 노드수, batch-len, mem-limit)** 최적 조합을 도출하고,
`azure_optimizer.py`에 자동 추천 로직을 구현합니다.

### 5.2 Dependencies

- Axis 1에서 **최적 SKU 2-3종**이 결정된 후 진행
- 가정: Axis 1 결과로 `E64s_v3` (또는 유사)가 선정됨

### 5.3 Test Matrix

**DB**: core_nt, Local SSD mode
**SKU**: Axis 1 최적 SKU (예: E64s_v3) + v1 baseline (E32s_v3)

| Test ID    | Query         | Bases  | batches | SKU      | Nodes | 목적                    |
| ---------- | ------------- | ------ | ------- | -------- | ----- | ----------------------- |
| A2-10-1N   | pathogen-10   | 37 KB  | 1       | best_sku | 1     | 최소 쿼리 baseline      |
| A2-50-1N   | pathogen-50   | 150 KB | 2       | best_sku | 1     | 소규모                  |
| A2-100-1N  | pathogen-100  | 300 KB | 3       | best_sku | 1     | 중규모, 1N              |
| A2-100-3N  | pathogen-100  | 300 KB | 3       | best_sku | 3     | 중규모, 3N (분산 효과?) |
| A2-300-1N  | pathogen-300  | 900 KB | 9       | best_sku | 1     | 최대 단일요청, 1N       |
| A2-300-3N  | pathogen-300  | 900 KB | 9       | best_sku | 3     | 최대 단일요청, 3N       |
| A2-300-5N  | pathogen-300  | 900 KB | 9       | best_sku | 5     | 최대 단일요청, 5N       |
| A2-1000-1N | pathogen-1000 | 3 MB   | 30      | best_sku | 1     | 멀티유저 시뮬           |
| A2-1000-3N | pathogen-1000 | 3 MB   | 30      | best_sku | 3     | 멀티유저, 3N            |
| A2-1000-5N | pathogen-1000 | 3 MB   | 30      | best_sku | 5     | 멀티유저, 5N            |
| A2-3054-3N | gut-3054      | 3.1 MB | 31      | best_sku | 3     | v1 비교 (DB만 다름)     |

**Total**: 11 tests

### 5.4 Measurement

v1과 동일 + 추가:

| Metric            | Method                                           |
| ----------------- | ------------------------------------------------ |
| batch 생성 수     | ElasticBLAST submit 로그                         |
| pod 스케줄링 지연 | pod creationTimestamp → startTime                |
| 유휴 노드 시간    | (batch 수 < node × pod/node) 시 일부 노드 미사용 |

### 5.5 Expected Output

1. **Query-Scale Heatmap**: (쿼리 수 × 노드 수) → wall-clock time
2. **Cost-Efficiency Table**: 각 조합의 cost/run
3. **Recommendation Matrix**:

```
| 쿼리 수 | 추천 노드 | 추천 SKU | 예상 시간 | 예상 비용 |
|---------|----------|---------|----------|----------|
| 1-50    | 1        | E64s    | X min    | $Y       |
| 50-100  | 1-3      | E64s    | X min    | $Y       |
| 100-300 | 3        | E64s    | X min    | $Y       |
| 300+    | 3-5      | E64s    | X min    | $Y       |
```

4. **`azure_optimizer.py` 코드**: `recommend_config()` 함수 구현

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

다수 사용자가 동시에 BLAST 요청을 보내는 **서비스 시나리오**를 검증합니다.
핵심: `reuse = true` 상시 클러스터에서 **큐 기반 다중 요청 처리** + **오토스케일링**.

### 6.2 Dependencies

- Axis 1, 2 완료 → 최적 SKU와 노드 설정 확정
- `reuse = true` 기능 검증
- 큐 연동 Worker 구현 (신규 개발)

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

#### Step 1: `reuse = true` 검증 (기존 기능)

```bash
# 첫 번째 submit — 클러스터 생성 (5-15 min)
elastic-blast submit --cfg bench-v2-reuse.ini

# 두 번째 submit — 클러스터 재사용 (즉시 시작)
elastic-blast submit --cfg bench-v2-reuse-2nd.ini

# 측정: 첫 submit E2E vs 두 번째 submit E2E
```

#### Step 2: 순차 다중 요청 스크립트

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

#### Step 3: 동시 다중 요청 (병렬 submit)

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

> **Known limitation**: 현재 ElasticBLAST는 동일 클러스터에서 동시 submit을 지원하지 않을 수 있음.
> 이 경우 Job namespace 분리 또는 큐 기반 순차 처리가 필요.

#### Step 4: Queue Worker (신규 개발 — 필요 시)

```python
# src/elastic_blast/queue_worker.py (향후 구현)
# Azure Queue Storage에서 메시지를 polling하여
# elastic-blast submit을 순차 실행하는 Worker
```

### 6.5 Test Matrix

| Test ID     | Scenario         | Requests | Queries/req | 동시성 | 클러스터        | 측정               |
| ----------- | ---------------- | -------- | ----------- | ------ | --------------- | ------------------ |
| A3-reuse    | reuse 검증       | 2 순차   | 10          | 1      | 신규→재사용     | 재사용 시 E2E 지연 |
| A3-seq5     | 순차 5 요청      | 5 순차   | 10          | 1      | 상시            | 평균 E2E           |
| A3-seq5-300 | 순차 5 × 300쿼리 | 5 순차   | 300         | 1      | 상시            | throughput         |
| A3-con3     | 동시 3 요청      | 3 동시   | 100         | 3      | 상시, 3N        | 간섭 효과          |
| A3-con10    | 동시 10 요청     | 10 동시  | 100         | 10     | 상시, 5N        | 최대 부하          |
| A3-burst    | Burst 부하       | 20/min   | 10          | burst  | 상시, autoscale | 스케일링 반응      |

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

**Gate**: Axis 1 결과로 최적 SKU 2-3종 선정 → Axis 2 진행 결정

### Day 2: Axis 2 — Query-Based Tuning (4-6 hours)

| #   | Task                      | Tests                              | Est. Time |
| --- | ------------------------- | ---------------------------------- | --------- |
| 2.1 | Single-node query scaling | A2-10/50/100/300/1000-1N           | 2 hr      |
| 2.2 | Multi-node query scaling  | A2-100/300/1000-3N, A2-300/1000-5N | 2 hr      |
| 2.3 | v1 comparison (core_nt)   | A2-3054-3N                         | 30 min    |
| 2.4 | Data analysis             | Heatmap, recommendation table      | 1 hr      |

**Gate**: Recommendation matrix 완성 → Axis 3 진행

### Day 3-4: Axis 3 — Multi-Request Service (6-10 hours)

| #   | Task                     | Tests                      | Est. Time |
| --- | ------------------------ | -------------------------- | --------- |
| 3.1 | `reuse=true` 검증        | A3-reuse                   | 30 min    |
| 3.2 | Sequential multi-request | A3-seq5, A3-seq5-300       | 3 hr      |
| 3.3 | Concurrent multi-request | A3-con3, A3-con10          | 2 hr      |
| 3.4 | Burst/autoscale test     | A3-burst                   | 1.5 hr    |
| 3.5 | Data analysis            | Latency charts, throughput | 1 hr      |

### Day 5: Report (3-4 hours)

| #   | Task                                           | Est. Time |
| --- | ---------------------------------------------- | --------- |
| 5.1 | v2 report draft                                | 2 hr      |
| 5.2 | Charts generation (`create_charts_v2.py` 확장) | 1 hr      |
| 5.3 | Customer-facing summary (1-pager)              | 30 min    |
| 5.4 | `azure_optimizer.py` 코드 업데이트             | 30 min    |

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
