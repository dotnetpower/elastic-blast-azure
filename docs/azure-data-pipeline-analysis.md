# ElasticBLAST Azure 데이터 파이프라인 완전 분석

## 개요

ElasticBLAST Azure는 BLAST 검색을 Azure Kubernetes Service(AKS)에서 분산 실행하는 시스템입니다.
이 문서는 `elastic-blast submit` 명령 실행부터 결과 수집까지의 전체 데이터 흐름을 분석합니다.

---

## 1. 전체 파이프라인 흐름도

```
[사용자 로컬] → [Azure Blob Storage] → [AKS 클러스터] → [Azure Blob Storage]
     │                 │                     │                    │
  config.ini      query_batches/        PV or hostPath          results/
  queries.fa      metadata/            /blast/blastdb          *.out.gz
                  job.yaml.template    /blast/queries           logs/
```

### 전체 단계 요약

```
1. Config 로딩 및 검증
2. Query 분할 (Client 또는 Cloud)
3. Blob Storage 업로드 (query batches + metadata)
4. AKS 클러스터 생성 + IAM 설정
5. 스토리지 초기화 (PV 또는 Local SSD)
   5a. BLAST DB 다운로드
   5b. Query batches 복사
6. BLAST 작업 제출 (직접 또는 Cloud Job Submission)
7. BLAST 실행 (각 Pod에서)
   7a. DB를 RAM에 캐시
   7b. BLAST 검색 수행
   7c. 결과를 Blob Storage로 업로드
8. 결과 수집 및 클린업
```

---

## 2. 단계별 상세 분석

### Stage 1: 설정 로딩 및 검증

**파일**: `src/elastic_blast/commands/submit.py` L126-L134, `src/elastic_blast/elb_config.py` L287-L370

```
elastic-blast submit --cfg config.ini
    ↓
submit(args, cfg, clean_up_stack)
    ↓
cfg.validate(ElbCommand.SUBMIT, dry_run)
```

**핵심 설정 클래스**: `AZUREConfig` (elb_config.py L287)

- `region`: Azure 리전 (기본값: `eastus`)
- `resourcegroup`: AKS 리소스 그룹
- `storage_account` / `storage_account_container`: Blob Storage 접근 (Managed Identity 사용)
- `acr_name` / `acr_resourcegroup`: Azure Container Registry 접근
- `elb_job_id`: `job-{uuid4().hex}` 형식의 고유 작업 ID (L305)
- `elb_docker_image`: `elbacr.azurecr.io/ncbi/elb:1.4.0` (constants.py L247)

**Azure 전용 경로 구조**:

```
{results_url}/{elb_job_id}/metadata/    ← 메타데이터
{results_url}/{elb_job_id}/query_batches/  ← 분할된 쿼리
{results_url}/{elb_job_id}/logs/        ← 실행 로그
{results_url}/{elb_job_id}/             ← BLAST 결과 (.out.gz)
```

---

### Stage 2: Query 분할 모드 결정 및 실행

**파일**: `src/elastic_blast/commands/submit.py` L62-L93 (`get_query_split_mode`), L256-L287 (`split_query`)

#### 분할 모드 판별 로직 (L62-L93):

| 조건                                     | 모드              | 설명                    |
| ---------------------------------------- | ----------------- | ----------------------- |
| `ELB_USE_CLIENT_SPLIT` 환경변수 설정     | `CLIENT`          | 강제 클라이언트 분할    |
| Azure blob URL 1개 + 파일 크기 < 임계값  | `CLIENT`          | 작은 파일은 로컬 분할   |
| Azure blob URL 1개 + 파일 크기 >= 임계값 | `CLOUD_TWO_STAGE` | 큰 파일은 클라우드 분할 |
| 파일 여러 개 또는 로컬 파일              | `CLIENT`          | 클라이언트 분할         |

- 압축 파일 임계값: 5MB (`ELB_DFLT_MIN_QUERY_FILESIZE_TO_SPLIT_ON_CLIENT_COMPRESSED`, constants.py L123)
- 비압축 파일 임계값: 20MB (`ELB_DFLT_MIN_QUERY_FILESIZE_TO_SPLIT_ON_CLIENT_UNCOMPRESSED`, constants.py L125)

#### CLIENT 모드 (L256-L287):

```
query_files → FASTAReader.read_and_cut() → batch_000.fa, batch_001.fa, ...
    ↓
open_for_write(blob_path) → 로컬 임시 디렉토리에 작성
    ↓
[Stage 3에서 copy_to_bucket()으로 Blob Storage 업로드]
```

- 분할된 배치들은 `{results}/{elb_job_id}/query_batches/batch_XXX.fa` 경로로 저장
- 배치 개수가 동시 실행 가능 Job 수보다 적으면 `batch_len`을 자동 조정하여 재분할

#### CLOUD_TWO_STAGE 모드:

- 클러스터 생성 후 init 작업의 `import-query-batches` 컨테이너가 쿼리 분할 수행
- `run.sh -i {INPUT_QUERY} -o {ELB_RESULTS} -b {BATCH_LEN} -c 0 -q /blast/queries/`

---

### Stage 3: Blob Storage 업로드

**파일**: `src/elastic_blast/elasticblast.py` L99-L113 (`upload_workfiles`), `src/elastic_blast/filehelper.py` L129-L176 (`copy_to_bucket`)

```
upload_workfiles()
    ↓
copy_to_bucket(dry_run, sas_token)
    ↓
azcopy cp {tempdir}/* {blob_url}?{sas_token} --recursive=true
```

**업로드 대상**:

1. **설정 파일** → `{results}/{elb_job_id}/metadata/elastic-blast-config.json` (submit.py L112-L118)
2. **Query 길이** → `{results}/{elb_job_id}/metadata/query_length.txt` (azure.py L115-L120)
3. **Query 배치들** → `{results}/{elb_job_id}/query_batches/batch_*.fa` (filehelper.py L143-L148)
4. **Job 템플릿** (cloud_job_submission시) → `{results}/{elb_job_id}/metadata/job.yaml.template` (azure.py L355-L359)

**시간 소요**: 데이터 크기에 비례. `azcopy`를 사용하여 병렬 업로드.

---

### Stage 4: AKS 클러스터 생성

**파일**: `src/elastic_blast/azure.py` L325-L397 (`_initialize_cluster`), L1066-L1173 (`start_cluster`)

#### 4a. 클러스터 상태 확인 (L366):

```python
aks_status = check_cluster(cfg)  # az aks list로 확인
```

#### 4b. 클러스터 생성 (reuse=false 또는 클러스터 미존재 시) (L1066-L1173):

```
az aks create
    --resource-group {rg}
    --name {cluster_name}
    --node-vm-size {machine_type}  # 기본: Standard_E32s_v3
    --node-count {num_nodes}
    --enable-cluster-autoscaler
    --min-count {num_nodes}
    --max-count {num_nodes*3}
    --enable-managed-identity
    --enable-blob-driver          # PV 모드일 때만
    --tags {labels}
```

- **타임아웃**: 1800초 (30분) (L1168)
- **PV 모드**: `--enable-blob-driver` 추가로 Azure Blob CSI 드라이버 활성화
- **Local SSD 모드**: blob 드라이버 불필요

#### 4c. 클러스터 자격증명 획득 (L849-L868):

```
az aks get-credentials --resource-group {rg} --name {name} --overwrite-existing
kubectl config current-context  → k8s_ctx
```

#### 4d. 노드 레이블링 (L404-L420):

- Local SSD 모드에서만 사용
- 각 노드에 `ordinal=0`, `ordinal=1`, ... 레이블 부여
- init-ssd Job이 특정 노드에 스케줄링되도록 nodeSelector에서 사용

#### 4e. IAM 역할 할당 (L873-L1022):

```
1. Storage Account ID 조회   → az storage account show
2. AKS kubelet identity 조회 → az aks show --query identityProfile.kubeletidentity.clientId
3. Storage Blob Data Contributor 역할 할당 → az role assignment create
4. ACR Pull 역할 할당        → az role assignment create
5. Contributor 역할 할당     → subscription 레벨
```

---

### Stage 5: 스토리지 초기화

**파일**: `src/elastic_blast/kubernetes.py` L505-L513 (`initialize_storage`), L516-L647 (`initialize_local_ssd`), L650-L885 (`initialize_persistent_disk`)

두 가지 접근 방식이 있으며, `cfg.cluster.use_local_ssd` 설정으로 결정됩니다.

#### 방식 A: Persistent Volume (PV) 기반 (`use_local_ssd = false`)

**템플릿**: `templates/pvc-rwm-aks.yaml.template`, `templates/job-init-pv-aks.yaml.template`

```
[1] PVC 생성 (ReadWriteMany)
    ↓ azureblob-nfs-premium StorageClass
    ↓ blast-dbs-pvc-rwm
[2] init-pv Job 실행
    ├── Container: get-blastdb
    │   ├── azcopy login --identity
    │   ├── NCBI DB: update_blastdb.pl {db} --decompress
    │   └── Custom DB: azcopy cp {db_path} .
    └── Container: import-query-batches
        └── run.sh → query_batches를 PV로 복사
[3] PVC Bound 대기
[4] PVC 스냅샷 생성 (선택적)
```

**PVC 명세** (pvc-rwm-aks.yaml.template):

```yaml
accessModes: ReadWriteMany
storageClassName: azureblob-nfs-premium
storage: ${ELB_PD_SIZE} # 기본: 3000Gi
```

**init-pv Job** (job-init-pv-aks.yaml.template):

- 2개의 컨테이너가 **병렬 실행**:
  - `get-blastdb`: BLAST DB 다운로드 → `/blast/blastdb` (PV 마운트)
  - `import-query-batches`: 쿼리 배치 복사 → `/blast/queries` (PV 마운트)
- `azcopy login --identity`: Managed Identity로 인증
- Custom DB: `azcopy cp {blob_url} .` → 직접 복사
- NCBI DB: `update_blastdb.pl` → NCBI FTP/cloud에서 다운로드
- taxdb도 별도 다운로드
- **backoffLimit: 9** (최대 9회 재시도)

#### 방식 B: Local SSD 기반 (`use_local_ssd = true`)

**템플릿**: `templates/job-init-local-ssd-aks.yaml.template`

```
[1] DaemonSet: create-workspace
    → 모든 노드에 /workspace 디렉토리 생성
[2] init-ssd-{n} Job (노드 개수만큼 생성)
    ├── Container: get-blastdb
    │   ├── azcopy login --identity
    │   ├── DB 다운로드 → /blast/blastdb (hostPath /workspace)
    │   └── taxdb 다운로드
    └── Container: import-query-batches
        └── query_batches를 hostPath로 복사
```

**핵심 차이점**:

- `hostPath: /workspace` 사용 (노드 로컬 디스크)
- `nodeSelector: ordinal: "${NODE_ORDINAL}"` → 각 노드에 1개씩 Job 할당
- 모든 노드에서 **동시에** DB 다운로드 발생 (N개 노드 × DB 크기)
- DaemonSet이 `/workspace` 디렉토리를 미리 생성

**시간 소요**:

- DB 크기에 따라 수 분 ~ 수십 분 소요
- Local SSD: N개 노드 × 다운로드 시간 (네트워크 대역폭 병목)
- PV: 1회 다운로드 + NFS 공유 (디스크 I/O 병목)

---

### Stage 6: BLAST 작업 제출

**파일**: `src/elastic_blast/azure.py` L130-L184 (`submit`), `src/elastic_blast/kubernetes.py` L1030-L1089 (`submit_job_submission_job`)

두 가지 제출 방식이 있습니다:

#### 방식 A: Cloud Job Submission (기본)

`cloud_job_submission = True` (환경변수 `ELB_DISABLE_JOB_SUBMISSION_ON_THE_CLOUD` 미설정 시)

```
[1] job.yaml.template 생성 + Blob Storage 업로드
    (azure.py L352-L359)
[2] submit-jobs Job 제출
    (kubernetes.py L1030-L1089)
    → job-submit-jobs-aks.yaml.template 사용
[3] submit-jobs Pod 내에서:
    → cloud-job-submit-aks.sh 실행
    ↓ setup jobs 완료 대기 (L72-L80)
    ↓ batch_list.txt + job.yaml.template 다운로드 (L154-L155)
    ↓ envsubst로 JOB_NUM 치환하여 개별 Job YAML 생성
    ↓ kubectl apply -f jobs/{dir}/ (100개씩 배치) (L177-L184)
    ↓ num_jobs_submitted.txt 업로드
```

**submit-jobs Pod 환경변수** (job-submit-jobs-aks.yaml.template):

- `ELB_RESULTS`: 결과 Blob URL
- `ELB_CLUSTER_NAME`: 클러스터 이름
- `ELB_NUM_NODES`: 노드 수
- `ELB_USE_LOCAL_SSD`: Local SSD 사용 여부
- `ELB_AZURE_RESOURCE_GROUP`: 리소스 그룹

#### 방식 B: 직접 제출 (`ELB_DISABLE_JOB_SUBMISSION_ON_THE_CLOUD` 설정 시)

```python
_generate_and_submit_jobs(query_batches)  # azure.py L465-L498
    ↓
write_job_files(job_path, ...)  # 로컬에서 Job YAML 생성
    ↓
kubernetes.submit_jobs(k8s_ctx, Path(job_path))  # kubectl apply
```

---

### Stage 7: BLAST 실행 (각 Pod)

**파일**: `templates/blast-batch-job-aks.yaml.template` (PV), `templates/blast-batch-job-local-ssd-aks.yaml.template` (SSD)

#### PV 모드 (blast-batch-job-aks.yaml.template)

각 BLAST Job은 3단계로 구성됩니다:

```yaml
# Pod 구조:
volumes:
  - blast-dbs: PVC blast-dbs-pvc-rwm (ReadWriteMany)

initContainers:
  [1] load-blastdb-into-ram:   # DB를 RAM에 캐시
      → vmtouch -tqm 5G (페이지 캐시에 로드)
      → /blast/blastdb (PV 마운트)

containers:
  [2] blast:                    # BLAST 검색 실행
      → {program} -db {db} -query /blast/blastdb/batch_{N}.fa
        -out /blast/blastdb/results/batch_{N}-{program}-{db}.out
        -num_threads {num_cpus}
      → gzip 결과 → BLAST_EXIT_CODE.out 기록

  [3] results-export:           # 결과 업로드
      → BLAST_EXIT_CODE.out 대기 (polling)
      → azcopy cp BLASTDB_LENGTH.out → {results}/metadata/
      → azcopy cp BLAST_RUNTIME-{N}.out → {results}/logs/
      → azcopy cp batch_{N}-*.out.gz → {results}/
```

**리소스 제한**:

```yaml
requests:
  memory: ${ELB_MEM_REQUEST}
  cpu: ${ELB_NUM_CPUS_REQ} # azure.py L435: (nodes*cpus //4) -2
limits:
  memory: ${ELB_MEM_LIMIT}
  cpu: ${ELB_NUM_CPUS} # 기본: 16
```

**DB → RAM 캐시** (initContainer):

```bash
blastdb_path -dbtype {mol_type} -db {db} -getvolumespath | \
    tr ' ' '\n' | parallel vmtouch -tqm 5G
```

- `vmtouch`로 DB 파일을 Linux 페이지 캐시에 적극 로드
- 후속 BLAST 검색에서 디스크 I/O 최소화

**결과 업로드** (sidecar container):

- `blast` 컨테이너와 **동시 실행** (sidecar 패턴)
- `BLAST_EXIT_CODE.out` 파일이 생길 때까지 1초 간격 polling
- `azcopy login --identity` → Managed Identity 인증
- 실패 시 `FAILURE.txt`를 metadata에 업로드

#### Local SSD 모드 (blast-batch-job-local-ssd-aks.yaml.template)

```yaml
volumes:
  - blast-dbs: hostPath /workspace       # 노드 로컬
  - shared-data: emptyDir                # Pod 내 공유

initContainers:
  [1] import-query-batches:
      → azcopy cp {results}/query_batches/batch_{N}.fa /shared/requests/

containers:
  [2] blast:
      → /blast/blastdb (hostPath, DB 이미 있음)
      → -query /shared/requests/batch_{N}.fa (emptyDir)
      → -out /shared/results/batch_{N}-*.out

  [3] results-export:
      → /shared/results/ 에서 Blob Storage로 업로드
```

**핵심 차이점**:
| 항목 | PV 모드 | Local SSD 모드 |
|------|---------|---------------|
| DB 위치 | PV (NFS) | hostPath `/workspace/blast` |
| Query 위치 | PV | emptyDir (initContainer에서 복사) |
| DB 캐싱 | vmtouch (initContainer) | 불필요 (로컬 디스크) |
| Query 복사 | init 단계에서 PV로 한번 | **매 Pod마다** Blob→emptyDir |
| 장점 | DB 1회 다운로드 | 빠른 디스크 I/O |
| 단점 | NFS 오버헤드 | 노드별 DB 다운로드, 쿼리 매번 복사 |

---

### Stage 8: 결과 수집

**결과물 위치** (Blob Storage):

```
{results}/{elb_job_id}/
├── batch_000-blastx-swissprot.out.gz     # BLAST 결과 (gzip)
├── batch_001-blastx-swissprot.out.gz
├── ...
├── metadata/
│   ├── elastic-blast-config.json          # 설정 스냅샷
│   ├── query_length.txt                   # 총 쿼리 길이
│   ├── num_jobs_submitted.txt             # 제출된 Job 수
│   ├── BLASTDB_LENGTH.out                 # DB 크기
│   ├── SUCCESS.txt 또는 FAILURE.txt       # 최종 상태
│   └── disk-id.txt                        # PV 디스크 ID
├── logs/
│   ├── BLAST_RUNTIME-000.out              # 각 배치 실행시간
│   ├── BLAST_RUNTIME-001.out
│   ├── k8s-setup-get-blastdb.log          # 설정 로그
│   └── k8s-submit-submit-jobs.log
└── query_batches/
    ├── batch_000.fa                        # 분할된 쿼리
    ├── batch_001.fa
    └── ...
```

---

## 3. 데이터 이동 경로

```
┌──────────────┐     azcopy cp      ┌───────────────────┐
│  사용자 로컬  │ ─────────────────→ │  Azure Blob Storage │
│              │                     │  (Storage Account)  │
│  queries.fa  │  split_query()      │                     │
│  config.ini  │  → batch_XXX.fa     │  /{job_id}/         │
└──────────────┘                     │    query_batches/   │
                                     │    metadata/        │
                                     └─────────┬───────────┘
                                               │
                      ┌────────────────────────┤
                      │ init-pv / init-ssd      │ results-export
                      │ (download)              │ (upload)
                      ↓                         │
              ┌───────────────┐                 │
              │  AKS 클러스터   │                │
              │               │                 │
              │ ┌───────────┐ │                 │
              │ │  PV/NFS   │ │  또는            │
              │ │ /blast/   │ │  hostPath        │
              │ │  blastdb/ │ │  /workspace/     │
              │ │  queries/ │ │                  │
              │ └─────┬─────┘ │                  │
              │       │       │                  │
              │  ┌────┴────┐  │           ┌──────┴──────┐
              │  │ Pod N   │  │           │  Blob Storage │
              │  │         │  │  azcopy   │  /{job_id}/   │
              │  │ blast   │──┼──────────→│  *.out.gz     │
              │  │ -query  │  │           │  logs/        │
              │  │ -db     │  │           │  metadata/    │
              │  └─────────┘  │           └─────────────┘
              └───────────────┘
```

---

## 4. 시간 소모 작업 분석

### 병목 지점

| 단계                  | 작업                                 | 예상 시간     | 병목 원인                       |
| --------------------- | ------------------------------------ | ------------- | ------------------------------- |
| **AKS 생성**          | `az aks create`                      | 5-15분        | Azure 인프라 프로비저닝         |
| **IAM 설정**          | `az role assignment create` (3회)    | 1-3분         | Azure AD 전파                   |
| **DB 다운로드 (PV)**  | `update_blastdb.pl` 또는 `azcopy cp` | 5-30분+       | DB 크기 (nt: ~80GB, nr: ~100GB) |
| **DB 다운로드 (SSD)** | 노드 N개 × DB 다운로드               | 5-30분+       | N배의 네트워크 대역폭 사용      |
| **PVC Snapshot**      | blob storage 스냅샷                  | 1-5분         | PV 모드에서만                   |
| **Query 업로드**      | `azcopy cp` split batches            | 크기 비례     | 네트워크 대역폭                 |
| **BLAST 실행**        | `blastx`, `blastn` 등                | 수 분~수 시간 | CPU/메모리 집약                 |
| **결과 업로드**       | Pod당 `azcopy cp`                    | 수 초~수 분   | 결과 크기 비례                  |

### 특히 비용이 큰 작업

1. **DB 다운로드 (가장 큰 병목)**
   - PV 모드: 1회 다운로드이지만 NFS 읽기 지연
   - SSD 모드: N개 노드 각각 다운로드 → 네트워크 비용 N배
   - `nt` DB (~80GB) → 3노드 SSD = ~240GB 다운로드

2. **Per-Pod DB Loading** (PV 모드)
   - 각 BLAST Pod의 initContainer에서 `vmtouch`로 DB를 RAM에 캐시
   - DB가 크면 (>100GB) 이 과정도 수 분 소요
   - 하지만 이미 다른 Pod가 캐시했다면 빠름

3. **Per-Pod Query 복사** (SSD 모드)
   - 매 Pod의 initContainer에서 `azcopy cp`로 해당 배치 다운로드
   - 소규모 파일이므로 빠르지만, Pod 수 × 요청 수

---

## 5. PV vs Local SSD 상세 비교

### Persistent Volume (Azure Blob NFS Premium)

```
장점:
✅ DB를 1회만 다운로드
✅ 모든 Pod이 동시에 같은 PV 접근 (ReadWriteMany)
✅ 큰 DB에서 네트워크 비용 절약
✅ Query가 PV에 미리 복사되어 있음

단점:
❌ NFS 프로토콜 오버헤드 (latency 높음)
❌ vmtouch로 RAM 캐시 필요 (initContainer 시간 소요)
❌ Azure Blob NFS는 Premium tier 비용
❌ PVC 생성/바인딩 대기 시간
```

### Local SSD (hostPath /workspace)

```
장점:
✅ 로컬 디스크 I/O → 최고 성능
✅ NFS 오버헤드 없음
✅ vmtouch 불필요

단점:
❌ 노드마다 DB 전체를 다운로드 (N배 시간/비용)
❌ 매 Pod마다 query batch를 Blob에서 복사 (initContainer)
❌ 노드 장애 시 데이터 유실
❌ 오토스케일링 제한적 (새 노드에 DB 없음)
```

---

## 6. BLAST DB 로딩 메커니즘

### Stage 5에서: DB 다운로드

**PV 모드** (job-init-pv-aks.yaml.template):

```bash
azcopy login --identity
# NCBI DB:
update_blastdb.pl ${ELB_DB} --decompress --source NCBI
# Custom DB:
azcopy cp '${ELB_DB_PATH}' .   # blob → PV
# tar.gz면 압축 해제
[ -f ${ELB_DB}.tar.gz ] && tar xzf ${ELB_DB}.tar.gz
# DB 검증:
blastdbcmd -info -db ${ELB_DB} -dbtype ${ELB_DB_MOL_TYPE}
blastdbcheck -db ${ELB_DB} -dbtype ${ELB_DB_MOL_TYPE} -no_isam -ends 5
# taxdb도 다운로드:
update_blastdb.pl taxdb --decompress --source NCBI
```

**SSD 모드** (job-init-local-ssd-aks.yaml.template):

- 동일한 과정이지만 `hostPath: /workspace`에 저장
- 각 노드에서 `nodeSelector: ordinal: "${NODE_ORDINAL}"`로 개별 실행

### Stage 7에서: DB RAM 캐시

**PV 모드 전용** (blast-batch-job-aks.yaml.template, initContainer):

```bash
blastdb_path -dbtype ${ELB_DB_MOL_TYPE} -db ${ELB_DB} -getvolumespath | \
    tr ' ' '\n' | \
    parallel vmtouch -tqm 5G
```

- `blastdb_path`: DB alias에서 실제 볼륨 파일 경로 목록 추출
- `parallel vmtouch -tqm 5G`: 각 볼륨 파일을 5GB 단위로 RAM에 로드
  - `-t`: touch (페이지 캐시에 로드)
  - `-q`: quiet
  - `-m 5G`: 최대 5GB/파일

**SSD 모드**: initContainer가 `import-query-batches`만 수행 (DB는 이미 로컬)

---

## 7. 결과 업로드 메커니즘

**PV 모드** (blast-batch-job-aks.yaml.template, `results-export` container):

```bash
# BLAST 완료 대기 (sidecar 패턴)
until [ -s /blast/blastdb/results/BLAST_EXIT_CODE.out ]; do
    sleep 1
done

azcopy login --identity
azcopy cp /blast/blastdb/results/BLASTDB_LENGTH.out ${ELB_RESULTS}/metadata/
azcopy cp /blast/blastdb/results/BLAST_RUNTIME-${JOB_NUM}.out ${ELB_RESULTS}/logs/
azcopy cp /blast/blastdb/results/batch_${JOB_NUM}-*.out.gz ${ELB_RESULTS}/
exit `cat /blast/blastdb/results/BLAST_EXIT_CODE.out`
```

**SSD 모드** (blast-batch-job-local-ssd-aks.yaml.template):

```bash
# 동일한 패턴이지만 /shared/results/ 경로 사용
azcopy cp /shared/results/BLASTDB_LENGTH.out ${ELB_RESULTS}/metadata/
azcopy cp /shared/results/BLAST_RUNTIME-${JOB_NUM}.out ${ELB_RESULTS}/logs/
azcopy cp /shared/results/batch_${JOB_NUM}-*.out.gz ${ELB_RESULTS}/
```

**핵심**: `results-export` 컨테이너는 `blast` 컨테이너와 **동시 실행**되며,
공유 볼륨의 `BLAST_EXIT_CODE.out` 파일을 polling하여 완료를 감지합니다.
실패 시 error 파일을 `metadata/FAILURE.txt`로 업로드합니다.

---

## 8. K8S Job 이름 및 레이블 체계

**상수 정의** (constants.py L364-L371):

| 상수                            | 값                      | 용도                      |
| ------------------------------- | ----------------------- | ------------------------- |
| `K8S_JOB_GET_BLASTDB`           | `get-blastdb`           | DB 다운로드 컨테이너      |
| `K8S_JOB_LOAD_BLASTDB_INTO_RAM` | `load-blastdb-into-ram` | DB RAM 캐시 initContainer |
| `K8S_JOB_IMPORT_QUERY_BATCHES`  | `import-query-batches`  | 쿼리 배치 복사 컨테이너   |
| `K8S_JOB_SUBMIT_JOBS`           | `submit-jobs`           | 작업 제출 Job/컨테이너    |
| `K8S_JOB_BLAST`                 | `blast`                 | BLAST 실행 컨테이너       |
| `K8S_JOB_RESULTS_EXPORT`        | `results-export`        | 결과 업로드 컨테이너      |
| `K8S_JOB_INIT_PV`               | `init-pv`               | PV 초기화 Job             |
| `K8S_JOB_CLOUD_SPLIT_SSD`       | `cloud-split-ssd`       | 클라우드 쿼리 분할 Job    |

**레이블 체계**:

- `app=setup`: init-pv, init-ssd-\* Jobs
- `app=submit`: submit-jobs Job
- `app=blast`: BLAST 실행 Jobs

---

## 9. Docker 이미지 역할

| 이미지                          | ACR 경로                                                | 용도                                 |
| ------------------------------- | ------------------------------------------------------- | ------------------------------------ |
| `ncbi/elb`                      | `elbacr.azurecr.io/ncbi/elb:1.4.0`                      | BLAST 실행, DB 다운로드, 결과 업로드 |
| `ncbi/elasticblast-query-split` | `elbacr.azurecr.io/ncbi/elasticblast-query-split:0.1.4` | 쿼리 분할 (cloud split)              |
| `ncbi/elasticblast-job-submit`  | `elbacr.azurecr.io/ncbi/elasticblast-job-submit:4.1.0`  | Cloud Job Submission                 |
| `ncbi/elasticblast-janitor`     | `elbacr.azurecr.io/ncbi/elasticblast-janitor:0.4.0`     | 자동 클린업 (미구현)                 |

---

## 10. 인증 메커니즘

| 컨텍스트           | 인증 방법              | 상세                                   |
| ------------------ | ---------------------- | -------------------------------------- |
| CLI → Azure        | Azure CLI (`az login`) | 사용자 계정                            |
| CLI → Blob Storage | SAS Token              | `get_sas_token()` (elb_config.py L362) |
| Pod → Blob Storage | Managed Identity       | `azcopy login --identity`              |
| Pod → ACR          | AcrPull RBAC           | kubelet identity에 할당                |
| CLI → AKS          | Azure CLI              | `az aks get-credentials`               |
| Pod → K8S API      | ServiceAccount         | `elb-janitor-rbac.yaml`                |

---

## 11. 주요 파일 참조 인덱스

| 파일                                                    | 주요 기능                | 핵심 줄                                                                                      |
| ------------------------------------------------------- | ------------------------ | -------------------------------------------------------------------------------------------- |
| `src/elastic_blast/azure.py`                            | ElasticBlastAzure 클래스 | submit: L130, \_initialize_cluster: L325, start_cluster: L1066                               |
| `src/elastic_blast/commands/submit.py`                  | submit 진입점            | submit(): L126, split_query(): L256                                                          |
| `src/elastic_blast/kubernetes.py`                       | K8S 작업 관리            | initialize_storage: L505, initialize_persistent_disk: L650, submit_job_submission_job: L1030 |
| `src/elastic_blast/filehelper.py`                       | Blob Storage I/O         | copy_to_bucket: L129, upload_file_to_azure: L114                                             |
| `src/elastic_blast/elb_config.py`                       | Azure 설정               | AZUREConfig: L287                                                                            |
| `src/elastic_blast/constants.py`                        | 상수 정의                | AKS_PROVISIONING_STATE: L395, K8S Job 이름: L364                                             |
| `templates/job-init-pv-aks.yaml.template`               | PV 초기화 Job            | DB 다운+쿼리 복사                                                                            |
| `templates/job-init-local-ssd-aks.yaml.template`        | SSD 초기화 Job           | DaemonSet + per-node init                                                                    |
| `templates/blast-batch-job-aks.yaml.template`           | PV BLAST Job             | vmtouch+blast+export                                                                         |
| `templates/blast-batch-job-local-ssd-aks.yaml.template` | SSD BLAST Job            | import+blast+export                                                                          |
| `templates/job-submit-jobs-aks.yaml.template`           | Cloud Job 제출           | submit-jobs Pod                                                                              |
| `templates/pvc-rwm-aks.yaml.template`                   | Azure Blob NFS PVC       | ReadWriteMany                                                                                |
| `docker-job-submit/cloud-job-submit-aks.sh`             | Cloud 제출 스크립트      | batch_list → kubectl apply                                                                   |
