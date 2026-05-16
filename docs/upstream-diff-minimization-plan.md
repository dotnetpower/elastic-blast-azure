# Upstream Diff Minimization Plan

> **Status**: Planning (보류, 추후 진행)
> **Author**: 2026-05-16
> **Goal**: `ncbi/elastic-blast` upstream과의 diff를 최소화하여 fork를 정리하고, 향후 PR 가능 형태로 만들기.

---

## 현황 분석 (2026-05-16 기준)

- `master` ↔ `upstream/master`: **82 커밋**, 509 파일 변경, 약 243K 삽입
- 변경은 크게 3가지로 분류됨:

### A. 순수 신규 파일 (upstream 영향 0)

- `src/elastic_blast/azure*.py` (5개): `azure.py`(2027), `azure_api.py`(672), `azure_api_types.py`(224), `azure_cli_glue.py`(388), `azure_traits.py`(494)
- `src/elastic_blast/templates/*-aks*` (~25개): YAML 템플릿 + scripts/
- `tests/azure/`, `tests/azure_traits/`, `tests/submit/data/*azure*.ini`, `tests/status/data/status-test-azure.ini`
- `docker-*/Dockerfile.azure`, `cloud-job-submit-aks.sh`, `elastic-blast-janitor-azure.sh` 등

### B. upstream 파일 수정 (20개 파일, ~1,570 라인 변경) — **줄여야 할 대상**

| 파일                                                | 현재 diff | 비고                        |
| --------------------------------------------------- | --------- | --------------------------- |
| `src/elastic_blast/kubernetes.py`                   | **685**   | 가장 큼                     |
| `src/elastic_blast/elb_config.py`                   | **347**   |                             |
| `src/elastic_blast/util.py`                         | 149       |                             |
| `src/elastic_blast/filehelper.py`                   | 136       |                             |
| `bin/elastic-blast`                                 | 133       |                             |
| `src/elastic_blast/constants.py`                    | 108       | append-only로 OK            |
| `src/elastic_blast/commands/submit.py`              | 82        |                             |
| `src/elastic_blast/tuner.py`                        | 57        |                             |
| `src/elastic_blast/commands/prepare.py`             | 53 (신규) | Azure 전용?                 |
| `src/elastic_blast/commands/run_summary.py`         | 52        |                             |
| `src/elastic_blast/config.py`                       | 46        |                             |
| `src/elastic_blast/db_metadata.py`                  | 28        |                             |
| `src/elastic_blast/janitor.py`                      | 15        |                             |
| `src/elastic_blast/elasticblast.py`                 | 14        |                             |
| `src/elastic_blast/jobs.py`                         | 6         |                             |
| `src/elastic_blast/split.py`                        | 6         | bytes-decode 픽스 (PR 가능) |
| `src/elastic_blast/aws.py`                          | 3         | `handle_error` 사용         |
| `src/elastic_blast/base.py`                         | 3         | `DBSource.AZURE` + 오타     |
| `src/elastic_blast/elasticblast_factory.py`         | 3         | 이미 최소                   |
| `src/elastic_blast/commands/status.py`              | 2         | `--command` 인자            |
| `src/elastic_blast/resources/quotas/quota_check.py` | 3         |                             |

### C. 벤치마크/내부용 (upstream과 무관) — **별도 브랜치 분리 대상**

- `benchmark/` 전체
- `docker-openapi/` (FastAPI SaaS 래퍼)
- `docs/azure-pipeline-reference.md`, `docs/azure-data-pipeline-analysis.md`, `docs/improvement-plan.md`, `docs/azure-app-insights.md`
- `docs/azure-prereq.md`, `docs/environment.md` ← **유지** (Azure 사용자 문서)
- `.github/copilot-instructions.md`, `.vscode/`
- `setup.cfg_cloud`, `Makefile-create-blastdb-metadata`, `Makefile-gcp-elb-janitor` 등 신규 빌드 산출물
- `__queuestorage__/`, `private/`, `benchmark/.enable-public-access.*` (gitignore 처리 필요)

---

## 핵심 전략

> **"upstream 파일 수정 = CSP 디스패치 1줄 또는 1개 함수 호출"** 원칙으로 수렴시키고, 모든 Azure 로직은 신규 `azure_*.py` 모듈로 위임.

---

## 0단계 — 안전망 & 브랜치 토폴로지

```bash
# 현재 상태 보존
git tag archive/pre-restructure-2026-05-16 master
git push origin archive/pre-restructure-2026-05-16
```

제안 브랜치 구조:

| 브랜치                      | 역할                                            | 베이스          |
| --------------------------- | ----------------------------------------------- | --------------- |
| `upstream/master`           | NCBI 원본 (read-only)                           | —               |
| `azure` (또는 `azure-core`) | **Azure 지원만**, upstream diff 최소화, PR 후보 | upstream/master |
| `benchmark`                 | 벤치마크 스크립트/설정/결과/보고서              | `azure`         |
| `openapi`                   | FastAPI SaaS 래퍼                               | `azure`         |
| `dev` (기존 `master` 유지)  | 통합 작업 브랜치                                | 위 셋 머지      |

---

## 1단계 — 벤치마크/내부용 자산 분리

upstream PR 후보에서 제외할 것들:

| 경로                                  | 분리 대상 브랜치  |
| ------------------------------------- | ----------------- |
| `benchmark/`                          | `benchmark`       |
| `docker-openapi/`                     | `openapi`         |
| `docs/azure-pipeline-reference.md` 등 | `benchmark`/내부  |
| `.github/copilot-instructions.md`     | 내부 (PR 시 제외) |
| `.vscode/`                            | gitignore         |
| `__queuestorage__/`, `private/`       | gitignore + 삭제  |

---

## 2단계 — upstream 파일 수정 최소화 (핵심)

### 2.1 그냥 되돌릴 것 (gratuitous diffs)

| 파일                                                                          | 액션                                                                                           |
| ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `base.py`                                                                     | `'invalid value' → 'invalid @ value'` 오타성 변경 revert. `DBSource.AZURE = auto()` 1줄만 유지 |
| `aws.py`                                                                      | `handle_error()` 사용 변경 revert (또는 별도 cleanup PR로 분리)                                |
| 모든 `print(f'\033[33m ...\033[0m')` 디버그 출력                              | 전체 삭제                                                                                      |
| `# update config file in metadata / no need\n# write_config_to_metadata(cfg)` | 의도가 모호한 주석 처리 — 원복하거나 명확한 이유 주석 필요                                     |

### 2.2 Azure 로직을 별도 모듈로 추출

| upstream 파일                                   | 현재 diff | 추출 대상                                                                                                         | 잔존 diff (목표) |
| ----------------------------------------------- | --------- | ----------------------------------------------------------------------------------------------------------------- | ---------------- |
| `kubernetes.py`                                 | 685       | `kubernetes_aks.py` 신규 — `setup_pv`, `submit_jobs`, `delete_all`, `get_jobs` 등의 Azure 분기를 한 줄 디스패치로 | < 50             |
| `elb_config.py`                                 | 347       | `AZUREConfig`를 `azure_config.py`로 이동, lazy import                                                             | < 60             |
| `filehelper.py`                                 | 136       | `azure_filehelper.py` (이미 일부 존재) — URL prefix 디스패치 테이블 도입                                          | < 30             |
| `util.py`                                       | 149       | `azure_util.py` 신규 — `validate_azure_*`, `azure_get_regions`, blob 관련 함수 분기 추출                          | < 40             |
| `tuner.py`                                      | 57        | `azure_tuner.py` 신규 — `azure_get_mem_limit`, `azure_get_machine_type` 이동                                      | < 20             |
| `db_metadata.py`                                | 28        | `get_db_metadata` 의 source==AZURE 분기를 `azure_db_metadata.py`로 위임                                           | < 15             |
| `jobs.py`                                       | 6         | template 선택을 `_select_blast_job_template(cfg)` 헬퍼로 캡슐화                                                   | 6                |
| `janitor.py`                                    | 15        | URL prefix dispatch는 통합 헬퍼로 흡수                                                                            | < 5              |
| `commands/submit.py`                            | 82        | `gcp_prj/sas_token` 추출을 `_get_csp_credentials(cfg)` 헬퍼 1개로                                                 | < 20             |
| `commands/status.py`                            | 2         | `--command` arg가 Azure 전용이면 `azure.py`에서 동적 추가                                                         | 0~2              |
| `elasticblast.py`                               | 14        | `sas_token` 처리를 `_get_csp_credentials()` 헬퍼로 일원화                                                         | < 5              |
| `elasticblast_factory.py`                       | 3         | 이미 최소 — 유지                                                                                                  | 3                |
| `config.py`                                     | 46        | env var 처리를 `_load_azure_env_vars(cfg)` 함수로 묶고 azure_config.py로 이동                                     | < 15             |
| `constants.py`                                  | 108       | append-only로 OK. 파일 끝에 `# === Azure-specific constants ===` 섹션 헤더 명시                                   | 108 (유지)       |
| `split.py`                                      | 6         | `bytes` 디코딩 처리는 일반적인 버그픽스 — upstream에 그대로 PR 가능                                               | 6                |
| `bin/elastic-blast`                             | 133       | argparse Azure 옵션을 `azure_args.add_arguments(subparser)`로 위임                                                | < 20             |
| `setup.py`, `Makefile`, `requirements/base.txt` | 소량      | Azure SDK deps만 추가, 나머지 변경 revert                                                                         | 최소             |

### 2.3 새로 만들 헬퍼 (`src/elastic_blast/cloud_helpers.py`)

```python
def get_csp_credentials(cfg) -> tuple[Optional[str], Optional[str]]:
    """Returns (gcp_project, azure_sas_token) for the active CSP."""
    gcp_prj = None
    sas_token = None
    if cfg.cloud_provider.cloud == CSP.GCP:
        gcp_prj = cfg.gcp.get_project_for_gcs_downloads()
    elif cfg.cloud_provider.cloud == CSP.AZURE:
        sas_token = cfg.azure.get_sas_token()
    return gcp_prj, sas_token
```

이것 하나로 `commands/submit.py`의 ~10곳 반복되는 분기가 사라짐.

---

## 3단계 — 실행 순서

```
P0. archive 태그 + 새 azure 브랜치 (upstream/master 기준)
P1. azure 브랜치에 신규 Azure 파일들만 cherry-pick/copy
P2. 그 위에 upstream 파일 변경을 "최소 형태"로 다시 작성 (위 2.2 표대로)
    P2.1 cloud_helpers.py 추가
    P2.2 kubernetes_aks.py, azure_config.py, azure_filehelper.py, azure_util.py, azure_tuner.py 추출
    P2.3 upstream 파일은 dispatch 1줄/import 1줄만 추가
    P2.4 디버그 print, 오타성 diff 모두 제거
P3. tests/azure/ 통과 검증 (`pytest tests/azure/ -v`)
P4. 통합 검증 (실제 AKS submit — bench-1node.ini 같은 작은 설정으로)
P5. benchmark 브랜치 만들기 (master 기반에서 azure 변경분만 제거)
P6. openapi 브랜치 동일 방식
P7. README에 "fork 차이점" 섹션 정리, upstream PR 가능 부분 표기
```

---

## 4단계 — 분리 후 운영 모델

```
upstream/master ──→ azure ──┬──→ benchmark
                            └──→ openapi
                                   ↓
                            (필요 시 dev에 머지하여 통합 사용)
```

- 벤치마크 실행: `benchmark` 브랜치에서. `git switch benchmark && git merge azure` 로 최신 azure 코어 흡수
- upstream PR 시도: `azure` 브랜치에서. 작은 cleanup PR부터 분할 제출
- 운영용 SaaS 이미지: `openapi` 브랜치

---

## 5단계 — PR 분할 제안 (선택)

upstream에 실제 보내려면 단일 거대 PR보다:

1. **PR #1 (작음)**: `safe_exec`에 `timeout` 옵션 + `handle_error()` + `split.py` bytes-decode 픽스
2. **PR #2 (작음)**: filehelper에 prefix→opener dispatch 테이블 도입 (기존 GCP/AWS만, 리팩터)
3. **PR #3 (중간)**: tuner/db_metadata에 CSP dispatch 도입 (리팩터)
4. **PR #4 (큼)**: Azure CSP 본체 — `azure*.py`, templates, tests, dispatch 등록

---

## 보류 사유 / 다음 액션

- **2026-05-16**: 보안 취약점 점검을 우선 처리하기로 결정. 본 계획은 보안 fix 완료 후 재개.
- 보안 fix 결과는 별도 문서 또는 본 계획 6단계로 통합.
