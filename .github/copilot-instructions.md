---
description: "elastic-blast-azure"
applyTo: "**"
---

# elastic-blast-azure Instructions

ElasticBLAST Azure extends NCBI's ElasticBLAST to run on Microsoft Azure.
It leverages Azure Kubernetes Service (AKS) for distributed, large-scale BLAST sequence searches in the cloud.

- Upstream: `https://github.com/ncbi/elastic-blast.git`
- Official docs: <https://blast.ncbi.nlm.nih.gov/doc/elastic-blast/>
- Current version: 1.5.0 (BLAST+ 2.17.0)

---

## Core Principles

### 1. Language Rules

- **English only**: All conversations, documentation, code comments, commit messages, and PR descriptions must be in English.
- No exceptions.

### 2. Upstream Code Preservation

- This repository is forked from `ncbi/elastic-blast`. Minimize changes to upstream files.
- Azure-specific logic should live in dedicated files (`azure.py`, `azure_traits.py`, `*-aks*` templates) whenever possible.
- When upstream files must be modified, use minimal CSP branch additions (`if cfg.cloud_provider == CSP.AZURE`).
- Periodically sync with upstream: `git fetch upstream && git merge upstream/master`.

### 3. Security and Validation

- All external inputs must be validated and sanitized.
- Never include secrets (passwords, API keys, connection strings) in code or config files.
- Use `DefaultAzureCredential` / Managed Identity instead of storage account keys where possible.
- Minimize emoji in documentation for readability.
- Only provide fact-based, verified information.

### 4. Auto-update Rules

- When resolving code errors, add the fix to the "Known Issues" section of this document.
- Target: deprecated attributes, unsupported blocks/properties, version-specific syntax changes.
- Format: concise, clear sentences (e.g., "ResourceName: correct attribute (~~wrong attribute~~)").

### 5. User Interaction

- Clarify ambiguous requests before proceeding.
- Break complex problems into step-by-step solutions.

---

## Project Structure

```
elastic-blast-azure/
├── bin/
│   └── elastic-blast                  # Main CLI entry point
├── src/elastic_blast/                 # Core Python package
│   ├── azure.py                       # ElasticBlastAzure class (AKS implementation, 903 lines)
│   ├── azure_sdk.py                   # Azure SDK clients (replaces az CLI subprocess calls)
│   ├── azure_traits.py                # Azure VM types, blob helpers (Managed Identity)
│   ├── azure_cost_tracker.py          # VM pricing and cost estimation
│   ├── azure_optimizer.py             # Optimization profiles (cost/balanced/performance)
│   ├── azure_monitor.py               # Application Insights telemetry (OpenTelemetry)
│   ├── aws.py                         # AWS implementation (upstream)
│   ├── gcp.py                         # GCP implementation (upstream)
│   ├── elb_config.py                  # Config management (ElasticBlastConfig, AZUREConfig)
│   ├── constants.py                   # Constants (CSP enum, status, config keys)
│   ├── kubernetes.py                  # Kubernetes job management
│   ├── filehelper.py                  # File I/O abstraction (local/GCS/S3/Azure Blob)
│   ├── util.py                        # Utilities (safe_exec, handle_error, BLAST DB validation)
│   ├── tuner.py                       # Resource tuning (memory limits, thread counts)
│   ├── elasticblast_factory.py        # CSP factory (CSP -> implementation mapping)
│   ├── commands/                      # CLI commands
│   │   ├── submit.py                  # elastic-blast submit
│   │   ├── status.py                  # elastic-blast status
│   │   ├── delete.py                  # elastic-blast delete
│   │   └── run_summary.py            # elastic-blast run-summary (Azure supported)
│   └── templates/                     # Kubernetes YAML templates
│       ├── *-aks.yaml.template        # Azure AKS-specific templates
│       ├── elb-finalizer-aks.yaml.template  # Auto-cleanup finalizer job
│       ├── pvc-rwm-anf-aks.yaml.template    # Azure NetApp Files PVC
│       ├── storage-aks-anf.yaml       # ANF StorageClass
│       ├── vmtouch-daemonset-aks.yaml.template  # DB RAM caching DaemonSet
│       ├── scripts/                   # AKS pod scripts (ConfigMap)
│       │   ├── blast-run-aks.sh       # BLAST execution (streaming results support)
│       │   ├── blast-vmtouch-aks.sh   # Dynamic RAM caching (80% available)
│       │   ├── init-db-download-aks.sh      # DB download (azcopy optimized, retry)
│       │   ├── init-db-partitioned-aks.sh   # Partitioned DB download
│       │   ├── results-export-aks.sh  # Results upload to Blob Storage
│       │   ├── query-download-ssd-aks.sh    # Local SSD query download
│       │   ├── elb-finalizer-aks.sh   # Auto-cleanup script
│       │   └── blob-stream-upload.py  # Direct-to-Blob streaming uploader
│       └── *.yaml.template            # GCP/common templates
├── docker-job-submit/                 # Job submission Docker image
│   ├── Dockerfile.azure               # Multi-stage alpine (no google/cloud-sdk)
│   ├── cloud-job-submit-aks.sh        # AKS job submission (cleaned, no GCP code)
│   └── Makefile                       # VERSION=4.1.0
├── docker-janitor/                    # Resource cleanup Docker image
│   ├── Dockerfile.azure               # Azure janitor (alpine-based)
│   ├── elastic-blast-janitor-azure.sh # AKS cluster cleanup script
│   └── Dockerfile.gcp                 # GCP only (upstream)
├── tests/
│   ├── azure/                         # Azure unit tests (108 tests)
│   ├── azure_traits/                  # Azure VM traits unit tests
│   ├── submit/                        # Submit tests (includes Azure configs)
│   ├── status/                        # Status tests (includes Azure config)
│   └── ...                            # Other test suites (aws, gcp, config, etc.)
├── docs/
│   ├── azure-prereq.md                # Azure prerequisites
│   ├── environment.md                 # Environment setup guide
│   ├── improvement-plan.md            # Improvement roadmap
│   └── azure-data-pipeline-analysis.md # Data pipeline analysis
├── requirements/
│   ├── base.txt                       # Runtime dependencies
│   └── test.txt                       # Test dependencies
├── Makefile                           # Top-level build
└── setup.cfg                          # Package configuration
```

### Key Classes and Modules

| Module                   | Description                                                            |
| ------------------------ | ---------------------------------------------------------------------- |
| `ElasticBlastAzure`      | Azure AKS-based ElasticBLAST implementation (`azure.py`)               |
| `AzureClients`           | Lazy-initialized Azure SDK clients (`azure_sdk.py`)                    |
| `AzureOptimizer`         | Cost/balanced/performance profiles (`azure_optimizer.py`)              |
| `ElasticBlastConfig`     | Config file parsing and validation (`elb_config.py`)                   |
| `AZUREConfig`            | Azure-specific config section (`elb_config.py`)                        |
| `CSP`                    | Cloud service provider enum: GCP, AWS, AZURE (`constants.py`)          |
| `ElbStatus`              | Run status: SUCCESS, FAILURE, CREATING, RUNNING, etc. (`constants.py`) |
| `AKS_PROVISIONING_STATE` | AKS cluster state: CREATING, SUCCEEDED, FAILED, etc. (`constants.py`)  |

### Supported Cloud Platforms

| Platform  | Status               | Key Files                                                         |
| --------- | -------------------- | ----------------------------------------------------------------- |
| GCP       | Upstream (stable)    | `gcp.py`, `gcp_traits.py`                                         |
| AWS       | Upstream (stable)    | `aws.py`, `aws_traits.py`                                         |
| **Azure** | **Production-ready** | `azure.py`, `azure_sdk.py`, `azure_traits.py`, `*-aks*` templates |

---

## Azure Configuration

### INI Config File Format

```ini
[cloud-provider]
azure-region = koreacentral
azure-acr-resource-group = rg-elbacr
azure-acr-name = elbacr
azure-resource-group = rg-elb
azure-storage-account = stgelb
azure-storage-account-container = blast-db

[cluster]
name = elastic-blast
machine-type = Standard_E32s_v3
num-nodes = 3
exp-use-local-ssd = true       # Use local SSD mode (hostPath) instead of NFS PV
reuse = true                   # Reuse existing AKS cluster

[blast]
program = blastx
db = https://<storage>.blob.core.windows.net/<container>/<db-path>
queries = https://<storage>.blob.core.windows.net/<container>/<query-file>
results = https://<storage>.blob.core.windows.net/<container>/results
options = -task blastx-fast -evalue 0.01 -outfmt 7
```

### Supported Azure VM Types

| VM Size                    | vCPU | RAM (GB) | Use Case         |
| -------------------------- | ---- | -------- | ---------------- |
| Standard_HB120rs_v3        | 120  | 480      | HPC              |
| Standard_HC44rs            | 44   | 352      | HPC              |
| Standard_E64s_v3           | 64   | 432      | Memory-optimized |
| Standard_E32s_v3 (default) | 32   | 256      | Memory-optimized |
| Standard_E16s_v3           | 16   | 128      | Memory-optimized |
| Standard_D32s_v3           | 32   | 128      | General purpose  |
| Standard_D8s_v3            | 8    | 32       | Dev/test         |

### Environment Variables

| Variable                    | Description                                                        | Required    |
| --------------------------- | ------------------------------------------------------------------ | ----------- |
| `AZCOPY_AUTO_LOGIN_TYPE`    | Set to `AZCLI` for azcopy to use Azure CLI credentials (local dev) | Recommended |
| `ELB_DISABLE_AUTO_SHUTDOWN` | Disable automatic cluster shutdown                                 | Optional    |

### Authentication

The Azure implementation uses **Managed Identity** exclusively:

- **In AKS pods**: Uses `azcopy login --identity` (Workload Identity / kubelet identity)
- **Local development**: Uses `DefaultAzureCredential` which chains Azure CLI credentials

Storage Account keys and SAS tokens are **not used**. Ensure your AKS cluster's kubelet identity has `Storage Blob Data Contributor` role on the storage account.

---

## Docker Images

### ACR (Azure Container Registry) Images

| Image                           | Purpose          | Version | Makefile                     |
| ------------------------------- | ---------------- | ------- | ---------------------------- |
| `ncbi/elb`                      | BLAST execution  | 1.4.0   | `docker-blast/Makefile`      |
| `ncbi/elasticblast-query-split` | Query splitting  | 0.1.4   | `docker-qs/Makefile`         |
| `ncbi/elasticblast-job-submit`  | Job submission   | 4.1.0   | `docker-job-submit/Makefile` |
| `ncbi/elasticblast-janitor`     | Resource cleanup | 0.4.0   | `docker-janitor/Makefile`    |

### Building Docker Images for Azure

```bash
# Set AZURE_REGISTRY in each Makefile to your ACR (default: elbacr.azurecr.io)
cd docker-blast && make azure-build
cd docker-job-submit && make azure-build
cd docker-qs && make azure-build
```

Note: `docker-janitor` does not have an Azure build target yet.

---

## AKS Data Pipeline

### Two Storage Modes

**PV Mode** (`azureblob-nfs-premium`): Shared NFS PVC, DB downloaded once, all pods share.

```
init-pv Job → DB to NFS PV (1x) → BLAST pods mount PV → vmtouch caches DB to RAM
```

**Local-SSD Mode** (`hostPath`): DB downloaded per-node, local disk I/O.

```
init-ssd-N Jobs → DB to each node's /workspace (Nx) → BLAST pods use hostPath
```

### Job Execution Flow

```
1. elastic-blast submit (CLI)
2. Query split + upload to Blob Storage
3. AKS cluster create (az aks create, 5-15 min)
4. Storage initialization (init-pv or init-ssd-N)
5. submit-jobs Pod: downloads templates, generates Job YAMLs, kubectl apply
6. BLAST batch Jobs (N parallel):
   - initContainer: vmtouch (PV) or azcopy download queries (SSD)
   - blast container: runs BLAST search
   - results-export sidecar: azcopy uploads results to Blob Storage
7. Results available at {results}/{job_id}/*.out.gz
```

---

## Development Setup

### Required Tools

| Tool      | Version | Installation                                              |
| --------- | ------- | --------------------------------------------------------- |
| Python    | 3.11+   | `sudo apt install python3.11`                             |
| Azure CLI | Latest  | `curl -sL https://aka.ms/InstallAzureCLIDeb \| sudo bash` |
| kubectl   | Latest  | `sudo snap install kubectl --classic`                     |
| azcopy    | v10     | `https://aka.ms/downloadazcopy-v10-linux`                 |

### Local Development

```bash
# Create and activate virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements/test.txt

# Build ElasticBLAST (note: pip install -e . may fail due to packit)
make elastic-blast

# Run with PYTHONPATH (workaround for editable install)
PYTHONPATH=src:$PYTHONPATH python -m elastic_blast ...

# Run Azure tests
pytest tests/azure/ -v
pytest tests/azure_traits/ -v
```

### Dependencies

**Runtime (requirements/base.txt):**

- `azure-core`, `azure-identity`, `azure-storage-blob` - Azure SDK
- `boto3`, `botocore` - AWS SDK
- `tenacity` - Retry logic
- `dataclasses-json` - JSON serialization

**Test (requirements/test.txt):**

- `pytest`, `pytest-cov`, `pytest-mock`
- `mypy`, `pylint`
- `moto` - AWS mocking

---

## Coding Guidelines

### Python

1. **Type hints** on all functions:

   ```python
   def get_machine_properties(machineType: str) -> InstanceProperties:
   ```

2. **Docstrings** in English:

   ```python
   """Given the Azure VM size, returns a tuple of number of CPUs and amount of RAM in GB."""
   ```

3. **Error handling** with `UserReportError`:

   ```python
   raise UserReportError(returncode=DEPENDENCY_ERROR, message='Error message')
   ```

4. **Logging** via `logging` module:

   ```python
   logging.debug(f'AKS status: {aks_status}')
   ```

5. **JSONEnumEncoder**: Use `super().default(o)` not `json.JSONEncoder(self, o)`. Handle `bytes` type for Azure user fields.

### Kubernetes Templates

- AKS-specific templates: `*-aks.yaml.template`
- Template variables: `${VARIABLE_NAME}` format
- Location: `src/elastic_blast/templates/`
- Always include `azcopy login --identity;` before azcopy commands in pod scripts

### Constants

- Azure constants use `ELB_DFLT_AZURE_*` or `CFG_CP_AZURE_*` prefixes
- Add new constants to `src/elastic_blast/constants.py`
- Group related constants together
- Append Azure constants at the end of the file to minimize merge conflicts

---

## Known Issues and Fixes

### Authentication

| Issue                                                             | Fix                                                              |
| ----------------------------------------------------------------- | ---------------------------------------------------------------- |
| `Public access is not permitted on this storage account` (urllib) | Replace `urllib.request.urlopen` with `azcopy cp` for Azure URLs |
| `azcopy` auth failure in AKS pods                                 | Add `azcopy login --identity;` in pod scripts (Managed Identity) |

### AKS Cluster

| Issue                                                             | Fix                                                              |
| ----------------------------------------------------------------- | ---------------------------------------------------------------- |
| Cluster provisioning takes 5-15 minutes                           | Wait for AKS provisioning to complete before proceeding          |
| kubectl context error                                             | Re-run `az aks get-credentials`                                  |
| Stale jobs cause immutable field errors                           | `kubectl delete jobs --all` before resubmitting                  |
| Kubernetes 1.31+ returns `SuccessCriteriaMet Complete` conditions | Use jsonpath-based job failure check instead of pattern matching |

### Code Fixes Applied

| Issue                                        | Fix                                                        |
| -------------------------------------------- | ---------------------------------------------------------- |
| `JSONEnumEncoder.default()` wrong super call | `json.JSONEncoder(self, o)` -> `super().default(o)`        |
| `bytes` type in Azure user field             | Added `isinstance(o, bytes)` handling in `JSONEnumEncoder` |
| `cfg.gcp.project` error on Azure             | Added `if cfg.gcp:` guard in `enable_service_account()`    |
| `extglob` pattern matching fails on K8s 1.31 | Rewrote job completion check in `cloud-job-submit-aks.sh`  |

---

## Testing

```bash
# Azure tests only
pytest tests/azure/ -v

# Azure traits tests
pytest tests/azure_traits/ -v

# Submit tests (includes Azure configs)
pytest tests/submit/ -v

# All tests with coverage
pytest --cov=elastic_blast tests/
```

### Test Data

- `tests/azure/data/` - Azure test config files
- `tests/submit/data/JAIJZY.ini` - Real Azure integration test config
- `tests/status/data/status-test-azure.ini` - Azure status test config
- `tests/config/data/` - General config test INI files

---

## CLI Commands

```bash
elastic-blast submit --cfg <config.ini>        # Submit BLAST search
elastic-blast status --cfg <config.ini>        # Check run status
elastic-blast delete --cfg <config.ini>        # Delete cloud resources
elastic-blast run-summary --cfg <config.ini>   # Get run summary
```

---

## Benchmark Report Format

Benchmark reports in `benchmark/results/report-final.md` must follow an **academic paper structure**:

### Required Sections

1. **Abstract** — Summary of findings with quantitative results
2. **Introduction** — Background, motivation, research questions (RQ1-RQn), related work
3. **Experimental Setup** — Infrastructure table, datasets table, methodology
4. **Results** — One subsection per research question, each containing:
   - Finding statement (bold, one line)
   - Data table
   - **Mermaid chart** (`xychart-beta` for bar charts, `pie` for breakdowns)
   - Interpretation paragraph explaining what the data means
5. **Cost Analysis** — Per-test cost chart + cost-effectiveness comparison
6. **Discussion** — Answers to RQs table, comparison with prior work, limitations
7. **Conclusion** — Key contributions (numbered), production recommendations table, future work
8. **Appendix** — Complete results table, issues log, infrastructure validation

### Chart Requirements

- Every quantitative comparison MUST include a Mermaid chart
- Use `xychart-beta` for bar comparisons (e.g., storage, scaling)
- Use `pie` for breakdowns (e.g., cold-start time allocation)
- Charts must have titles and axis labels

### Data Collection Per Test

Each benchmark test must collect:

- **Timing**: total elapsed, per-phase (cluster create, DB download, job submit, BLAST execution)
- **Cost**: `elapsed_hours * vm_cost_hr * num_nodes`
- **K8s**: job start/completion timestamps, pod succeeded/failed counts
- **Azure Monitor** (when available): CPU%, Memory, Disk IOPS, Network time-series
- **Pod metrics** (when available): /proc/diskstats, /proc/meminfo, ps aux
- **BLAST runtime**: \time output from blast-run-aks.sh

### References

Always cite:

- [1] Altschul et al. 1990 (original BLAST paper)
- [2] Camacho et al. 2023 (ElasticBLAST paper)
- [3] Tsai 2021 (Azure HPC BLAST benchmark blog)

---

## References

- [NCBI ElasticBLAST Official Docs](https://blast.ncbi.nlm.nih.gov/doc/elastic-blast/)
- [ElasticBLAST Paper (BMC Bioinformatics 2023)](https://doi.org/10.1186/s12859-023-05245-9)
- [Azure HPC BLAST Benchmark (Tsai 2021)](https://techcommunity.microsoft.com/blog/azurehighperformancecomputingblog/running-ncbi-blast-on-azure-%E2%80%93-performance-scalability-and-best-practice/2410483)
- [Azure Prerequisites](docs/azure-prereq.md)
- [Environment Setup Guide](docs/environment.md)
- [Improvement Plan](docs/improvement-plan.md)
- [Data Pipeline Analysis](docs/azure-data-pipeline-analysis.md)
- [Benchmark Report](benchmark/results/report-final.md)
