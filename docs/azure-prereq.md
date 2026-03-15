# ElasticBLAST on Azure — Setup Guide

> **Project Status**: Active research (March 2026)
>
> This repository extends NCBI's ElasticBLAST to run distributed BLAST searches on Microsoft Azure.
> For performance evaluation results, see the [Benchmark Report](../benchmark/results/report-final.md).
>
> **Validated Versions**: Azure CLI 2.81.0 · kubectl v1.34.5 · azcopy v10.28.0 · BLAST+ 2.17.0 · AKS K8s v1.33

---

## What is ElasticBLAST?

ElasticBLAST is a cloud-based tool developed by [NCBI](https://www.ncbi.nlm.nih.gov) that distributes BLAST sequence searches across multiple cloud instances. Where a single-machine BLAST search against a 2TB database might take over 100 hours, ElasticBLAST can reduce this to a few hours by splitting the work across many nodes.

**This fork** adds full Azure AKS support, including:

- Azure SDK integration (no `az CLI` subprocess calls)
- Multiple storage backends (Blob NFS, Local NVMe SSD, Azure NetApp Files)
- Warm cluster reuse (84% faster for repeated searches)
- DB partitioning for terabyte-scale databases
- Automated benchmarking and cost tracking

---

## Prerequisites

Before you begin, make sure you have:

| Requirement   | Minimum              | Why                    |
| ------------- | -------------------- | ---------------------- |
| Azure account | Active subscription  | Resource deployment    |
| vCPU quota    | 32+ in target region | AKS nodes need compute |
| Azure CLI     | v2.70+               | Cluster management     |
| kubectl       | v1.28+               | K8s operations         |
| azcopy        | v10+                 | Fast blob transfers    |
| Python        | 3.11+                | ElasticBLAST runtime   |
| Git           | Any                  | Clone this repo        |

> **Tip**: Check your current vCPU quota with:
>
> ```bash
> az vm list-usage -l <your-region> --query "[?name.value=='cores'].{used:currentValue,limit:limit}" -o table
> ```

---

## Step 1: Install Required Tools

### Azure CLI

The Azure CLI is used to manage your Azure resources (AKS clusters, storage accounts, etc.).

```bash
# Linux (Ubuntu/Debian)
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Verify installation
az version
```

For other operating systems, see the [official installation guide](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

### kubectl

kubectl is the Kubernetes command-line tool. ElasticBLAST uses it to manage BLAST jobs on AKS.

```bash
# Ubuntu/Debian
sudo snap install kubectl --classic

# Verify
kubectl version --client
```

### azcopy

azcopy is a high-performance data transfer tool for Azure Blob Storage. It's essential for uploading databases and downloading results.

```bash
# Download and install azcopy v10
wget -q https://aka.ms/downloadazcopy-v10-linux -O /tmp/azcopy.tar.gz
tar -xf /tmp/azcopy.tar.gz -C /tmp
sudo mv /tmp/azcopy_linux_*/azcopy /usr/local/bin/
rm -rf /tmp/azcopy*

# Verify
azcopy --version
```

---

## Step 2: Log In to Azure

All Azure operations require authentication. Use device code login for remote/headless environments:

```bash
az login --use-device-code
```

This will display a URL and a code. Open the URL in any browser, enter the code, and sign in with your Azure account.

### Verify your subscription

After login, confirm you're on the correct subscription:

```bash
az account show --query '{name:name, id:id, state:state}' -o table
```

If you have multiple subscriptions, switch to the correct one:

```bash
az account set --subscription "Your Subscription Name"
```

---

## Step 3: Create a Resource Group

A resource group is a logical container for all Azure resources used by ElasticBLAST.

```bash
# Choose your variables
REGION=koreacentral        # Azure region (pick one close to you)
RG=rg-elb                  # Resource group name

az group create --name $RG --location $REGION
```

---

## Step 4: Create an Azure Container Registry (ACR)

ElasticBLAST runs BLAST searches inside Docker containers on AKS. These container images need to be stored in a private registry that AKS can access.

```bash
ACR_RG=rg-elbacr           # Separate RG for ACR (recommended)
ACR_NAME=elbacr            # ACR name (must be globally unique, lowercase)

# Create ACR resource group
az group create --name $ACR_RG --location $REGION

# Create the ACR
az acr create --resource-group $ACR_RG --name $ACR_NAME --sku Standard
```

> **Why a separate resource group?** ACR is shared across all AKS clusters, while each benchmark may have its own resource group. Keeping ACR separate prevents accidental deletion.

The following Docker images are required:

| Image                           | Purpose                   | Folder               |
| ------------------------------- | ------------------------- | -------------------- |
| `ncbi/elb`                      | BLAST execution           | `docker-blast/`      |
| `ncbi/elasticblast-job-submit`  | Job submission to K8s     | `docker-job-submit/` |
| `ncbi/elasticblast-query-split` | Query file splitting      | `docker-qs/`         |
| `elb-openapi`                   | Management API (optional) | `docker-openapi/`    |

---

## Step 5: Set Up the Development Environment

### 5.1 Operating System

This project is developed and tested on **Ubuntu 22.04**. If you're on Windows, use WSL2 with Ubuntu. Alternatively, create an Azure VM running Ubuntu.

For detailed environment setup, see [environment.md](./environment.md).

### 5.2 Clone the Repository

```bash
git clone https://github.com/dotnetpower/elastic-blast-azure.git
cd elastic-blast-azure
```

### 5.3 Create a Python Virtual Environment

```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements/test.txt
```

> **Note**: `pip install -e .` may not work due to packit configuration. Use `PYTHONPATH=src:$PYTHONPATH` when running ElasticBLAST commands instead.

---

## Step 6: Build and Push Docker Images

Each Docker image needs to be built and pushed to your ACR.

### 6.1 Update the ACR Registry Name

In each Docker folder's `Makefile`, update the registry URL:

```makefile
# Change this line in each Makefile:
AZURE_REGISTRY?=elbacr.azurecr.io
# To your ACR:
AZURE_REGISTRY?=youracr.azurecr.io
```

### 6.2 Build and Push

```bash
# Log in to your ACR
az acr login --name $ACR_NAME

# Build and push each image
cd docker-blast && make azure-build && cd ..
cd docker-job-submit && make azure-build && cd ..
cd docker-qs && make azure-build && cd ..
```

### 6.3 Verify

```bash
az acr repository list --name $ACR_NAME -o table
```

You should see:

```
Result
----------------------------------
elb-openapi
ncbi/elasticblast-job-submit
ncbi/elasticblast-query-split
ncbi/elb
```

---

## Step 7: Create a Storage Account and Upload Databases

### 7.1 Create the Storage Account

```bash
SA_NAME=stgelb              # Storage account name (must be globally unique, lowercase)

az storage account create \
  --resource-group $RG \
  --name $SA_NAME \
  --hns true \
  --location $REGION \
  --sku Standard_LRS
```

### 7.2 Create Containers

```bash
# Log in with Azure AD for blob operations
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI

# Create containers (if they don't exist)
az storage container create --account-name $SA_NAME --name blast-db --auth-mode login
az storage container create --account-name $SA_NAME --name queries --auth-mode login
az storage container create --account-name $SA_NAME --name results --auth-mode login
```

### 7.3 Upload BLAST Databases

The fastest way to get NCBI databases into Azure is **direct transfer from the NCBI public S3 bucket**. This avoids the slow NCBI FTP and transfers data at cloud-to-cloud speed.

```bash
# Enable public network access temporarily
az storage account update -n $SA_NAME --public-network-access Enabled -o none
sleep 10  # Wait for propagation

# Example: Transfer nt_prok database (82GB, ~2 minutes at 860 MB/s)
azcopy cp \
  "https://ncbi-blast-databases.s3.amazonaws.com/2025-09-16-01-05-02/" \
  "https://$SA_NAME.blob.core.windows.net/blast-db/nt_prok/" \
  --recursive \
  --include-pattern "nt_prok*" \
  --block-size-mb=256

# Disable public access when done
az storage account update -n $SA_NAME --public-network-access Disabled -o none
```

> **Why S3?** NCBI publishes pre-formatted BLAST databases on AWS S3 (`s3://ncbi-blast-databases`). azcopy can transfer directly from S3 to Azure Blob at up to 860 MB/s — **the same 82GB transfer takes hours via NCBI FTP but only 2 minutes from S3.**

### 7.4 Upload Query Files

```bash
az storage account update -n $SA_NAME --public-network-access Enabled -o none
azcopy cp ./your-query-file.fa "https://$SA_NAME.blob.core.windows.net/queries/"
az storage account update -n $SA_NAME --public-network-access Disabled -o none
```

### 7.5 Available Databases on S3

To see what databases are available:

```bash
# List all databases in the NCBI S3 bucket
LATEST=$(curl -s "https://ncbi-blast-databases.s3.amazonaws.com/latest-dir")
curl -s "https://ncbi-blast-databases.s3.amazonaws.com/?list-type=2&prefix=${LATEST}/&delimiter=/" \
  | grep -oP '<CommonPrefixes><Prefix>\K[^<]+' | sed "s|${LATEST}/||" | sort -u
```

Common databases and their approximate sizes:

| Database         | Description                | Approx. Size | Recommended VM         |
| ---------------- | -------------------------- | ------------ | ---------------------- |
| `pdbnt`          | PDB nucleotide             | ~500 MB      | Any                    |
| `env_nt`         | Environmental nucleotide   | ~3 GB        | Any                    |
| `nt_prok`        | Prokaryote nucleotide      | **82 GB**    | E16s_v3 (128GB RAM)    |
| `nt`             | Full nucleotide collection | **500+ GB**  | E32s_v3 + partitioning |
| `refseq_protein` | RefSeq protein             | ~120 GB      | E32s_v3 (256GB RAM)    |

---

## Step 8: Create the Configuration File

ElasticBLAST uses an INI configuration file. Create one for your search:

```ini
# my-search.ini

[cloud-provider]
azure-region = koreacentral
azure-acr-resource-group = rg-elbacr
azure-acr-name = elbacr
azure-resource-group = rg-elb
azure-storage-account = stgelb
azure-storage-account-container = blast-db

[cluster]
name = elastic-blast
machine-type = Standard_E32s_v3    # 32 vCPU, 256 GB RAM
num-nodes = 1
reuse = true                       # Reuse cluster for repeat searches (84% faster)
exp-use-local-ssd = false          # Set true for NVMe (6.2x CPU efficiency for large DBs)

[blast]
program = blastn                   # blastn, blastp, blastx, tblastn, tblastx
db = https://stgelb.blob.core.windows.net/blast-db/nt_prok/nt_prok
queries = https://stgelb.blob.core.windows.net/queries/my-query.fa
results = https://stgelb.blob.core.windows.net/results
options = -evalue 0.01 -outfmt 7
```

### Configuration Options Explained

| Section   | Key                 | Description                          | Example                         |
| --------- | ------------------- | ------------------------------------ | ------------------------------- |
| `cluster` | `machine-type`      | Azure VM size                        | `Standard_E32s_v3` (256GB RAM)  |
| `cluster` | `num-nodes`         | Number of AKS worker nodes           | `1` (start small)               |
| `cluster` | `reuse`             | Keep cluster alive between searches  | `true` (recommended)            |
| `cluster` | `exp-use-local-ssd` | Use local NVMe instead of Blob NFS   | `true` for DB > 50GB            |
| `blast`   | `program`           | BLAST program to run                 | `blastn`, `blastx`, etc.        |
| `blast`   | `db`                | Full URL to database in Blob Storage | Must include volume prefix      |
| `blast`   | `queries`           | Full URL to query file(s)            | Supports `.gz` compressed files |

### Storage Mode Guide

| DB Size   | `exp-use-local-ssd`      | Why                                                                                |
| --------- | ------------------------ | ---------------------------------------------------------------------------------- |
| < 50 GB   | `false` (Blob NFS)       | Cheaper, shared storage, DB fits in RAM                                            |
| 50-200 GB | **`true` (NVMe)**        | 6.2x better CPU efficiency (see [benchmark](../benchmark/results/report-final.md)) |
| 200 GB+   | `true` + `db-partitions` | Split DB across nodes                                                              |

---

## Step 9: Run ElasticBLAST

### 9.1 Set Environment Variables

```bash
cd elastic-blast-azure
source venv/bin/activate

# Required: azcopy authentication
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI

# Recommended: skip DB integrity check for custom databases
export ELB_SKIP_DB_VERIFY=true
```

### 9.2 Submit a Search

```bash
PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast submit --cfg my-search.ini
```

This will:

1. Create an AKS cluster (5-10 minutes for cold start)
2. Download the BLAST database to the cluster
3. Split your query file into batches
4. Submit BLAST jobs to Kubernetes
5. Upload results to your Blob Storage `results` container

### 9.3 Check Status

```bash
PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast status --cfg my-search.ini
```

### 9.4 Download Results

```bash
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
az storage account update -n stgelb --public-network-access Enabled -o none

# List results
azcopy list "https://stgelb.blob.core.windows.net/results/"

# Download results
azcopy cp "https://stgelb.blob.core.windows.net/results/" ./my-results/ --recursive

az storage account update -n stgelb --public-network-access Disabled -o none
```

### 9.5 Clean Up Resources

```bash
# Delete the search (removes jobs, keeps cluster if reuse=true)
PYTHONPATH=src:$PYTHONPATH python bin/elastic-blast delete --cfg my-search.ini

# To fully delete the AKS cluster and free all resources:
az aks delete -g rg-elb -n elastic-blast --yes
```

> **Cost warning**: AKS clusters incur charges while running. Always delete or stop clusters when not in use:
>
> ```bash
> az aks stop -g rg-elb -n elastic-blast    # Stop (preserves state, no compute cost)
> az aks start -g rg-elb -n elastic-blast   # Restart later
> ```

---

## Step 10: Run Benchmarks (Optional)

The benchmark suite tests ElasticBLAST across multiple storage backends, DB sizes, thread counts, and node configurations.

```bash
# See all available phases
PYTHONPATH=src:$PYTHONPATH python benchmark/run_benchmark.py --help

# Dry-run: generate configs without running (no cost)
PYTHONPATH=src:$PYTHONPATH python benchmark/run_benchmark.py --phase ALL --dry-run

# Run Phase A: cold vs warm cluster comparison
PYTHONPATH=src:$PYTHONPATH python benchmark/run_benchmark.py --phase A

# Run Phase H: large DB (82GB nt_prok) storage comparison
PYTHONPATH=src:$PYTHONPATH python benchmark/run_benchmark.py --phase H
```

### Benchmark Phases

| Phase | Description                   | Tests | Approx. Time |
| ----- | ----------------------------- | ----- | ------------ |
| A     | Cold start vs warm reuse      | 2     | ~15 min      |
| B     | Storage comparison (10MB DB)  | 2     | ~15 min      |
| C     | Scale-out (1 vs 3 nodes)      | 2     | ~20 min      |
| D     | Storage comparison (2GB DB)   | 2     | ~15 min      |
| E     | Scale-out with 2GB DB         | 2     | ~15 min      |
| F     | Thread scaling (1-32 threads) | 6     | ~25 min      |
| G     | Concurrent queries            | 3     | ~20 min      |
| H     | Large DB (82GB) NFS vs NVMe   | 2-4   | ~3 hours     |

Results are saved to `benchmark/results/YYYY-MM-DD_HHMM/report.md`.

---

## VSCode Remote Development (Optional)

For remote development on an Azure VM:

```bash
sudo apt-get install openssh-server sshfs
```

Then connect from VSCode using the Remote-SSH extension.

---

## Known Issues and Troubleshooting

### Storage Account Networking

**Symptom**: `AuthorizationFailure` or `PublicAccessNotPermitted` errors.

**Fix**: Enable public network access on the storage account. The benchmark runner does this automatically, but for manual operations:

```bash
az storage account update -n $SA_NAME --public-network-access Enabled -o none
# ... do your work ...
az storage account update -n $SA_NAME --public-network-access Disabled -o none
```

> **Note**: After enabling, wait 10-30 seconds for propagation before using azcopy.

### Managed Identity Permissions

**Symptom**: Pods fail with `azcopy login --identity` errors or `AuthorizationFailure` from inside AKS.

**Fix**: Grant the AKS kubelet identity the required roles:

```bash
KUBELET_ID=$(az aks show -g $RG -n $CLUSTER \
  --query 'identityProfile.kubeletidentity.objectId' -o tsv)
SA_ID=$(az storage account show -n $SA_NAME -g $RG --query id -o tsv)
ACR_ID=$(az acr show -n $ACR_NAME --query id -o tsv)

# Storage access (for DB download and result upload)
az role assignment create --assignee $KUBELET_ID \
  --role 'Storage Blob Data Contributor' --scope $SA_ID

# ACR access (for pulling Docker images)
az role assignment create --assignee $KUBELET_ID \
  --role 'AcrPull' --scope $ACR_ID
```

### AuthorizationFailure with Custom DB URLs

**Symptom**: Warning about `get_latest_dir failed` in logs.

**Explanation**: This occurs when using custom DB URLs (e.g., `https://stgelb.blob.core.windows.net/blast-db/...`) because ElasticBLAST tries to list blobs using the Azure SDK. This has been fixed with a graceful `try/except` — **the warning is harmless and can be ignored**.

### vCPU Quota Exceeded

**Symptom**: `ErrCode_InsufficientVCPUQuota` when creating AKS clusters.

**Fix**: Check your quota and delete unused clusters:

```bash
# Check quota
az vm list-usage -l $REGION \
  --query "[?name.value=='cores'].{used:currentValue,limit:limit}" -o table

# Delete stopped clusters to free quota
az aks list -g $RG --query "[?powerState.code=='Stopped'].name" -o tsv \
  | xargs -I{} az aks delete -g $RG -n {} --yes --no-wait
```

| VM Type | vCPU per node | 3 nodes = |
| ------- | ------------- | --------- |
| D8s_v3  | 8             | 24 vCPU   |
| E16s_v3 | 16            | 48 vCPU   |
| E32s_v3 | 32            | 96 vCPU   |

### BLAST `-num_threads` Duplication

**Symptom**: BLAST fails with "duplicate option" error.

**Fix**: Do **not** add `-num_threads` to the INI `options` field. ElasticBLAST automatically sets it from the VM's CPU count. The script detects and avoids duplication if you do include it.

### ElasticBLAST Memory Check Rejection

**Symptom**: `DB memory requirements exceed memory available` error.

**Explanation**: ElasticBLAST checks that the DB fits in VM RAM before creating the cluster. For an 82GB DB, you need at least a 128GB RAM VM (E16s_v3).

| DB Size    | Minimum VM             | RAM    |
| ---------- | ---------------------- | ------ |
| < 50 GB    | D8s_v3                 | 32 GB  |
| 50-100 GB  | **E16s_v3**            | 128 GB |
| 100-250 GB | E32s_v3                | 256 GB |
| 250 GB+    | E32s_v3 + partitioning | 256 GB |

### Which Storage Backend Should I Use?

Our benchmark shows **dramatic differences** between Blob NFS and Local NVMe for large databases:

| DB Size vs RAM                 | Blob NFS CPU | NVMe CPU  | Recommendation                 |
| ------------------------------ | ------------ | --------- | ------------------------------ |
| DB << RAM (e.g., 2GB on 256GB) | ~15%         | ~15%      | Either (no difference)         |
| DB ≈ 50-80% of RAM             | **14.6%**    | **90.5%** | **NVMe (6.2x more efficient)** |
| DB > RAM                       | Very slow    | Fast      | **NVMe required**              |

Set `exp-use-local-ssd = true` in your INI file for databases larger than 50% of VM RAM.

For the full analysis, see the [Benchmark Report](../benchmark/results/report-final.md).

---

## References

- [NCBI ElasticBLAST Official Documentation](https://blast.ncbi.nlm.nih.gov/doc/elastic-blast/)
- [ElasticBLAST Paper (BMC Bioinformatics 2023)](https://doi.org/10.1186/s12859-023-05245-9)
- [Azure HPC BLAST Benchmark (Tsai 2021)](https://techcommunity.microsoft.com/blog/azurehighperformancecomputingblog/running-ncbi-blast-on-azure-%E2%80%93-performance-scalability-and-best-practice/2410483)
- [Improvement Plan](./improvement-plan.md)
- [Benchmark Report](../benchmark/results/report-final.md)
