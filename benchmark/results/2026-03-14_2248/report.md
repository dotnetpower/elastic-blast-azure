# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-14 14:06 UTC
> Region: koreacentral
> Test baseline: 115 passed, 8 skipped

---

## 1. Test Environment

| Item | Value |
| ---- | ----- |
| AKS Region | koreacentral |
| Resource Group | rg-elb-koc |
| Storage Account | stgelb |
| ACR | elbacr.azurecr.io |

## 3. Phase B: Storage Comparison

| Test | Storage | VM | Nodes | Total (s) | Cost ($) | Status |
| ---- | ------- | -- | ----- | --------- | -------- | ------ |
| B1 | blob_nfs | Standard_E32s_v3 | 1 | 426.6 | 0.24 | success |
| B2 | nvme | Standard_E32s_v3 | 1 | 372.0 | 0.21 | success |

**Relative performance vs Blob NFS**:

- blob_nfs: 1.00x (slower)
- nvme: 1.15x (faster)

## 5. All Results Summary

| Test | Dataset | VM | Nodes | Storage | Total (s) | Cost ($) | Status |
| ---- | ------- | -- | ----- | ------- | --------- | -------- | ------ |
| B1 | small | Standard_E32s_v3 | 1 | blob_nfs | 426.6 | 0.24 | success |
| B2 | small | Standard_E32s_v3 | 1 | nvme | 372.0 | 0.21 | success |
