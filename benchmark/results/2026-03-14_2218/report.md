# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-14 13:29 UTC
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

## 2. Phase A: Baseline (Blob NFS, cold vs warm)

| Test | Storage | Reuse | Total (s) | Cost ($) | Status |
| ---- | ------- | ----- | --------- | -------- | ------ |
| A1 | blob_nfs | cold | 410.2 | 0.23 | success |
| A2 | warm | warm | 63.9 | 0.04 | success |

**Warm cluster savings**: 84% (410s -> 64s)

## 5. All Results Summary

| Test | Dataset | VM | Nodes | Storage | Total (s) | Cost ($) | Status |
| ---- | ------- | -- | ----- | ------- | --------- | -------- | ------ |
| A1 | small | Standard_E32s_v3 | 1 | blob_nfs | 410.2 | 0.23 | success |
| A2 | small | Standard_E32s_v3 | 1 | warm | 63.9 | 0.04 | success |
