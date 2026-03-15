# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-14 15:32 UTC
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

## 5. All Results Summary

| Test | Dataset | VM | Nodes | Storage | Total (s) | Cost ($) | Status |
| ---- | ------- | -- | ----- | ------- | --------- | -------- | ------ |
| E1 | medium | Standard_E32s_v3 | 1 | blob_nfs | 422.4 | 0.24 | success |
| E2 | medium | Standard_E32s_v3 | 3 | blob_nfs | 403.1 | 0.68 | success |
