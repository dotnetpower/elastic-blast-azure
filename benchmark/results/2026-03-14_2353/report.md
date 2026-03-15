# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-14 15:09 UTC
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
| D1 | medium | Standard_E32s_v3 | 1 | blob_nfs | 363.0 | 0.20 | success |
| D2 | medium | Standard_E32s_v3 | 1 | nvme | 320.8 | 0.18 | success |
