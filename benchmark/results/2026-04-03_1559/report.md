# ElasticBLAST Azure Benchmark Report

> Generated: 2026-04-03 11:31 UTC
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
| H1 | large | Standard_E16s_v3 | 1 | blob_nfs | 438.3 | 0.12 | success |
| H2 | large | Standard_E16s_v3 | 1 | nvme | 340.8 | 0.10 | success |
| H3 | large | Standard_E32s_v3 | 1 | blob_nfs | 344.0 | 0.19 | success |
| H4 | large | Standard_E32s_v3 | 1 | warm | 448.1 | 0.25 | success |
