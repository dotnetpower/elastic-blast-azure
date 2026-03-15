# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-15 02:56 UTC
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
| G1 | medium | Standard_E32s_v3 | 1 | blob_nfs | 459.0 | 0.26 | success |
| G2 | medium | Standard_E32s_v3 | 1 | warm | 128.0 | 0.07 | failed |
| G4 | medium | Standard_E32s_v3 | 1 | warm | 68.1 | 0.04 | failed |

## 6. Failures

### G2

```
2/2 failed
```

### G4

```
4/4 failed
```

