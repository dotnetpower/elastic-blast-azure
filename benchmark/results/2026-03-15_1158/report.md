# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-15 03:14 UTC
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
| G1 | medium | Standard_E32s_v3 | 1 | blob_nfs | 388.2 | 0.22 | success |
| G2 | medium | Standard_E32s_v3 | 1 | warm | 66.5 | 0.04 | failed |
| G4 | medium | Standard_E32s_v3 | 1 | warm | 77.8 | 0.04 | failed |

## 6. Failures

### G2

```
1/2 failed
```

### G4

```
3/4 failed
```

