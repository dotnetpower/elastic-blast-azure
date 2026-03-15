# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-15 09:20 UTC
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
| H1 | large | Standard_D8s_v3 | 1 | blob_nfs | 13.6 | 0.00 | failed |
| H2 | large | Standard_D8s_v3 | 1 | nvme | 12.5 | 0.00 | failed |
| H3 | large | Standard_E32s_v3 | 1 | blob_nfs | 435.9 | 0.24 | success |
| H4 | large | Standard_E32s_v3 | 1 | warm | 438.2 | 0.25 | success |

## 6. Failures

### H1

```
ERROR: BLAST database https://stgelb.blob.core.windows.net/blast-db/nt_prok/nt_prok memory requirements exceed memory available on selected machine type "Standard_D8s_v3". Please select machine type with at least 81.7GB available memory.
```

### H2

```
ERROR: BLAST database https://stgelb.blob.core.windows.net/blast-db/nt_prok/nt_prok memory requirements exceed memory available on selected machine type "Standard_D8s_v3". Please select machine type with at least 81.7GB available memory.
```

