# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-14 14:24 UTC
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

## 4. Phase C: Scale-out

| Test | Storage | VM | Nodes | Total (s) | Cost ($) | Status |
| ---- | ------- | -- | ----- | --------- | -------- | ------ |
| C1 | blob_nfs | Standard_E32s_v3 | 1 | 438.2 | 0.25 | success |
| C2 | blob_nfs | Standard_E32s_v3 | 3 | 56.5 | 0.09 | failed |

**Scaling**: 1 -> 3 nodes = 7.8x speedup (259% efficiency)

## 5. All Results Summary

| Test | Dataset | VM | Nodes | Storage | Total (s) | Cost ($) | Status |
| ---- | ------- | -- | ----- | ------- | --------- | -------- | ------ |
| C1 | small | Standard_E32s_v3 | 1 | blob_nfs | 438.2 | 0.25 | success |
| C2 | small | Standard_E32s_v3 | 3 | blob_nfs | 56.5 | 0.09 | failed |

## 6. Failures

### C2

```
ERROR: Failed to create AKS cluster elb-bench-c2: (ErrCode_InsufficientVCPUQuota) Insufficient regional vcpu quota left for location koreacentral. left regional vcpu quota 70, requested quota 96. If you want to increase the quota, please follow this instruction: https://learn.microsoft.com/en-us/azure/quotas/view-quotas. Surge nodes would also consume vcpu quota, please consider use smaller maxSurge or use maxUnavailable to proceed upgrade without surge nodes, details: aka.ms/aks/maxUnavailable.
```

