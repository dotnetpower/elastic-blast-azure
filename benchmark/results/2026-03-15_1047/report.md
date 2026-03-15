# ElasticBLAST Azure Benchmark Report

> Generated: 2026-03-15 02:08 UTC
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
| F1 | medium | Standard_E32s_v3 | 1 | blob_nfs | 450.6 | 0.25 | success |
| F2 | medium | Standard_E32s_v3 | 1 | warm | 50.5 | 0.03 | failed |
| F3 | medium | Standard_E32s_v3 | 1 | warm | 50.3 | 0.03 | failed |
| F4 | medium | Standard_E32s_v3 | 1 | warm | 50.7 | 0.03 | failed |
| F5 | medium | Standard_E32s_v3 | 1 | warm | 51.4 | 0.03 | failed |
| F6 | medium | Standard_E32s_v3 | 1 | warm | 53.0 | 0.03 | failed |

## 6. Failures

### F2

```
ERROR: The command "kubectl --context=elb-bench-f1 apply -f /tmp/tmp8o_oic29/job-submit-jobs.yaml" returned with exit code 1
```

### F3

```
ERROR: The command "kubectl --context=elb-bench-f1 apply -f /tmp/tmpa_7wftqy/job-import-queries.yaml" returned with exit code 1
```

### F4

```
ERROR: The command "kubectl --context=elb-bench-f1 apply -f /tmp/tmp7g7q__6q/job-import-queries.yaml" returned with exit code 1
```

### F5

```
ERROR: The command "kubectl --context=elb-bench-f1 apply -f /tmp/tmp_mbhemku/job-import-queries.yaml" returned with exit code 1
```

### F6

```
ERROR: The command "kubectl --context=elb-bench-f1 apply -f /tmp/tmplczfc2ax/job-import-queries.yaml" returned with exit code 1
```

