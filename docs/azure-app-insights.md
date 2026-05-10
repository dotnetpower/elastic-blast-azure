# Azure Application Insights for ElasticBLAST

ElasticBLAST can send telemetry to Azure Application Insights for monitoring search lifecycle events (job submission, cluster creation/deletion, failures).

## Prerequisites

Install the OpenTelemetry packages (not included in base requirements):

```bash
pip install opentelemetry-sdk azure-monitor-opentelemetry-exporter
```

## Setup

1. Create an Application Insights resource in the Azure Portal (or use an existing one).

2. Copy the **Connection String** from the resource's Overview page.

3. Set the environment variable before running `elastic-blast`:

```bash
export APPLICATIONINSIGHTS_CONNECTION_STRING="InstrumentationKey=xxx;IngestionEndpoint=https://xxx.in.applicationinsights.azure.com/"
```

## Tracked Events

| Event            | Span Name             | Attributes                                             | When                                  |
| ---------------- | --------------------- | ------------------------------------------------------ | ------------------------------------- |
| Search submitted | `elb.search.submit`   | job_id, program, db, num_jobs, num_nodes, machine_type | After BLAST jobs are submitted        |
| Search completed | `elb.search.complete` | job_id, succeeded, failed, program, db                 | After status check detects completion |
| Cluster created  | `elb.cluster.create`  | cluster_name, duration_s, num_nodes, machine_type      | After AKS cluster provisioning        |
| Cluster deleted  | `elb.cluster.delete`  | cluster_name, duration_s, num_nodes, machine_type      | After AKS cluster deletion            |

## Metrics

| Metric                          | Type      | Description                        |
| ------------------------------- | --------- | ---------------------------------- |
| `elb.jobs.submitted`            | Counter   | Total BLAST jobs submitted         |
| `elb.jobs.failed`               | Counter   | BLAST jobs that failed             |
| `elb.cluster.create_duration_s` | Histogram | Cluster creation time distribution |

## KQL Query Examples

```kql
// Job submissions in the last 24h
customEvents
| where name == "elb.search.submit"
| where timestamp > ago(24h)
| project timestamp, job_id=customDimensions["elb.job_id"],
          program=customDimensions["elb.program"],
          db=customDimensions["elb.db"],
          num_jobs=toint(customDimensions["elb.num_jobs"])

// Cluster creation times
customMetrics
| where name == "elb.cluster.create_duration_s"
| summarize avg(value), max(value), count() by bin(timestamp, 1d)

// Failed jobs
customEvents
| where name == "elb.search.complete"
| where toint(customDimensions["elb.failed"]) > 0
| project timestamp, job_id=customDimensions["elb.job_id"],
          failed=customDimensions["elb.failed"]
```

## Disabling

Unset the environment variable (default behavior):

```bash
unset APPLICATIONINSIGHTS_CONNECTION_STRING
```

When the variable is not set, all telemetry functions are no-ops with zero overhead.
