#!/bin/bash
# query-download-ssd-aks.sh — Download query batch for local-SSD mode
#
# Environment variables (set by K8s pod spec):
#   ELB_RESULTS - Azure Blob Storage URL for results (contains query_batches/)
#   JOB_NUM     - Batch job number

set -e

mkdir -p /shared/requests
mkdir -p /shared/results

azcopy login --identity
azcopy cp "${ELB_RESULTS}/query_batches/batch_${JOB_NUM}.fa" /shared/requests
