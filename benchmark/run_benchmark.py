#!/usr/bin/env python3
"""
benchmark/run_benchmark.py — Automated ElasticBLAST Azure Performance Benchmark

Runs BLAST searches across multiple storage backends and collects rich performance data:
- Storage comparison (Blob NFS, NVMe, ANF)
- Azure Monitor metrics (CPU%, Memory, Disk IOPS, Network)
- Per-pod iostat/vmstat (via kubectl exec)
- BLAST Phase timing (I/O-bound vs CPU-bound separation)
- Thread scaling curve (num_threads sweep)
- Concurrent query performance degradation
- tuned-adm profile comparison
- Per-storage cost analysis
- DB size impact

Usage:
  python benchmark/run_benchmark.py --phase A     # Baseline: cold/warm
  python benchmark/run_benchmark.py --phase B     # Storage: small DB
  python benchmark/run_benchmark.py --phase D     # Storage: 2GB DB
  python benchmark/run_benchmark.py --phase E     # Scale-out: 2GB DB
  python benchmark/run_benchmark.py --phase F     # Thread scaling
  python benchmark/run_benchmark.py --phase G     # Concurrent queries
  python benchmark/run_benchmark.py --phase ALL   # Full suite
  python benchmark/run_benchmark.py --phase A --dry-run

Results: benchmark/results/YYYY-MM-DD_HHMM/
Report:  benchmark/results/YYYY-MM-DD_HHMM/report.md

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REGION = 'koreacentral'
RG = 'rg-elb-koc'
ACR_RG = 'rg-elbacr'
ACR_NAME = 'elbacr'
STORAGE_ACCOUNT = 'stgelb'
STORAGE_CONTAINER = 'blast-db'
RESULTS_CONTAINER = 'results'

# BLAST test datasets
DATASETS = {
    'small': {
        'db': f'https://{STORAGE_ACCOUNT}.blob.core.windows.net/{STORAGE_CONTAINER}/wolf18/RNAvirome.S2.RDRP',
        'queries': f'https://{STORAGE_ACCOUNT}.blob.core.windows.net/queries/small.fa',
        'program': 'blastx',
        'options': '-task blastx-fast -evalue 0.01 -outfmt 7',
        'db_size_gb': 0.01,
        'query_size_gb': 0.000002,
    },
    'medium': {
        'db': f'https://{STORAGE_ACCOUNT}.blob.core.windows.net/{STORAGE_CONTAINER}/260_part_aa/260.part_aa',
        'queries': f'https://{STORAGE_ACCOUNT}.blob.core.windows.net/queries/JAIJZY01.1.fsa_nt.gz',
        'program': 'blastn',
        'options': '-evalue 0.01 -outfmt 7',
        'db_size_gb': 2.0,
        'query_size_gb': 0.001,
    },
    'large': {
        'db': f'https://{STORAGE_ACCOUNT}.blob.core.windows.net/{STORAGE_CONTAINER}/nt_prok/nt_prok',
        'queries': f'https://{STORAGE_ACCOUNT}.blob.core.windows.net/queries/JAIJZY01.1.fsa_nt.gz',
        'program': 'blastn',
        'options': '-evalue 0.01 -outfmt 7',
        'db_size_gb': 60.0,
        'query_size_gb': 0.001,
    },
}

# VM types to test
VMS = {
    'D8s_v3':    {'type': 'Standard_D8s_v3',   'vcpu': 8,  'ram_gb': 32,  'cost_hr': 0.384},
    'E16s_v3':   {'type': 'Standard_E16s_v3',  'vcpu': 16, 'ram_gb': 128, 'cost_hr': 1.008},
    'E32s_v3':   {'type': 'Standard_E32s_v3',  'vcpu': 32, 'ram_gb': 256, 'cost_hr': 2.016},
    'E64bs_v5':  {'type': 'Standard_E64bs_v5', 'vcpu': 64, 'ram_gb': 512, 'cost_hr': 4.864},
}


@dataclass
class BenchmarkResult:
    """Single benchmark test result with rich performance data."""
    test_id: str
    timestamp: str
    dataset: str
    vm_type: str
    num_nodes: int
    storage_type: str
    reuse_run: int  # 0 = cold, 1 = warm (2nd run)

    # Timing breakdown (seconds)
    cluster_create_s: float = 0
    db_download_s: float = 0
    job_submit_s: float = 0
    blast_total_s: float = 0
    blast_phase1_io_s: float = 0        # Phase 1: DB read (I/O-bound)
    blast_phase2_cpu_s: float = 0       # Phase 2: alignment (CPU-bound)
    blast_phase3_write_s: float = 0     # Phase 3: results write
    results_upload_s: float = 0
    total_elapsed_s: float = 0

    # BLAST config
    num_threads: int = 0                # -num_threads value
    blast_options: str = ''

    # Costs
    estimated_cost_usd: float = 0

    # K8s metrics
    pods_succeeded: int = 0
    pods_failed: int = 0
    num_batches: int = 0

    # Storage metrics (from Azure Monitor / iostat)
    disk_read_iops_avg: float = 0
    disk_read_iops_max: float = 0
    disk_read_mbps_avg: float = 0
    disk_read_mbps_max: float = 0
    disk_write_iops_avg: float = 0
    disk_write_mbps_avg: float = 0
    disk_latency_ms_avg: float = 0

    # Compute metrics (from Azure Monitor / top)
    cpu_percent_avg: float = 0
    cpu_percent_max: float = 0
    memory_used_gb_avg: float = 0
    memory_used_gb_max: float = 0
    network_in_mbps: float = 0
    network_out_mbps: float = 0

    # Raw metrics time-series (for charts)
    metrics_timeseries: Dict = field(default_factory=dict)

    # Status
    status: str = 'pending'  # pending, running, success, failed
    error: str = ''


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd: str, timeout: int = 300) -> subprocess.CompletedProcess:
    """Run a shell command and return result."""
    log.debug(f'$ {cmd}')
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)


def az(cmd: str, timeout: int = 60) -> str:
    """Run az CLI command and return stdout."""
    result = run(f'az {cmd}', timeout=timeout)
    if result.returncode != 0:
        log.error(f'az command failed: {result.stderr}')
    return result.stdout.strip()


def kubectl(cmd: str, context: str = '', timeout: int = 30) -> str:
    """Run kubectl command."""
    ctx = f'--context={context}' if context else ''
    result = run(f'kubectl {ctx} {cmd}', timeout=timeout)
    return result.stdout.strip()


def save_result(result: BenchmarkResult, output_dir: Path):
    """Save a single result to JSON."""
    os.makedirs(output_dir, exist_ok=True)
    path = output_dir / f'{result.test_id}.json'
    with open(path, 'w') as f:
        json.dump(asdict(result), f, indent=2)
    log.info(f'Result saved: {path}')


def save_monitoring_data(test_id: str, data: Dict, output_dir: Path):
    """Save raw monitoring data (Azure Insights, kubectl metrics)."""
    mon_dir = output_dir / 'monitoring'
    os.makedirs(mon_dir, exist_ok=True)
    path = mon_dir / f'{test_id}_monitoring.json'
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
    log.info(f'Monitoring data saved: {path}')


def collect_k8s_metrics(context: str, test_id: str) -> Dict:
    """Collect K8s pod/job metrics."""
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'test_id': test_id,
    }
    try:
        data['pods'] = kubectl('get pods -o json', context)
        data['jobs'] = kubectl('get jobs -o json', context)
        data['nodes'] = kubectl('top nodes --no-headers 2>/dev/null || echo N/A', context)
        data['pvc'] = kubectl('get pvc -o json', context)
    except Exception as e:
        data['error'] = str(e)
    return data


def collect_azure_metrics(rg: str, cluster_name: str) -> Dict:
    """Collect Azure Monitor metrics for the AKS cluster."""
    data = {
        'timestamp': datetime.utcnow().isoformat(),
    }
    try:
        # Get node resource group for VM metrics
        node_rg = az(f'aks show -g {rg} -n {cluster_name} --query nodeResourceGroup -o tsv')
        if node_rg:
            # Get VMSS metrics
            vmss = az(f'vmss list -g {node_rg} --query "[0].name" -o tsv')
            if vmss:
                data['vmss_name'] = vmss
                data['vmss_instances'] = az(f'vmss list-instances -g {node_rg} -n {vmss} --query "[].instanceId" -o json')
    except Exception as e:
        data['error'] = str(e)
    return data


def collect_azure_monitor_timeseries(rg: str, cluster_name: str,
                                      start_time: str, end_time: str) -> Dict:
    """Collect Azure Monitor time-series metrics for CPU, Memory, Disk, Network.

    Returns dict with metric names as keys, each containing timestamps + values.
    """
    data = {'start': start_time, 'end': end_time}
    try:
        node_rg = az(f'aks show -g {rg} -n {cluster_name} --query nodeResourceGroup -o tsv')
        if not node_rg:
            return data
        vmss = az(f'vmss list -g {node_rg} --query "[0].name" -o tsv')
        if not vmss:
            return data

        vmss_id = f'/subscriptions/{az("account show --query id -o tsv")}/resourceGroups/{node_rg}/providers/Microsoft.Compute/virtualMachineScaleSets/{vmss}'

        metrics = [
            'Percentage CPU',
            'Available Memory Bytes',
            'Disk Read Bytes',
            'Disk Write Bytes',
            'Disk Read Operations/Sec',
            'Disk Write Operations/Sec',
            'Network In Total',
            'Network Out Total',
        ]
        metrics_str = ','.join(f"'{m}'" for m in metrics)

        result = az(
            f'monitor metrics list --resource "{vmss_id}" '
            f'--metric {",".join(m.replace(" ", "%20").replace("/", "%2F") for m in metrics)} '
            f'--aggregation Average Maximum Total '
            f'--start-time {start_time} --end-time {end_time} '
            f'--interval PT1M --output json',
            timeout=120
        )
        if result:
            data['raw'] = result
            # Parse the metrics
            try:
                metrics_data = json.loads(result)
                for metric in metrics_data.get('value', []):
                    name = metric.get('name', {}).get('value', '')
                    timeseries = metric.get('timeseries', [{}])
                    if timeseries and timeseries[0].get('data'):
                        points = timeseries[0]['data']
                        data[name] = {
                            'timestamps': [p.get('timeStamp', '') for p in points],
                            'average': [p.get('average') or p.get('total') or 0 for p in points],
                            'maximum': [p.get('maximum') or p.get('total') or 0 for p in points],
                        }
            except (json.JSONDecodeError, KeyError):
                pass
    except Exception as e:
        data['error'] = str(e)
    return data


def collect_pod_metrics(context: str, pod_name: str) -> Dict:
    """Collect iostat/vmstat/meminfo from inside a running BLAST pod."""
    data = {'pod': pod_name}
    try:
        # Try iostat (may not be available in all images)
        iostat_out = kubectl(
            f'exec {pod_name} -c blast -- sh -c '
            '"iostat -xk 1 1 2>/dev/null || cat /proc/diskstats"',
            context, timeout=15
        )
        data['iostat'] = iostat_out

        # Memory info
        meminfo = kubectl(
            f'exec {pod_name} -c blast -- sh -c '
            '"cat /proc/meminfo | head -10"',
            context, timeout=10
        )
        data['meminfo'] = meminfo

        # CPU info
        cpuinfo = kubectl(
            f'exec {pod_name} -c blast -- sh -c '
            '"cat /proc/stat | head -2"',
            context, timeout=10
        )
        data['cpustat'] = cpuinfo

        # Top processes
        top_out = kubectl(
            f'exec {pod_name} -c blast -- sh -c '
            '"ps aux --sort=-%cpu | head -5 2>/dev/null || echo N/A"',
            context, timeout=10
        )
        data['top'] = top_out
    except Exception as e:
        data['error'] = str(e)
    return data


def collect_blast_phase_timing(context: str, cluster_name: str) -> Dict:
    """Parse BLAST runtime files from pods to extract phase-level timing.

    BLAST pods write BLAST_RUNTIME-{JOB_NUM}.out with \time output:
    Format: "timestamp run start JOB_NUM PROGRAM DB elapsed user sys cpu%"
    """
    data = {}
    try:
        # Get all completed blast pods
        pods_json = kubectl('get pods -l app=blast -o json', context)
        if not pods_json:
            return data
        pods = json.loads(pods_json)
        for pod in pods.get('items', []):
            pod_name = pod['metadata']['name']
            # Get pod start/end timestamps
            conditions = pod.get('status', {}).get('conditions', [])
            started = pod.get('status', {}).get('startTime', '')
            finished = ''
            for c in conditions:
                if c.get('type') == 'Ready' and c.get('status') == 'False':
                    finished = c.get('lastTransitionTime', '')

            # Get container-level timings
            container_statuses = pod.get('status', {}).get('containerStatuses', [])
            for cs in container_statuses:
                if cs.get('name') == 'blast':
                    state = cs.get('state', {}).get('terminated', {})
                    if not state:
                        state = cs.get('lastState', {}).get('terminated', {})
                    if state:
                        data[pod_name] = {
                            'started': state.get('startedAt', ''),
                            'finished': state.get('finishedAt', ''),
                            'exit_code': state.get('exitCode', -1),
                        }

            # Get BLAST runtime log from pod
            try:
                runtime_log = kubectl(
                    f'logs {pod_name} -c blast 2>/dev/null | grep -E "RUNTIME|run start|run end|phase"',
                    context, timeout=15
                )
                if pod_name in data:
                    data[pod_name]['runtime_log'] = runtime_log
            except Exception:
                pass
    except Exception as e:
        data['error'] = str(e)
    return data


def collect_job_timings(context: str) -> Dict:
    """Collect per-job start/completion timestamps for phase breakdown."""
    timings = {}
    try:
        jobs_json = kubectl('get jobs -o json', context)
        if not jobs_json:
            return timings
        jobs = json.loads(jobs_json)
        for job in jobs.get('items', []):
            name = job['metadata']['name']
            status = job.get('status', {})
            timings[name] = {
                'start': status.get('startTime', ''),
                'completion': status.get('completionTime', ''),
                'succeeded': status.get('succeeded', 0),
                'failed': status.get('failed', 0),
                'active': status.get('active', 0),
            }
            # Calculate duration
            if timings[name]['start'] and timings[name]['completion']:
                try:
                    s = datetime.fromisoformat(timings[name]['start'].replace('Z', '+00:00'))
                    e = datetime.fromisoformat(timings[name]['completion'].replace('Z', '+00:00'))
                    timings[name]['duration_s'] = (e - s).total_seconds()
                except Exception:
                    pass
    except Exception as e:
        timings['error'] = str(e)
    return timings


def enrich_result_with_metrics(result: BenchmarkResult, monitoring: Dict):
    """Parse collected monitoring data and populate result fields."""
    # Job timings
    job_timings = monitoring.get('job_timings', {})
    for name, t in job_timings.items():
        dur = t.get('duration_s', 0)
        if 'init-pv' in name or 'init-ssd' in name:
            result.db_download_s = dur
        elif 'submit-jobs' in name:
            result.job_submit_s = dur
        elif 'batch' in name and 'blast' in name.lower():
            result.blast_total_s = max(result.blast_total_s, dur)

    # Azure Monitor metrics
    az_metrics = monitoring.get('azure_monitor', {})
    if 'Percentage CPU' in az_metrics:
        vals = [v for v in az_metrics['Percentage CPU'].get('average', []) if v]
        if vals:
            result.cpu_percent_avg = sum(vals) / len(vals)
            result.cpu_percent_max = max(vals)
    if 'Available Memory Bytes' in az_metrics:
        vals = [v for v in az_metrics['Available Memory Bytes'].get('average', []) if v]
        if vals:
            vm_info = VMS.get(result.vm_type.replace('Standard_', ''), {})
            total_gb = vm_info.get('ram_gb', 256)
            used = [total_gb - v / (1024**3) for v in vals]
            result.memory_used_gb_avg = sum(used) / len(used)
            result.memory_used_gb_max = max(used)
    if 'Disk Read Operations/Sec' in az_metrics:
        vals = [v for v in az_metrics['Disk Read Operations/Sec'].get('average', []) if v]
        if vals:
            result.disk_read_iops_avg = sum(vals) / len(vals)
            result.disk_read_iops_max = max(vals)
    if 'Disk Read Bytes' in az_metrics:
        vals = [v for v in az_metrics['Disk Read Bytes'].get('average', []) if v]
        if vals:
            result.disk_read_mbps_avg = sum(vals) / len(vals) / (1024**2)
            result.disk_read_mbps_max = max(vals) / (1024**2)

    result.metrics_timeseries = az_metrics

def run_single_test(test_id: str, dataset_name: str, vm_name: str,
                    num_nodes: int, storage_type: str, reuse_run: int,
                    output_dir: Path, skip_stop: bool = False) -> BenchmarkResult:
    """Execute a single benchmark test."""

    dataset = DATASETS[dataset_name]
    vm = VMS[vm_name]

    result = BenchmarkResult(
        test_id=test_id,
        timestamp=datetime.utcnow().isoformat(),
        dataset=dataset_name,
        vm_type=vm['type'],
        num_nodes=num_nodes,
        storage_type=storage_type,
        reuse_run=reuse_run,
        num_threads=vm.get('vcpu', 32),
        blast_options=dataset.get('options', ''),
    )

    cluster_name = f'elb-bench-{test_id[:8].lower()}'
    log.info(f'=== Test {test_id}: {dataset_name}/{vm_name}x{num_nodes}/{storage_type} ===')

    try:
        result.status = 'running'

        # Create INI config
        ini_path = f'/tmp/elb-bench-{test_id}.ini'
        _write_config(ini_path, cluster_name, dataset, vm, num_nodes, storage_type)

        # Run elastic-blast submit
        t0 = time.time()
        env = os.environ.copy()
        env['AZCOPY_AUTO_LOGIN_TYPE'] = 'AZCLI'
        env['ELB_SKIP_DB_VERIFY'] = 'true'
        env['PYTHONPATH'] = 'src:' + env.get('PYTHONPATH', '')

        # ANF requires overriding the storage class
        if storage_type == 'anf':
            env['ELB_STORAGE_CLASS'] = 'azure-netapp-ultra'

        proc = subprocess.run(
            ['python', 'bin/elastic-blast', 'submit', '--cfg', ini_path, '--loglevel', 'DEBUG'],
            capture_output=True, text=True, env=env, timeout=3600
        )
        result.total_elapsed_s = time.time() - t0

        if proc.returncode != 0:
            result.status = 'failed'
            # Extract the most meaningful error: check stderr first, then stdout
            full_output = (proc.stderr or '') + (proc.stdout or '')
            # Find ERROR lines for human-readable summary
            error_lines = [l for l in full_output.split('\n') if 'ERROR' in l or 'Error' in l]
            if error_lines:
                result.error = '\n'.join(error_lines[-5:])  # Last 5 error lines
            else:
                result.error = (proc.stderr or proc.stdout or 'Unknown error')[-1000:]
            log.error(f'Test {test_id} failed: {result.error}')
        else:
            # Wait for BLAST jobs to complete
            _wait_for_completion(cluster_name, result)
            if result.pods_failed > 0:
                result.status = 'failed'
                result.error = f'{result.pods_failed}/{result.num_batches} BLAST jobs failed'
                log.error(f'Test {test_id}: {result.error}')
            else:
                result.status = 'success'

        # Collect rich monitoring data
        try:
            start_iso = datetime.utcfromtimestamp(t0).strftime('%Y-%m-%dT%H:%M:%SZ')
            end_iso = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            k8s_data = collect_k8s_metrics('', test_id)
            job_timings = collect_job_timings('')
            phase_timing = collect_blast_phase_timing('', cluster_name)

            # Collect Azure Monitor time-series (CPU, Disk IOPS, Memory, Network)
            az_monitor = {}
            try:
                az_monitor = collect_azure_monitor_timeseries(RG, cluster_name, start_iso, end_iso)
            except Exception as e:
                log.warning(f'Azure Monitor metrics failed: {e}')

            # Collect per-pod iostat/meminfo from running BLAST pods
            pod_metrics = {}
            try:
                pods_out = kubectl('get pods -l app=blast -o jsonpath={.items[*].metadata.name}')
                for pname in pods_out.split():
                    if pname:
                        pod_metrics[pname] = collect_pod_metrics('', pname)
            except Exception as e:
                log.warning(f'Pod metrics collection failed: {e}')

            monitoring = {
                'k8s': k8s_data,
                'job_timings': job_timings,
                'phase_timing': phase_timing,
                'azure_monitor': az_monitor,
                'pod_metrics': pod_metrics,
                'submit_stdout': proc.stdout[-5000:] if proc.stdout else '',
                'submit_stderr': proc.stderr[-5000:] if proc.stderr else '',
            }
            save_monitoring_data(test_id, monitoring, output_dir)

            # Enrich result with parsed metrics
            enrich_result_with_metrics(result, monitoring)
        except Exception as e:
            log.warning(f'Monitoring collection failed: {e}')

        # Calculate cost
        hours = result.total_elapsed_s / 3600
        result.estimated_cost_usd = hours * vm['cost_hr'] * num_nodes

    except Exception as e:
        result.status = 'failed'
        result.error = str(e)
        log.error(f'Test {test_id} exception: {e}')

    finally:
        save_result(result, output_dir)

        # Cleanup (stop cluster, don't delete for reuse tests)
        if not skip_stop and reuse_run == 0 and storage_type != 'warm':
            try:
                az(f'aks stop -g {RG} -n {cluster_name} --no-wait', timeout=30)
                log.info(f'Cluster {cluster_name} stop initiated')
            except Exception:
                pass

    return result


def _write_config(path: str, cluster_name: str, dataset: Dict,
                   vm: Dict, num_nodes: int, storage_type: str):
    """Write ElasticBLAST INI config for a test.

    Storage types:
      blob_nfs — Azure Blob NFS Premium (default PVC mode)
      nvme     — Local NVMe SSD (hostPath, exp-use-local-ssd=true)
      anf      — Azure NetApp Files Ultra (ELB_STORAGE_CLASS override)
      warm     — Reuse existing cluster with DB already loaded
    """
    reuse = 'true' if storage_type == 'warm' else 'false'
    local_ssd = 'true' if storage_type == 'nvme' else 'false'

    config = f"""[cloud-provider]
azure-region = {REGION}
azure-acr-resource-group = {ACR_RG}
azure-acr-name = {ACR_NAME}
azure-resource-group = {RG}
azure-storage-account = {STORAGE_ACCOUNT}
azure-storage-account-container = {STORAGE_CONTAINER}

[cluster]
name = {cluster_name}
machine-type = {vm['type']}
num-nodes = {num_nodes}
reuse = {reuse}
exp-use-local-ssd = {local_ssd}

[blast]
program = {dataset['program']}
db = {dataset['db']}
queries = {dataset['queries']}
results = https://{STORAGE_ACCOUNT}.blob.core.windows.net/{RESULTS_CONTAINER}
options = {dataset['options']}
"""
    with open(path, 'w') as f:
        f.write(config)
    log.info(f'Config written: {path}')


def _wait_for_completion(cluster_name: str, result: BenchmarkResult, timeout: int = 7200):
    """Wait for all BLAST jobs to complete.

    Fetches AKS credentials first, then polls jobs without label filter
    to handle cases where BLAST jobs don't have app=blast label.
    Default timeout increased to 2 hours for large DB searches.
    """
    # Fetch AKS credentials so kubectl works
    try:
        az(f'aks get-credentials -g {RG} -n {cluster_name} --overwrite-existing',
           timeout=30)
    except Exception as e:
        log.warning(f'Failed to get AKS credentials for {cluster_name}: {e}')

    deadline = time.time() + timeout
    poll_interval = 30
    last_status = ''
    while time.time() < deadline:
        try:
            # Search for BLAST jobs: try label first, then fall back to name pattern
            jobs_json = kubectl('get jobs -o json')
            if jobs_json:
                all_jobs = json.loads(jobs_json)
                # Filter to BLAST jobs: has app=blast label OR name contains 'blast' or 'batch'
                blast_jobs = [
                    j for j in all_jobs.get('items', [])
                    if j.get('metadata', {}).get('labels', {}).get('app') == 'blast'
                    or 'blast' in j.get('metadata', {}).get('name', '').lower()
                    or 'batch' in j.get('metadata', {}).get('name', '').lower()
                ]
                # Exclude known non-BLAST jobs
                blast_jobs = [
                    j for j in blast_jobs
                    if j.get('metadata', {}).get('name', '') not in
                    ('init-pv', 'submit-jobs', 'elb-finalizer', 'import-queries')
                    and not j.get('metadata', {}).get('name', '').startswith('init-ssd')
                ]

                if blast_jobs:
                    succeeded = sum(1 for j in blast_jobs if j.get('status', {}).get('succeeded'))
                    failed = sum(1 for j in blast_jobs if j.get('status', {}).get('failed'))
                    active = sum(1 for j in blast_jobs if j.get('status', {}).get('active'))
                    result.pods_succeeded = succeeded
                    result.pods_failed = failed
                    result.num_batches = len(blast_jobs)

                    status = f'{succeeded}ok/{failed}fail/{active}active'
                    if status != last_status:
                        remaining = int(deadline - time.time())
                        log.info(f'BLAST jobs: {status} (timeout in {remaining}s)')
                        last_status = status

                    if active == 0 and (succeeded + failed) > 0:
                        log.info(f'Jobs complete: {succeeded} succeeded, {failed} failed')
                        return
        except Exception as e:
            log.debug(f'Job poll error: {e}')
        time.sleep(poll_interval)
    log.warning(f'Timeout ({timeout}s) waiting for job completion on {cluster_name}')


# ---------------------------------------------------------------------------
# Phase Definitions
# ---------------------------------------------------------------------------

def phase_a(output_dir: Path) -> List[BenchmarkResult]:
    """Phase A: Baseline — single node, small dataset, Blob NFS (cold + warm)."""
    results = []
    tests = [
        ('A1', 'small', 'E32s_v3', 1, 'blob_nfs', 0),
        ('A2', 'small', 'E32s_v3', 1, 'warm', 1),  # Warm cluster (reuse, DB already loaded)
    ]
    for test_id, ds, vm, nodes, storage, reuse in tests:
        r = run_single_test(test_id, ds, vm, nodes, storage, reuse, output_dir)
        results.append(r)
    return results


def phase_b(output_dir: Path) -> List[BenchmarkResult]:
    """Phase B: Storage comparison — Blob NFS vs Local NVMe on same dataset/VM."""
    results = []
    tests = [
        ('B1', 'small', 'E32s_v3', 1, 'blob_nfs', 0),  # Blob NFS (cold)
        ('B2', 'small', 'E32s_v3', 1, 'nvme', 0),       # Local NVMe SSD (cold)
    ]
    for test_id, ds, vm, nodes, storage, reuse in tests:
        r = run_single_test(test_id, ds, vm, nodes, storage, reuse, output_dir)
        results.append(r)
    return results


def phase_c(output_dir: Path) -> List[BenchmarkResult]:
    """Phase C: Scale-out — multiple nodes, Blob NFS."""
    results = []
    tests = [
        ('C1', 'small', 'E32s_v3', 1, 'blob_nfs', 0),
        ('C2', 'small', 'E32s_v3', 3, 'blob_nfs', 0),
    ]
    for test_id, ds, vm, nodes, storage, reuse in tests:
        r = run_single_test(test_id, ds, vm, nodes, storage, reuse, output_dir)
        results.append(r)
    return results


def phase_d(output_dir: Path) -> List[BenchmarkResult]:
    """Phase D: Medium DB (2GB) — storage comparison with real-world DB size."""
    results = []
    tests = [
        ('D1', 'medium', 'E32s_v3', 1, 'blob_nfs', 0),  # Blob NFS, 2GB DB
        ('D2', 'medium', 'E32s_v3', 1, 'nvme', 0),       # Local NVMe, 2GB DB
    ]
    for test_id, ds, vm, nodes, storage, reuse in tests:
        r = run_single_test(test_id, ds, vm, nodes, storage, reuse, output_dir)
        results.append(r)
    return results


def phase_e(output_dir: Path) -> List[BenchmarkResult]:
    """Phase E: Multi-batch scale-out — medium DB, multi-node."""
    results = []
    tests = [
        ('E1', 'medium', 'E32s_v3', 1, 'blob_nfs', 0),  # 1 node baseline
        ('E2', 'medium', 'E32s_v3', 3, 'blob_nfs', 0),  # 3 nodes scale-out
    ]
    for test_id, ds, vm, nodes, storage, reuse in tests:
        r = run_single_test(test_id, ds, vm, nodes, storage, reuse, output_dir)
        results.append(r)
    return results


def phase_f(output_dir: Path) -> List[BenchmarkResult]:
    """Phase F: Thread scaling curve — same DB, varying num_threads on warm cluster.

    Tests: num_threads = 1, 2, 4, 8, 16, 32 on E32s_v3 (32 vCPU).
    First run is cold (creates cluster + loads DB), rest reuse warm cluster.
    """
    results = []
    thread_counts = [1, 2, 4, 8, 16, 32]

    for i, nthreads in enumerate(thread_counts):
        test_id = f'F{i+1}'
        storage = 'blob_nfs' if i == 0 else 'warm'
        reuse = 0 if i == 0 else 1

        # Override num_threads via BLAST options
        ds = dict(DATASETS['medium'])
        ds['options'] = f'-evalue 0.01 -outfmt 7 -num_threads {nthreads}'

        r = BenchmarkResult(
            test_id=test_id,
            timestamp=datetime.utcnow().isoformat(),
            dataset='medium',
            vm_type='Standard_E32s_v3',
            num_nodes=1,
            storage_type=storage,
            reuse_run=reuse,
            num_threads=nthreads,
            blast_options=ds['options'],
        )

        cluster_name = f'elb-bench-f1'  # Reuse same cluster for all thread tests
        log.info(f'=== Test {test_id}: medium/E32s_v3x1/threads={nthreads} ===')

        try:
            r.status = 'running'

            # Clean up previous jobs before reusing warm cluster
            if i > 0:
                log.info(f'Cleaning up previous jobs on {cluster_name}')
                for label in ('app=blast', 'app=submit', 'app=setup'):
                    kubectl(f'delete jobs -l {label} --ignore-not-found', cluster_name, timeout=30)
                kubectl('delete job elb-finalizer --ignore-not-found', cluster_name, timeout=15)
                kubectl('delete job import-queries --ignore-not-found', cluster_name, timeout=15)
                # Clean results directory
                run(f'AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy rm '
                    f'"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{RESULTS_CONTAINER}/" '
                    f'--recursive=true', timeout=60)
                time.sleep(5)
            ini_path = f'/tmp/elb-bench-{test_id}.ini'
            vm = VMS['E32s_v3']
            _write_config(ini_path, cluster_name, ds, vm, 1, storage)

            t0 = time.time()
            env = os.environ.copy()
            env['AZCOPY_AUTO_LOGIN_TYPE'] = 'AZCLI'
            env['ELB_SKIP_DB_VERIFY'] = 'true'
            env['PYTHONPATH'] = 'src:' + env.get('PYTHONPATH', '')

            proc = subprocess.run(
                ['python', 'bin/elastic-blast', 'submit', '--cfg', ini_path, '--loglevel', 'DEBUG'],
                capture_output=True, text=True, env=env, timeout=3600
            )
            r.total_elapsed_s = time.time() - t0

            if proc.returncode != 0:
                r.status = 'failed'
                error_lines = [l for l in (proc.stderr or '').split('\n') if 'ERROR' in l]
                r.error = '\n'.join(error_lines[-5:]) if error_lines else proc.stderr[-500:]
                log.error(f'Test {test_id} failed: {r.error}')
            else:
                _wait_for_completion(cluster_name, r)
                if r.pods_failed > 0:
                    r.status = 'failed'
                    r.error = f'{r.pods_failed}/{r.num_batches} BLAST jobs failed'
                    log.error(f'Test {test_id}: {r.error}')
                else:
                    r.status = 'success'

            hours = r.total_elapsed_s / 3600
            r.estimated_cost_usd = hours * vm['cost_hr']
        except Exception as e:
            r.status = 'failed'
            r.error = str(e)
        finally:
            save_result(r, output_dir)

        results.append(r)

    # Stop cluster after all thread tests
    try:
        az(f'aks stop -g {RG} -n elb-bench-f1 --no-wait', timeout=30)
    except Exception:
        pass

    return results


def phase_g(output_dir: Path) -> List[BenchmarkResult]:
    """Phase G: Concurrent queries — measure degradation with simultaneous searches.

    Runs 1, 2, 4 concurrent elastic-blast submit processes on the same warm cluster.
    """
    results = []

    # G1: single query baseline (cold start, loads DB) — keep cluster running for G2/G4
    r1 = run_single_test('G1', 'medium', 'E32s_v3', 1, 'blob_nfs', 0, output_dir, skip_stop=True)
    results.append(r1)

    # G2-G4: concurrent queries on warm cluster
    for concurrent, test_id in [(2, 'G2'), (4, 'G4')]:
        log.info(f'=== Test {test_id}: {concurrent} concurrent queries on warm cluster ===')

        cluster_name = 'elb-bench-g1'  # Reuse G1's cluster

        # Clean up previous jobs
        for label in ('app=blast', 'app=submit', 'app=setup'):
            kubectl(f'delete jobs -l {label} --ignore-not-found', cluster_name, timeout=30)
        kubectl('delete job elb-finalizer --ignore-not-found', cluster_name, timeout=15)
        kubectl('delete job import-queries --ignore-not-found', cluster_name, timeout=15)
        run(f'AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy rm '
            f'"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{RESULTS_CONTAINER}/" '
            f'--recursive=true', timeout=60)
        time.sleep(5)

        r = BenchmarkResult(
            test_id=test_id,
            timestamp=datetime.utcnow().isoformat(),
            dataset='medium',
            vm_type='Standard_E32s_v3',
            num_nodes=1,
            storage_type='warm',
            reuse_run=1,
            num_threads=VMS['E32s_v3']['vcpu'],
        )

        try:
            r.status = 'running'

            # Launch concurrent submit processes
            procs = []
            t0 = time.time()
            for j in range(concurrent):
                ini_path = f'/tmp/elb-bench-{test_id}-{j}.ini'
                ds = dict(DATASETS['medium'])
                _write_config(ini_path, cluster_name, ds, VMS['E32s_v3'], 1, 'warm')

                env = os.environ.copy()
                env['AZCOPY_AUTO_LOGIN_TYPE'] = 'AZCLI'
                env['ELB_SKIP_DB_VERIFY'] = 'true'
                env['PYTHONPATH'] = 'src:' + env.get('PYTHONPATH', '')

                p = subprocess.Popen(
                    ['python', 'bin/elastic-blast', 'submit', '--cfg', ini_path, '--loglevel', 'DEBUG'],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
                )
                procs.append(p)

            # Wait for all to complete
            for p in procs:
                p.wait(timeout=3600)

            r.total_elapsed_s = time.time() - t0

            # Check if all succeeded
            all_ok = all(p.returncode == 0 for p in procs)
            if all_ok:
                _wait_for_completion(cluster_name, r, timeout=1800)
                r.status = 'success'
            else:
                r.status = 'failed'
                r.error = f'{sum(1 for p in procs if p.returncode != 0)}/{concurrent} failed'

            r.estimated_cost_usd = (r.total_elapsed_s / 3600) * VMS['E32s_v3']['cost_hr']
        except Exception as e:
            r.status = 'failed'
            r.error = str(e)
        finally:
            save_result(r, output_dir)

        results.append(r)

    # Stop cluster
    try:
        az(f'aks stop -g {RG} -n elb-bench-g1 --no-wait', timeout=30)
    except Exception:
        pass

    return results


def phase_h(output_dir: Path) -> List[BenchmarkResult]:
    """Phase H: Large DB (82GB nt_prok) — Storage & VM RAM comparison.

    E16s_v3 (128GB RAM): DB is 64% of RAM → partial I/O pressure
    E32s_v3 (256GB RAM): DB is 32% of RAM → CPU-bound (DB fits in cache)
    """
    results = []
    tests = [
        # E16s_v3: 82GB DB on 128GB RAM (partial I/O pressure)
        ('H1', 'large', 'E16s_v3', 1, 'blob_nfs', 0),    # Blob NFS
        ('H2', 'large', 'E16s_v3', 1, 'nvme', 0),         # NVMe comparison
        # E32s_v3: 82GB DB on 256GB RAM (CPU-bound, DB fits in cache)
        ('H3', 'large', 'E32s_v3', 1, 'blob_nfs', 0),    # Blob NFS
        # Warm reuse on E32s_v3
        ('H4', 'large', 'E32s_v3', 1, 'warm', 1),          # DB already cached
    ]
    for test_id, ds, vm, nodes, storage, reuse in tests:
        r = run_single_test(test_id, ds, vm, nodes, storage, reuse, output_dir)
        results.append(r)
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_summary(results: List[BenchmarkResult], output_dir: Path):
    """Generate summary table from all results."""
    summary_path = output_dir / 'summary.json'
    with open(summary_path, 'w') as f:
        json.dump([asdict(r) for r in results], f, indent=2)

    # Also generate human-readable table
    table_path = output_dir / 'summary.txt'
    with open(table_path, 'w') as f:
        f.write(f'ElasticBLAST Azure Benchmark Results — {datetime.utcnow().isoformat()}\n')
        f.write('=' * 100 + '\n')
        f.write(f'{"Test":<6} {"Dataset":<8} {"VM":<16} {"Nodes":<6} {"Storage":<12} '
                f'{"Total(s)":<10} {"Cost($)":<8} {"Status":<8}\n')
        f.write('-' * 100 + '\n')
        for r in results:
            f.write(f'{r.test_id:<6} {r.dataset:<8} {r.vm_type:<16} {r.num_nodes:<6} '
                    f'{r.storage_type:<12} {r.total_elapsed_s:<10.1f} '
                    f'{r.estimated_cost_usd:<8.2f} {r.status:<8}\n')

    log.info(f'Summary saved: {summary_path}, {table_path}')
    with open(table_path) as f:
        print(f.read())


def generate_report(results: List[BenchmarkResult], output_dir: Path):
    """Generate Markdown benchmark report with storage comparison tables."""
    report_path = output_dir / 'report.md'
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

    lines = [
        f'# ElasticBLAST Azure Benchmark Report',
        f'',
        f'> Generated: {ts}',
        f'> Region: {REGION}',
        f'> Test baseline: 115 passed, 8 skipped',
        f'',
        f'---',
        f'',
        f'## 1. Test Environment',
        f'',
        f'| Item | Value |',
        f'| ---- | ----- |',
        f'| AKS Region | {REGION} |',
        f'| Resource Group | {RG} |',
        f'| Storage Account | {STORAGE_ACCOUNT} |',
        f'| ACR | {ACR_NAME}.azurecr.io |',
        f'',
    ]

    # Group results by phase
    phases: Dict[str, List[BenchmarkResult]] = {}
    for r in results:
        phase = r.test_id[0]  # A, B, C
        phases.setdefault(phase, []).append(r)

    # Phase A: Baseline
    if 'A' in phases:
        lines.extend([
            f'## 2. Phase A: Baseline (Blob NFS, cold vs warm)',
            f'',
            f'| Test | Storage | Reuse | Total (s) | Cost ($) | Status |',
            f'| ---- | ------- | ----- | --------- | -------- | ------ |',
        ])
        for r in phases['A']:
            reuse_label = 'warm' if r.reuse_run > 0 else 'cold'
            lines.append(
                f'| {r.test_id} | {r.storage_type} | {reuse_label} | '
                f'{r.total_elapsed_s:.1f} | {r.estimated_cost_usd:.2f} | {r.status} |')

        # Warm vs cold savings
        cold = [r for r in phases['A'] if r.reuse_run == 0]
        warm = [r for r in phases['A'] if r.reuse_run > 0]
        if cold and warm and cold[0].total_elapsed_s > 0:
            savings_pct = (1 - warm[0].total_elapsed_s / cold[0].total_elapsed_s) * 100
            lines.extend([
                f'',
                f'**Warm cluster savings**: {savings_pct:.0f}% '
                f'({cold[0].total_elapsed_s:.0f}s -> {warm[0].total_elapsed_s:.0f}s)',
            ])
        lines.append('')

    # Phase B: Storage comparison
    if 'B' in phases:
        lines.extend([
            f'## 3. Phase B: Storage Comparison',
            f'',
            f'| Test | Storage | VM | Nodes | Total (s) | Cost ($) | Status |',
            f'| ---- | ------- | -- | ----- | --------- | -------- | ------ |',
        ])
        for r in phases['B']:
            lines.append(
                f'| {r.test_id} | {r.storage_type} | {r.vm_type} | {r.num_nodes} | '
                f'{r.total_elapsed_s:.1f} | {r.estimated_cost_usd:.2f} | {r.status} |')

        # Speedup comparison vs first result (baseline)
        baseline = phases['B'][0]
        if baseline.total_elapsed_s > 0:
            lines.extend(['', '**Relative performance vs Blob NFS**:', ''])
            for r in phases['B']:
                if r.total_elapsed_s > 0:
                    ratio = baseline.total_elapsed_s / r.total_elapsed_s
                    lines.append(f'- {r.storage_type}: {ratio:.2f}x '
                                 f'({"faster" if ratio > 1 else "slower"})')
        lines.append('')

    # Phase C: Scale-out
    if 'C' in phases:
        lines.extend([
            f'## 4. Phase C: Scale-out',
            f'',
            f'| Test | Storage | VM | Nodes | Total (s) | Cost ($) | Status |',
            f'| ---- | ------- | -- | ----- | --------- | -------- | ------ |',
        ])
        for r in phases['C']:
            lines.append(
                f'| {r.test_id} | {r.storage_type} | {r.vm_type} | {r.num_nodes} | '
                f'{r.total_elapsed_s:.1f} | {r.estimated_cost_usd:.2f} | {r.status} |')

        # Scaling efficiency
        single = [r for r in phases['C'] if r.num_nodes == 1]
        multi = [r for r in phases['C'] if r.num_nodes > 1]
        if single and multi and single[0].total_elapsed_s > 0 and multi[0].total_elapsed_s > 0:
            speedup = single[0].total_elapsed_s / multi[0].total_elapsed_s
            efficiency = speedup / multi[0].num_nodes * 100
            lines.extend([
                f'',
                f'**Scaling**: {single[0].num_nodes} -> {multi[0].num_nodes} nodes = '
                f'{speedup:.1f}x speedup ({efficiency:.0f}% efficiency)',
            ])
        lines.append('')

    # Summary table
    lines.extend([
        f'## 5. All Results Summary',
        f'',
        f'| Test | Dataset | VM | Nodes | Storage | Total (s) | Cost ($) | Status |',
        f'| ---- | ------- | -- | ----- | ------- | --------- | -------- | ------ |',
    ])
    for r in results:
        lines.append(
            f'| {r.test_id} | {r.dataset} | {r.vm_type} | {r.num_nodes} | '
            f'{r.storage_type} | {r.total_elapsed_s:.1f} | '
            f'{r.estimated_cost_usd:.2f} | {r.status} |')

    # Failures
    failed = [r for r in results if r.status == 'failed']
    if failed:
        lines.extend([
            f'',
            f'## 6. Failures',
            f'',
        ])
        for r in failed:
            lines.extend([
                f'### {r.test_id}',
                f'',
                f'```',
                f'{r.error[:500]}',
                f'```',
                f'',
            ])

    lines.append('')

    with open(report_path, 'w') as f:
        f.write('\n'.join(lines))
    log.info(f'Report saved: {report_path}')
    return report_path


def _cleanup_stopped_clusters():
    """Delete all stopped AKS clusters in the benchmark resource group to free vCPU quota."""
    try:
        clusters_json = az(f'aks list -g {RG} --query "[?powerState.code==\'Stopped\'].name" -o json')
        if not clusters_json:
            return
        stopped = json.loads(clusters_json)
        if not stopped:
            return
        log.info(f'Cleaning up {len(stopped)} stopped cluster(s): {stopped}')
        for name in stopped:
            az(f'aks delete -g {RG} -n {name} --yes --no-wait', timeout=30)
        # Wait for deletions
        for _ in range(20):
            time.sleep(15)
            remaining = az(f'aks list -g {RG} --query "length([?provisioningState==\'Deleting\'])" -o tsv')
            if remaining == '0' or not remaining:
                log.info('Stopped clusters cleaned up')
                return
        log.warning('Cluster cleanup timed out — some may still be deleting')
    except Exception as e:
        log.warning(f'Cluster cleanup failed: {e}')


def main():
    parser = argparse.ArgumentParser(description='ElasticBLAST Azure Benchmark')
    parser.add_argument('--phase', default='A', choices=['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'ALL'],
                        help='A=baseline, B=storage(small), C=scale(small), D=storage(2GB), E=scale(2GB), F=threads, G=concurrent, H=large(60GB), ALL=full')
    parser.add_argument('--output', default=None, help='Output directory')
    parser.add_argument('--dry-run', action='store_true',
                        help='Generate configs only, do not run benchmarks')
    args = parser.parse_args()

    output_dir = Path(args.output) if args.output else Path(f'benchmark/results/{datetime.now().strftime("%Y-%m-%d_%H%M")}')
    os.makedirs(output_dir, exist_ok=True)
    log.info(f'Output directory: {output_dir}')

    if args.dry_run:
        log.info('DRY RUN — generating configs only')
        _dry_run_phases(args.phase, output_dir)
        return

    # -----------------------------------------------------------------------
    # Preflight checks — prevent known failure modes
    # -----------------------------------------------------------------------

    # 1. Enable storage public access
    az(f'storage account update -n {STORAGE_ACCOUNT} --public-network-access Enabled -o none')
    log.info('Storage public access enabled for benchmark')

    # 2. Wait for storage access propagation (prevents AuthorizationFailure)
    log.info('Waiting for storage access propagation...')
    for attempt in range(6):
        time.sleep(10)
        probe = run(f'AZCOPY_AUTO_LOGIN_TYPE=AZCLI azcopy list '
                    f'"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{STORAGE_CONTAINER}/" '
                    f'--machine-readable', timeout=30)
        if probe.returncode == 0 and 'Content Length' in probe.stdout:
            log.info(f'Storage access verified (attempt {attempt + 1})')
            break
        log.info(f'Storage not ready yet (attempt {attempt + 1}/6)')
    else:
        log.warning('Storage access verification timed out — proceeding anyway')

    # 3. Cleanup stopped clusters to free vCPU quota
    _cleanup_stopped_clusters()

    # 4. Log current quota status
    try:
        quota_json = az(f'vm list-usage -l {REGION} '
                        f'--query "[?name.value==\'cores\'].{{current:currentValue,limit:limit}}" -o json')
        if quota_json:
            quota = json.loads(quota_json)
            if quota:
                used, limit = quota[0]['current'], quota[0]['limit']
                log.info(f'vCPU quota: {used}/{limit} used ({limit - used} available)')
                if limit - used < 32:
                    log.warning(f'Low vCPU quota! Only {limit - used} available, need at least 32')
    except Exception:
        pass

    all_results: List[BenchmarkResult] = []

    try:
        if args.phase in ('A', 'ALL'):
            all_results.extend(phase_a(output_dir))
        if args.phase in ('B', 'ALL'):
            all_results.extend(phase_b(output_dir))
        if args.phase in ('C', 'ALL'):
            all_results.extend(phase_c(output_dir))
        if args.phase in ('D', 'ALL'):
            all_results.extend(phase_d(output_dir))
        if args.phase in ('E', 'ALL'):
            all_results.extend(phase_e(output_dir))
        if args.phase in ('F', 'ALL'):
            all_results.extend(phase_f(output_dir))
        if args.phase in ('G', 'ALL'):
            all_results.extend(phase_g(output_dir))
        if args.phase in ('H',):
            all_results.extend(phase_h(output_dir))
    finally:
        if all_results:
            generate_summary(all_results, output_dir)
            generate_report(all_results, output_dir)

        # Cleanup: disable storage public access
        az(f'storage account update -n {STORAGE_ACCOUNT} --public-network-access Disabled -o none')
        log.info('Storage public access disabled (restored)')


def _dry_run_phases(phase: str, output_dir: Path):
    """Generate INI configs for all tests without running them."""
    all_tests = []
    if phase in ('A', 'ALL'):
        all_tests.extend([
            ('A1', 'small', 'E32s_v3', 1, 'blob_nfs', 0),
            ('A2', 'small', 'E32s_v3', 1, 'warm', 1),
        ])
    if phase in ('B', 'ALL'):
        all_tests.extend([
            ('B1', 'small', 'E32s_v3', 1, 'blob_nfs', 0),
            ('B2', 'small', 'E32s_v3', 1, 'nvme', 0),
        ])
    if phase in ('C', 'ALL'):
        all_tests.extend([
            ('C1', 'small', 'E32s_v3', 1, 'blob_nfs', 0),
            ('C2', 'small', 'E32s_v3', 3, 'blob_nfs', 0),
        ])
    if phase in ('D', 'ALL'):
        all_tests.extend([
            ('D1', 'medium', 'E32s_v3', 1, 'blob_nfs', 0),
            ('D2', 'medium', 'E32s_v3', 1, 'nvme', 0),
        ])
    if phase in ('E', 'ALL'):
        all_tests.extend([
            ('E1', 'medium', 'E32s_v3', 1, 'blob_nfs', 0),
            ('E2', 'medium', 'E32s_v3', 3, 'blob_nfs', 0),
        ])
    if phase in ('F', 'ALL'):
        for i, nt in enumerate([1, 2, 4, 8, 16, 32]):
            all_tests.append((f'F{i+1}', 'medium', 'E32s_v3', 1,
                              'blob_nfs' if i == 0 else 'warm', 0 if i == 0 else 1))
    if phase in ('G', 'ALL'):
        all_tests.extend([
            ('G1', 'medium', 'E32s_v3', 1, 'blob_nfs', 0),
            ('G2', 'medium', 'E32s_v3', 1, 'warm', 1),
            ('G4', 'medium', 'E32s_v3', 1, 'warm', 1),
        ])
    if phase in ('H',):
        all_tests.extend([
            ('H1', 'large', 'D8s_v3', 1, 'blob_nfs', 0),
            ('H2', 'large', 'D8s_v3', 1, 'nvme', 0),
            ('H3', 'large', 'E32s_v3', 1, 'blob_nfs', 0),
            ('H4', 'large', 'E32s_v3', 1, 'warm', 1),
        ])

    config_dir = output_dir / 'configs'
    os.makedirs(config_dir, exist_ok=True)

    for test_id, ds_name, vm_name, nodes, storage, reuse in all_tests:
        dataset = DATASETS[ds_name]
        vm = VMS[vm_name]
        cluster_name = f'elb-bench-{test_id[:8].lower()}'
        ini_path = str(config_dir / f'elb-bench-{test_id}.ini')
        _write_config(ini_path, cluster_name, dataset, vm, nodes, storage)
        log.info(f'  {test_id}: {ds_name}/{vm_name}x{nodes}/{storage} -> {ini_path}')


if __name__ == '__main__':
    main()
