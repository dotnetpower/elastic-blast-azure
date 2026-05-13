"""
elastic_blast/azure.py — Azure AKS implementation of ElasticBLAST

Manages the lifecycle of BLAST searches on Azure Kubernetes Service:
cluster creation, DB initialization, job submission, status checking, and cleanup.

Includes:
- Azure SDK clients (AzureClients, AKS/IAM/Disk operations)
- Application Insights telemetry (OpenTelemetry integration)
- Optimization profiles (cost/balanced/performance)
- Module-level resource management functions

Authors: Moon Hyuk Choi moonchoi@microsoft.com
"""

import os
import re
import shlex
import time
import logging
import threading
import math
import uuid
import tempfile
import base64
from pathlib import Path
from tempfile import TemporaryDirectory
from timeit import default_timer as timer
from typing import Any, DefaultDict, Dict, Optional, List, Tuple
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from tenacity import retry, stop_after_attempt, wait_exponential

from azure.identity import DefaultAzureCredential  # type: ignore
from azure.mgmt.containerservice import ContainerServiceClient  # type: ignore
from azure.mgmt.compute import ComputeManagementClient  # type: ignore
from azure.mgmt.storage import StorageManagementClient  # type: ignore
from azure.mgmt.authorization import AuthorizationManagementClient  # type: ignore
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError  # type: ignore

from .base import MemoryStr
from .subst import substitute_params
from .filehelper import open_for_write_immediate
from .jobs import read_job_template, write_job_files
from .util import (ElbSupportedPrograms, safe_exec, UserReportError, SafeExecError,
                   get_blastdb_info, get_usage_reporting, handle_error)
from . import kubernetes
from .constants import (
    CLUSTER_ERROR, DEPENDENCY_ERROR, INPUT_ERROR,
    ELB_NUM_JOBS_SUBMITTED, ELB_METADATA_DIR, ELB_STATE_DISK_ID_FILE,
    ELB_QUERY_BATCH_DIR, ELB_QUERY_LENGTH,
    K8S_JOB_CLOUD_SPLIT_SSD, K8S_JOB_INIT_PV, K8S_JOB_BLAST,
    K8S_JOB_GET_BLASTDB, K8S_JOB_IMPORT_QUERY_BATCHES,
    K8S_JOB_LOAD_BLASTDB_INTO_RAM, K8S_JOB_RESULTS_EXPORT, K8S_JOB_SUBMIT_JOBS,
    ELB_DFLT_BLAST_JOB_AKS_TEMPLATE, ELB_LOCAL_SSD_BLAST_JOB_AKS_TEMPLATE,
    ElbExecutionMode, ElbStatus, AKS_PROVISIONING_STATE, STATUS_MESSAGE_ERROR,
)
from .elb_config import ElasticBlastConfig, ResourceIds
from .elasticblast import ElasticBlast
from . import VERSION
from .azure_traits import (
    AZURE_VM_HOURLY_PRICES, SPOT_DISCOUNT_FACTOR,
    get_machine_properties, apply_auto_partition,
)


# ===========================================================================
# Azure SDK Clients
# ===========================================================================

class AzureClients:
    """Lazy-initialized Azure SDK clients using DefaultAzureCredential."""

    def __init__(self, subscription_id: str):
        self._subscription_id = subscription_id
        self._credential = DefaultAzureCredential()
        self._aks: Optional[ContainerServiceClient] = None
        self._compute: Optional[ComputeManagementClient] = None
        self._storage: Optional[StorageManagementClient] = None
        self._auth: Optional[AuthorizationManagementClient] = None

    @property
    def aks(self) -> ContainerServiceClient:
        if self._aks is None:
            self._aks = ContainerServiceClient(self._credential, self._subscription_id)
        return self._aks

    @property
    def compute(self) -> ComputeManagementClient:
        if self._compute is None:
            self._compute = ComputeManagementClient(self._credential, self._subscription_id)
        return self._compute

    @property
    def storage(self) -> StorageManagementClient:
        if self._storage is None:
            self._storage = StorageManagementClient(self._credential, self._subscription_id)
        return self._storage

    @property
    def auth(self) -> AuthorizationManagementClient:
        if self._auth is None:
            self._auth = AuthorizationManagementClient(self._credential, self._subscription_id)
        return self._auth

    @property
    def subscription_id(self) -> str:
        return self._subscription_id


_clients: Optional[AzureClients] = None


def _get_subscription_id() -> str:
    """Get Azure subscription ID from az CLI profile or Azure REST API."""
    import subprocess
    try:
        result = subprocess.run(
            ['az', 'account', 'show', '--query', 'id', '-o', 'tsv'],
            capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    import requests
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/.default")
    resp = requests.get(
        "https://management.azure.com/subscriptions?api-version=2022-12-01",
        headers={"Authorization": f"Bearer {token.token}"})
    resp.raise_for_status()
    subs = resp.json().get("value", [])
    if subs:
        return subs[0]["subscriptionId"]
    raise RuntimeError("No Azure subscription found")


def _get_clients() -> AzureClients:
    """Get or create the module-level AzureClients singleton."""
    global _clients
    if _clients is None:
        _clients = AzureClients(_get_subscription_id())
    return _clients


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def _sdk_check_cluster(resource_group: str, cluster_name: str, dry_run: bool = False) -> str:
    if dry_run:
        return ''
    try:
        clients = _get_clients()
        cluster = clients.aks.managed_clusters.get(resource_group, cluster_name)
        return cluster.provisioning_state or ''
    except ResourceNotFoundError:
        return ''
    except HttpResponseError as e:
        logging.warning(f'Error checking cluster {cluster_name}: {e.message}')
        return ''


def _sdk_start_cluster(resource_group: str, cluster_name: str, *,
                       location: str, machine_type: str, num_nodes: int,
                       use_local_ssd: bool = False, use_spot: bool = False,
                       tags: Optional[Dict[str, str]] = None,
                       k8s_version: Optional[str] = None,
                       dry_run: bool = False) -> Optional[Any]:
    if not re.match(r'^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$', cluster_name):
        raise ValueError(
            f'Invalid AKS cluster name "{cluster_name}". '
            'Must be 1-63 lowercase alphanumeric characters or hyphens.')

    agent_pool_profile = {
        "name": "nodepool1", "count": num_nodes, "vm_size": machine_type,
        "os_disk_type": "Managed", "mode": "System",
        "enable_auto_scaling": False, "type": "VirtualMachineScaleSets",
    }
    if use_spot:
        logging.warning('Spot VM requested but system pool does not support it.')

    cluster_params: Dict[str, Any] = {
        "location": location, "tags": tags or {},
        "identity": {"type": "SystemAssigned"}, "dnsPrefix": cluster_name,
        "kubernetes_version": k8s_version if k8s_version else None,
        "auto_upgrade_profile": {"upgrade_channel": "none"},
        "agent_pool_profiles": [agent_pool_profile],
        "network_profile": {"load_balancer_sku": "standard"},
        "storage_profile": {"blob_csi_driver": {"enabled": not use_local_ssd}},
    }

    if dry_run:
        logging.info(f'SDK: start_cluster({resource_group}, {cluster_name}, vm={machine_type}, nodes={num_nodes})')
        return None

    try:
        clients = _get_clients()
        start = timer()
        logging.info(f'Creating AKS cluster {cluster_name} in {resource_group} ({machine_type} x {num_nodes})')
        poller = clients.aks.managed_clusters.begin_create_or_update(
            resource_group, cluster_name, cluster_params)
        logging.debug(f'RUNTIME cluster-create-request {timer() - start:.1f}s')
        return poller
    except HttpResponseError as e:
        raise UserReportError(CLUSTER_ERROR,
            f'Failed to create AKS cluster {cluster_name}: {e.message}') from e
    except Exception as e:
        raise UserReportError(CLUSTER_ERROR,
            f'Azure SDK error creating cluster {cluster_name}: {e}') from e


def _sdk_delete_cluster(resource_group: str, cluster_name: str, dry_run: bool = False) -> Optional[Any]:
    if dry_run:
        return None
    try:
        clients = _get_clients()
        poller = clients.aks.managed_clusters.begin_delete(resource_group, cluster_name)
        return poller
    except ResourceNotFoundError:
        logging.info(f'Cluster {cluster_name} not found, nothing to delete')
        return None
    except HttpResponseError as e:
        logging.warning(f'Failed to delete cluster {cluster_name}: {e.message}')
        return None


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def _sdk_get_aks_clusters(resource_group: str, dry_run: bool = False) -> List[str]:
    if dry_run:
        return []
    clients = _get_clients()
    return [c.name for c in clients.aks.managed_clusters.list_by_resource_group(resource_group)]


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def _sdk_get_aks_credentials(resource_group: str, cluster_name: str, dry_run: bool = False) -> str:
    if dry_run:
        return 'k8s-uninitialized-context'
    try:
        clients = _get_clients()
        cred_result = clients.aks.managed_clusters.list_cluster_user_credentials(resource_group, cluster_name)
    except ResourceNotFoundError:
        raise UserReportError(CLUSTER_ERROR,
            f'Cluster {cluster_name} not found in resource group {resource_group}.')
    except HttpResponseError as e:
        raise UserReportError(CLUSTER_ERROR,
            f'Failed to get credentials for cluster {cluster_name}: {e.message}')

    if not cred_result.kubeconfigs:
        raise UserReportError(CLUSTER_ERROR, f'No kubeconfig returned for cluster {cluster_name}')

    kubeconfig_bytes = cred_result.kubeconfigs[0].value
    kube_dir = os.path.expanduser('~/.kube')
    os.makedirs(kube_dir, exist_ok=True)
    kube_config_path = os.path.join(kube_dir, 'config')

    with tempfile.NamedTemporaryFile(mode='wb', suffix='.yaml', delete=False) as tmp:
        tmp.write(kubeconfig_bytes)
        tmp_path = tmp.name
    try:
        env = os.environ.copy()
        existing = env.get('KUBECONFIG', kube_config_path)
        env['KUBECONFIG'] = f'{existing}:{tmp_path}'
        import subprocess
        with open(kube_config_path, 'w') as kube_out:
            subprocess.run(['kubectl', 'config', 'view', '--flatten'],
                           stdout=kube_out, env=env, check=True)
    finally:
        os.unlink(tmp_path)

    p = safe_exec('kubectl config current-context'.split())
    return handle_error(p.stdout).strip()


def _sdk_scale_node_pool(resource_group: str, cluster_name: str,
                         pool_name: str, node_count: int,
                         dry_run: bool = False) -> Optional[Any]:
    if dry_run:
        return None
    clients = _get_clients()
    logging.info(f'Scaling AKS node pool {pool_name} to {node_count} nodes')
    return clients.aks.agent_pools.begin_create_or_update(
        resource_group, cluster_name, pool_name, {"count": node_count})


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def _sdk_set_role_assignments(resource_group: str, cluster_name: str,
                              storage_account: str, acr_name: str,
                              acr_resource_group: str, dry_run: bool = False) -> None:
    if dry_run:
        return
    clients = _get_clients()
    sub_id = clients.subscription_id
    cluster = clients.aks.managed_clusters.get(resource_group, cluster_name)
    kubelet_id = cluster.identity_profile["kubeletidentity"].object_id

    sa = clients.storage.storage_accounts.get_properties(resource_group, storage_account)
    acr_id = f'/subscriptions/{sub_id}/resourceGroups/{acr_resource_group}/providers/Microsoft.ContainerRegistry/registries/{acr_name}'

    ROLE_DEFS = {
        'Storage Blob Data Contributor': 'ba92f5b4-2d11-453d-a403-e96b0029c9fe',
        'AcrPull': '7f951dda-4ed3-4680-a7ca-43fe172d538d',
        'Contributor': 'b24988ac-6180-42a0-ab88-20f7382dd24c',
    }
    for role_name, scope in [
        ('Storage Blob Data Contributor', sa.id),
        ('AcrPull', acr_id),
        ('Contributor', f'/subscriptions/{sub_id}'),
    ]:
        role_def_id = f'/subscriptions/{sub_id}/providers/Microsoft.Authorization/roleDefinitions/{ROLE_DEFS[role_name]}'
        try:
            clients.auth.role_assignments.create(
                scope, str(uuid.uuid4()),
                {"role_definition_id": role_def_id, "principal_id": kubelet_id,
                 "principal_type": "ServicePrincipal"})
        except HttpResponseError as e:
            if 'RoleAssignmentExists' not in str(e):
                raise


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def _sdk_get_disks(resource_group: str, dry_run: bool = False) -> List[str]:
    if dry_run:
        return []
    return [d.name for d in _get_clients().compute.disks.list_by_resource_group(resource_group)]


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def _sdk_get_snapshots(resource_group: str, dry_run: bool = False) -> List[str]:
    if dry_run:
        return []
    return [s.name for s in _get_clients().compute.snapshots.list_by_resource_group(resource_group)]


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def _sdk_delete_disk(resource_group: str, disk_name: str) -> None:
    if not disk_name:
        raise ValueError('No disk name provided')
    _get_clients().compute.disks.begin_delete(resource_group, disk_name).result()


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def _sdk_delete_snapshot(resource_group: str, snapshot_name: str) -> None:
    if not snapshot_name:
        raise ValueError('No snapshot name provided')
    _get_clients().compute.snapshots.begin_delete(resource_group, snapshot_name).result()


def _sdk_check_prerequisites() -> None:
    import shutil
    try:
        p = safe_exec('kubectl version --output=json --client=true')
        logging.debug(f'{":".join(p.stdout.decode().split())}')
    except SafeExecError as e:
        raise UserReportError(DEPENDENCY_ERROR,
            f"Required pre-requisite 'kubectl' doesn't work. Details: {e.message}")
    if shutil.which('azcopy') is None:
        raise UserReportError(DEPENDENCY_ERROR,
            "Required pre-requisite 'azcopy' is not installed.")
    try:
        DefaultAzureCredential().get_token("https://management.azure.com/.default")
    except Exception as e:
        raise UserReportError(DEPENDENCY_ERROR,
            f"Azure authentication failed. Run 'az login'. Details: {e}")


# ===========================================================================
# Application Insights Telemetry
#
# Activated by setting APPLICATIONINSIGHTS_CONNECTION_STRING env var.
# When unset, all track_* functions are no-ops (zero overhead).
# See docs/azure-app-insights.md for setup instructions.
# ===========================================================================

_MONITOR_CONN_STR = os.environ.get('APPLICATIONINSIGHTS_CONNECTION_STRING', '')
_monitor_initialized = False
_monitor_init_lock = threading.Lock()
_monitor_tracer = None
_monitor_meter = None
_monitor_jobs_submitted = None
_monitor_jobs_failed = None
_monitor_cluster_create_duration = None


def _ensure_monitor_initialized():
    global _monitor_initialized, _monitor_tracer, _monitor_meter
    global _monitor_jobs_submitted, _monitor_jobs_failed, _monitor_cluster_create_duration

    if _monitor_initialized:
        return
    with _monitor_init_lock:
        if _monitor_initialized:
            return
        _monitor_initialized = True

    if not _MONITOR_CONN_STR:
        return

    try:
        from opentelemetry import trace, metrics  # type: ignore[import-untyped]
        from opentelemetry.sdk.trace import TracerProvider  # type: ignore[import-untyped]
        from opentelemetry.sdk.metrics import MeterProvider  # type: ignore[import-untyped]
        from azure.monitor.opentelemetry.exporter import (  # type: ignore[import-untyped]
            AzureMonitorTraceExporter, AzureMonitorMetricExporter)
        from opentelemetry.sdk.trace.export import BatchSpanProcessor  # type: ignore[import-untyped]
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader  # type: ignore[import-untyped]

        tp = TracerProvider()
        tp.add_span_processor(BatchSpanProcessor(AzureMonitorTraceExporter(connection_string=_MONITOR_CONN_STR)))
        trace.set_tracer_provider(tp)
        _monitor_tracer = trace.get_tracer('elastic_blast.azure')

        mp = MeterProvider(metric_readers=[PeriodicExportingMetricReader(
            AzureMonitorMetricExporter(connection_string=_MONITOR_CONN_STR), export_interval_millis=60000)])
        metrics.set_meter_provider(mp)
        _monitor_meter = metrics.get_meter('elastic_blast.azure')
        _monitor_jobs_submitted = _monitor_meter.create_counter(
            'elb.jobs.submitted', description='Total BLAST jobs submitted')
        _monitor_jobs_failed = _monitor_meter.create_counter(
            'elb.jobs.failed', description='BLAST jobs failed')
        _monitor_cluster_create_duration = _monitor_meter.create_histogram(
            'elb.cluster.create_duration_s', description='Cluster creation time in seconds')
        logging.info('Azure Monitor: initialized')
    except ImportError:
        logging.debug('Azure Monitor: opentelemetry SDK not installed')
    except Exception as e:
        logging.warning(f'Azure Monitor: init failed ({e})')


def track_search_submitted(*, job_id: str, program: str, db: str,
                            num_jobs: int, num_nodes: int, machine_type: str) -> None:
    try:
        _ensure_monitor_initialized()
        if _monitor_tracer:
            with _monitor_tracer.start_as_current_span('elb.search.submit') as span:
                span.set_attribute('elb.job_id', job_id)
                span.set_attribute('elb.program', program)
                span.set_attribute('elb.db', db)
                span.set_attribute('elb.num_jobs', num_jobs)
                span.set_attribute('elb.num_nodes', num_nodes)
                span.set_attribute('elb.machine_type', machine_type)
        if _monitor_jobs_submitted:
            _monitor_jobs_submitted.add(num_jobs, {'program': program, 'db': db})
    except Exception as e:
        logging.debug(f'Telemetry error (search_submitted): {e}')


def track_search_completed(*, job_id: str, succeeded: int, failed: int,
                            program: str = '', db: str = '') -> None:
    try:
        _ensure_monitor_initialized()
        if _monitor_tracer:
            with _monitor_tracer.start_as_current_span('elb.search.complete') as span:
                span.set_attribute('elb.job_id', job_id)
                span.set_attribute('elb.succeeded', succeeded)
                span.set_attribute('elb.failed', failed)
                span.set_attribute('elb.program', program)
                span.set_attribute('elb.db', db)
        if _monitor_jobs_failed and failed > 0:
            _monitor_jobs_failed.add(failed, {'program': program, 'db': db})
    except Exception as e:
        logging.debug(f'Telemetry error (search_completed): {e}')


def track_cluster_created(*, cluster_name: str, duration_s: float,
                           num_nodes: int, machine_type: str) -> None:
    try:
        _ensure_monitor_initialized()
        if _monitor_tracer:
            with _monitor_tracer.start_as_current_span('elb.cluster.create') as span:
                span.set_attribute('elb.cluster_name', cluster_name)
                span.set_attribute('elb.duration_s', duration_s)
                span.set_attribute('elb.num_nodes', num_nodes)
                span.set_attribute('elb.machine_type', machine_type)
        if _monitor_cluster_create_duration:
            _monitor_cluster_create_duration.record(duration_s)
    except Exception as e:
        logging.debug(f'Telemetry error (cluster_created): {e}')


def track_cluster_deleted(*, cluster_name: str, duration_s: float,
                           num_nodes: int = 0, machine_type: str = '') -> None:
    try:
        _ensure_monitor_initialized()
        if _monitor_tracer:
            with _monitor_tracer.start_as_current_span('elb.cluster.delete') as span:
                span.set_attribute('elb.cluster_name', cluster_name)
                span.set_attribute('elb.duration_s', duration_s)
                span.set_attribute('elb.num_nodes', num_nodes)
                span.set_attribute('elb.machine_type', machine_type)
    except Exception as e:
        logging.debug(f'Telemetry error (cluster_deleted): {e}')


# ===========================================================================
# Optimization Profiles
# ===========================================================================

class OptimizationProfile(str, Enum):
    COST = 'cost'
    BALANCED = 'balanced'
    PERFORMANCE = 'performance'


_VM_PROFILES = {
    OptimizationProfile.COST: {
        'default_vm': 'Standard_D8s_v3', 'large_db_vm': 'Standard_E16s_v3',
        'spot': True, 'vmtouch_pct': 0.5, 'azcopy_concurrency': 16,
        'skip_db_verify': True, 'threads_per_pod': 2, 'mem_limit_gb': 4, 'mem_request_gb': 2,
    },
    OptimizationProfile.BALANCED: {
        'default_vm': 'Standard_E32s_v3', 'large_db_vm': 'Standard_E32s_v3',
        'spot': True, 'vmtouch_pct': 0.8, 'azcopy_concurrency': 64,
        'skip_db_verify': True, 'threads_per_pod': 8, 'mem_limit_gb': 8, 'mem_request_gb': 4,
    },
    OptimizationProfile.PERFORMANCE: {
        'default_vm': 'Standard_E64bs_v5', 'large_db_vm': 'Standard_E64bs_v5',
        'spot': False, 'vmtouch_pct': 0.9, 'azcopy_concurrency': 128,
        'skip_db_verify': False, 'threads_per_pod': 16, 'mem_limit_gb': 32, 'mem_request_gb': 16,
    },
}

_VM_SPECS = {
    'Standard_D8s_v3':   (8,   32,   0.384),  'Standard_D16s_v3':  (16,  64,   0.768),
    'Standard_D32s_v3':  (32,  128,  1.536),   'Standard_E16s_v3':  (16,  128,  1.008),
    'Standard_E32s_v3':  (32,  256,  2.016),   'Standard_E64s_v3':  (64,  432,  3.629),
    'Standard_E32bs_v5': (32,  256,  2.432),   'Standard_E64bs_v5': (64,  512,  4.864),
    'Standard_E96bs_v5': (96,  672,  7.296),   'Standard_L32s_v3':  (32,  256,  2.496),
    'Standard_L64s_v3':  (64,  512,  4.992),
}


@dataclass
class Prediction:
    """Predicted time and cost for a search."""
    profile: str;  vm_type: str;  num_nodes: int;  use_spot: bool
    estimated_hours: float;  estimated_cost: float
    overhead_minutes: float;  blast_minutes: float;  db_cached_pct: float
    num_pods: int = 0;  threads_per_pod: int = 16
    mem_limit_gb: float = 254;  mem_request_gb: float = 0.5
    pods_per_node: int = 0;  cpu_utilization_pct: float = 0
    batch_len: int = 100000;  db_size_gb: float = 0;  query_size_gb: float = 0

    def __str__(self) -> str:
        spot_label = ' (Spot)' if self.use_spot else ''
        return (
            f'  Profile: {self.profile.upper()}\n'
            f'  VM: {self.vm_type}{spot_label} x {self.num_nodes} nodes\n'
            f'  DB: {self.db_size_gb:.1f} GB  |  Query: {self.query_size_gb:.2f} GB  |  DB cache: {self.db_cached_pct:.0f}%\n'
            f'  Pods: {self.num_pods} (batch-len={self.batch_len:,})  |  '
            f'{self.threads_per_pod} threads/pod  |  '
            f'mem: {self.mem_request_gb:.0f}G req / {self.mem_limit_gb:.0f}G limit\n'
            f'  Per node: ~{self.pods_per_node} pods  |  CPU utilization: ~{self.cpu_utilization_pct:.0f}%\n'
            f'  Estimated time: {self.estimated_hours:.1f}h '
            f'(overhead {self.overhead_minutes:.0f}min + BLAST {self.blast_minutes:.0f}min)\n'
            f'  Estimated cost: ${self.estimated_cost:.2f}')


def get_profile() -> OptimizationProfile:
    value = os.environ.get('ELB_OPTIMIZATION', 'balanced').lower()
    try:
        return OptimizationProfile(value)
    except ValueError:
        logging.warning(f'Unknown optimization profile "{value}", using balanced')
        return OptimizationProfile.BALANCED


def predict(profile: OptimizationProfile, *, query_size_gb: float,
            db_size_gb: float, batch_len: int = 100000,
            num_batches: Optional[int] = None, num_nodes: Optional[int] = None,
            vm_type: Optional[str] = None) -> Prediction:
    cfg = _VM_PROFILES[profile]
    if not vm_type:
        vm_type = cfg['large_db_vm'] if db_size_gb > 100 else cfg['default_vm']
    vcpu, ram_gb, hourly = _VM_SPECS.get(vm_type, (32, 256, 2.0))
    if not num_batches or num_batches <= 0:
        num_batches = max(1, int(query_size_gb * 1024 * 1024 / batch_len))
    if not num_nodes:
        min_for_db = max(1, math.ceil(db_size_gb / (ram_gb * cfg['vmtouch_pct'])))
        min_for_q = max(1, math.ceil(num_batches / (5 if profile == OptimizationProfile.PERFORMANCE else 20)))
        max_n = {OptimizationProfile.COST: 10, OptimizationProfile.BALANCED: 50}.get(profile, 200)
        num_nodes = max(min_for_db, min(min_for_q, max_n))
    use_spot = cfg['spot']
    overhead = max(15, max(5, query_size_gb * 30)) + 2
    db_cache_pct = min(100, (ram_gb * cfg['vmtouch_pct'] * num_nodes / max(db_size_gb, 1)) * 100)
    batch_time = 29 * (1 - db_cache_pct / 100) + 1 + 14 + 1
    blast_minutes = (num_batches * batch_time) / num_nodes
    total_hours = (overhead + blast_minutes) / 60
    price = hourly * (SPOT_DISCOUNT_FACTOR if use_spot else 1.0)
    tpp = cfg['threads_per_pod']
    return Prediction(
        profile=profile.value, vm_type=vm_type, num_nodes=num_nodes,
        use_spot=use_spot, estimated_hours=total_hours,
        estimated_cost=price * num_nodes * total_hours,
        overhead_minutes=overhead, blast_minutes=blast_minutes,
        db_cached_pct=db_cache_pct, num_pods=num_batches,
        threads_per_pod=tpp, mem_limit_gb=cfg['mem_limit_gb'],
        mem_request_gb=cfg['mem_request_gb'],
        pods_per_node=max(1, vcpu // tpp),
        cpu_utilization_pct=min(100.0, (num_batches / max(num_nodes, 1) * tpp / max(vcpu, 1)) * 100),
        batch_len=batch_len, db_size_gb=db_size_gb, query_size_gb=query_size_gb)


def predict_all_profiles(*, query_size_gb: float, db_size_gb: float,
                          batch_len: int = 100000, num_batches: Optional[int] = None) -> str:
    lines = [f'Optimization Profiles (query={query_size_gb:.2f}GB, db={db_size_gb:.1f}GB, batch-len={batch_len:,})']
    for profile in OptimizationProfile:
        p = predict(profile, query_size_gb=query_size_gb, db_size_gb=db_size_gb,
                    batch_len=batch_len, num_batches=num_batches)
        spot = 'Spot' if p.use_spot else 'On-demand'
        lines.append(f'  {profile.value.upper():>11s}: {p.vm_type} x{p.num_nodes} ({spot}) '
                     f'~{p.estimated_hours:.1f}h ~${p.estimated_cost:.0f} '
                     f'cache={p.db_cached_pct:.0f}%')
    return '\n'.join(lines)


def apply_profile(cfg, profile: Optional[OptimizationProfile] = None,
                   query_size_gb: float = 0, db_size_gb: float = 0) -> Prediction:
    if profile is None:
        profile = get_profile()
    p_cfg = _VM_PROFILES[profile]
    if query_size_gb <= 0:
        query_size_gb = float(os.environ.get('ELB_QUERY_SIZE_GB', '0.1'))
    if db_size_gb <= 0:
        db_size_gb = float(os.environ.get('ELB_DB_SIZE_GB', '10'))
    pred = predict(profile, query_size_gb=query_size_gb, db_size_gb=db_size_gb,
                   batch_len=cfg.blast.batch_len,
                   num_nodes=cfg.cluster.num_nodes if cfg.cluster.num_nodes > 1 else None,
                   vm_type=cfg.cluster.machine_type if cfg.cluster.machine_type != 'Standard_E32s_v3' else None)
    if str(cfg.cluster.mem_request) == '0.5G':
        cfg.cluster.mem_request = MemoryStr(f'{p_cfg["mem_request_gb"]}G')
    from .constants import ELB_DFLT_AZURE_NUM_CPUS
    if cfg.cluster.num_cpus == ELB_DFLT_AZURE_NUM_CPUS:
        cfg.cluster.num_cpus = p_cfg['threads_per_pod']
    os.environ['AZCOPY_CONCURRENCY_VALUE'] = str(p_cfg['azcopy_concurrency'])
    if p_cfg['skip_db_verify']:
        os.environ['ELB_SKIP_DB_VERIFY'] = 'true'
    if profile in (OptimizationProfile.COST, OptimizationProfile.BALANCED):
        cfg.cluster.reuse = True
    logging.info(f'Applied optimization profile: {profile.value}')
    return pred


# ---------------------------------------------------------------------------
# ElasticBlastAzure — main class
# ---------------------------------------------------------------------------

class ElasticBlastAzure(ElasticBlast):
    """Azure AKS implementation of ElasticBLAST."""

    # Storage class for PVC creation.
    # Resolved from: (1) cfg [cluster] storage-class, (2) ELB_STORAGE_CLASS env var, (3) default
    # Values: azureblob-nfs-premium (default), azure-netapp-ultra (ANF)
    STORAGE_CLASS = os.environ.get('ELB_STORAGE_CLASS', 'azureblob-nfs-premium')

    def __init__(self, cfg: ElasticBlastConfig, create=False,
                 cleanup_stack: Optional[List[Any]] = None):
        super().__init__(cfg, create, cleanup_stack)
        # Override STORAGE_CLASS from config if set
        if cfg.cluster.storage_class:
            self.STORAGE_CLASS = cfg.cluster.storage_class
        self.query_files: List[str] = []
        self.cluster_initialized = False
        self.auto_shutdown = 'ELB_DISABLE_AUTO_SHUTDOWN' not in os.environ

    def _safe_collect_logs(self) -> None:
        """Collect K8s logs only if the kubectl context is available."""
        if self.cfg.appstate.k8s_ctx:
            kubernetes.collect_k8s_logs(self.cfg)

    # -- Query splitting ----------------------------------------------------

    def cloud_query_split(self, query_files: List[str]) -> None:
        """Submit query sequences for cloud-based splitting."""
        if self.dry_run:
            return
        self.query_files = query_files
        self._initialize_cluster()
        self.cluster_initialized = True

    def wait_for_cloud_query_split(self) -> None:
        """Block until cloud query split job completes."""
        if not self.query_files:
            return
        kubectl = self._kubectl()
        job = K8S_JOB_CLOUD_SPLIT_SSD if self.cfg.cluster.use_local_ssd else K8S_JOB_INIT_PV

        while True:
            if self.dry_run:
                return
            active = self._kubectl_jsonpath(f'get job {job}',
                                            '{.items[?(@.status.active)].metadata.name}')
            if not active:
                failed = self._kubectl_jsonpath(f'get job {job}',
                                               '{.items[?(@.status.failed)].metadata.name}')
                if failed:
                    msg = ('BLASTDB initialization failed' if job == K8S_JOB_INIT_PV
                           else 'Cloud query splitting failed')
                    raise UserReportError(returncode=CLUSTER_ERROR, message=msg)
                return
            time.sleep(30)

    def upload_query_length(self, query_length: int) -> None:
        """Upload query length metadata to Blob Storage."""
        if query_length <= 0:
            return
        fname = self._metadata_path(ELB_QUERY_LENGTH)
        with open_for_write_immediate(fname) as f:
            f.write(str(query_length))

    # -- Job submission -----------------------------------------------------

    def submit(self, query_batches: List[str], query_length: int,
               one_stage_cloud_query_split: bool) -> None:
        """Submit BLAST batch jobs to the AKS cluster."""
        if one_stage_cloud_query_split:
            raise UserReportError(returncode=INPUT_ERROR,
                message='One-stage cloud query split is not supported on Azure.')

        # Apply optimization profile and show prediction
        self._show_optimization_prediction(query_batches, query_length)

        # Auto-partition if enabled
        if self.cfg.blast.db_auto_partition and self.cfg.blast.db_partitions == 0:
            plan = apply_auto_partition(self.cfg)
            if plan and plan.db_partitions > 0:
                logging.info(str(plan))

        if self.cfg.blast.db_partitions > 0:
            return self._submit_partitioned(query_batches, query_length)

        if not self.cluster_initialized:
            self._check_job_number_limit(query_batches, query_length)
            self.query_files = []
            self._initialize_cluster(query_batches)
            self.cluster_initialized = True

        if self.cloud_job_submission:
            kubernetes.submit_job_submission_job(self.cfg)
        else:
            self._generate_and_submit_jobs(query_batches)
            if not self.cfg.cluster.use_local_ssd:
                self._save_persistent_disk_ids()

        # Deploy finalizer job to auto-detect completion and upload status marker
        self._submit_finalizer_job()

        self.cleanup_stack.clear()
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

    def prepare(self) -> None:
        """Prepare cluster: create AKS, download DB shards, warm cache.
        Does NOT submit BLAST jobs or finalizer."""
        cfg = self.cfg

        # Register cleanup in case prepare fails mid-way
        if not cfg.cluster.reuse:
            self.cleanup_stack.append(lambda: delete_cluster_with_cleanup(cfg, allow_missing=True))

        # Auto-partition if enabled
        if cfg.blast.db_auto_partition and cfg.blast.db_partitions == 0:
            plan = apply_auto_partition(cfg)
            if plan and plan.db_partitions > 0:
                logging.info(str(plan))

        if cfg.blast.db_partitions > 0 and cfg.cluster.use_local_ssd:
            self._initialize_cluster_sharded(None)
        elif cfg.blast.db_partitions > 0:
            self._initialize_cluster_partitioned(None)
        else:
            self._initialize_cluster(None)
        self.cluster_initialized = True

        # Clear cleanup stack on success — cluster should persist for later submit
        self.cleanup_stack.clear()
        logging.info('Cluster prepared and DB loaded. Ready for BLAST job submission.')

    # -- Status & lifecycle -------------------------------------------------

    def check_status(self, extended=False) -> Tuple[ElbStatus, Dict[str, int], Dict[str, str]]:
        """Check execution status of the ElasticBLAST search."""
        try:
            return self._check_status(extended)
        except SafeExecError as err:
            msg = err.message.strip()
            logging.info(msg)
            return ElbStatus.UNKNOWN, defaultdict(int), {STATUS_MESSAGE_ERROR: msg} if msg else {}

    def delete(self):
        """Delete cluster and resources. In reuse mode, only clean up jobs."""
        if self.cfg.cluster.reuse:
            self._cleanup_jobs_only()
        else:
            delete_cluster_with_cleanup(self.cfg)

    def run_command(self, cmd: str) -> str:
        """Run a kubectl command in the cluster context."""
        full_cmd = f'{cmd} --context={self._get_k8s_ctx()}'
        if self.dry_run:
            logging.info(full_cmd)
            return ''
        return handle_error(safe_exec(shlex.split(full_cmd)).stdout)

    def scale_nodes(self, node_count: int) -> None:
        """Scale AKS node pool. Use 0 to scale down for cost savings."""
        cfg = self.cfg
        _sdk_scale_node_pool(cfg.azure.resourcegroup, cfg.cluster.name,
                            'nodepool1', node_count, cfg.cluster.dry_run)

    def get_disk_quota(self) -> Tuple[float, float]:
        """Azure disk quota (not yet implemented). Returns (limit_gb, usage_gb)."""
        return 1e9, 0.0

    def merge_partitioned_results(self) -> List[str]:
        """Merge results from DB-partitioned search into main results directory.
        
        Handles both PV mode (part_XX/) and local-SSD shard mode (shard_XX/).
        """
        num_partitions = self.cfg.blast.db_partitions
        if num_partitions <= 0:
            return []

        base = os.path.join(self.cfg.cluster.results, self.cfg.azure.elb_job_id)
        # Detect directory pattern: shard_XX for local-SSD, part_XX for PV
        prefix = 'shard_' if self.cfg.cluster.use_local_ssd else 'part_'
        merged: List[str] = []
        for i in range(num_partitions):
            src = os.path.join(base, f'{prefix}{i:02d}', '*.out.gz')
            cmd = ['azcopy', 'cp', src, f'{base}/', '--recursive']
            if self.cfg.cluster.dry_run:
                logging.info(f'dry-run: {" ".join(cmd)}')
            else:
                try:
                    _retry_exec(cmd)
                    merged.append(src)
                except SafeExecError as e:
                    logging.warning(f'Failed to merge partition {i}: {e.message}')
        return merged

    # -- Internal helpers ---------------------------------------------------

    @staticmethod
    def _decode(value) -> str:
        """Safely decode bytes to str. Handles None, bytes, and str."""
        if value is None:
            return ''
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        return str(value)

    def _get_k8s_ctx(self) -> str:
        """Get or cache kubectl context."""
        if not self.cfg.appstate.k8s_ctx:
            ctx = get_aks_credentials(self.cfg)
            if not ctx:
                raise UserReportError(returncode=CLUSTER_ERROR,
                                      message='Failed to get AKS kubectl context')
            self.cfg.appstate.k8s_ctx = ctx
        return self.cfg.appstate.k8s_ctx

    def _kubectl(self) -> str:
        """Return kubectl command prefix with context."""
        return f'kubectl --context={self._get_k8s_ctx()}'

    def _kubectl_jsonpath(self, resource_cmd: str, jsonpath: str) -> str:
        """Run kubectl with jsonpath output and return result."""
        cmd = f'{self._kubectl()} {resource_cmd} -o jsonpath=\'{jsonpath}\''
        proc = safe_exec(cmd)
        return handle_error(proc.stdout)

    def _metadata_path(self, filename: str) -> str:
        """Return full blob path for a metadata file."""
        return os.path.join(self.cfg.cluster.results, self.cfg.azure.elb_job_id,
                            ELB_METADATA_DIR, filename)

    def _results_path(self) -> str:
        """Return base results path including job_id."""
        return os.path.join(self.cfg.cluster.results, self.cfg.azure.elb_job_id)

    def _job_template_name(self) -> str:
        """Return correct job template name based on storage mode."""
        if self.cfg.cluster.use_local_ssd:
            return ELB_LOCAL_SSD_BLAST_JOB_AKS_TEMPLATE
        return ELB_DFLT_BLAST_JOB_AKS_TEMPLATE

    def _build_substitutions(self, query_batches: List[str], *,
                              db: str = '', db_label: str = '',
                              results_path: str = '') -> Dict[str, str]:
        """Build template substitution dictionary.

        Shared by both standard and partitioned job generation.
        """
        cfg = self.cfg
        if not db:
            db, _, db_label = get_blastdb_info(cfg.blast.db)
        if not results_path:
            results_path = self._results_path()

        program = cfg.blast.program
        # CPU allocation: fewer CPUs when 1 job per node, else quarter of total
        # Cap at num_cpus - 2 to ensure request never exceeds per-node limit
        if len(query_batches) == cfg.cluster.num_nodes:
            cpu_req = cfg.cluster.num_cpus - 2
        else:
            cpu_req = ((cfg.cluster.num_nodes * cfg.cluster.num_cpus) // 4) - 2
        cpu_req = min(cpu_req, cfg.cluster.num_cpus - 2)
        cpu_req = max(1, cpu_req)

        return {
            'ELB_BLAST_PROGRAM': program,
            'ELB_DB': db,
            'ELB_DB_LABEL': db_label,
            'ELB_MEM_REQUEST': str(cfg.cluster.mem_request),
            'ELB_MEM_LIMIT': str(cfg.cluster.mem_limit),
            'ELB_BLAST_OPTIONS': cfg.blast.options,
            'ELB_BLAST_TIMEOUT': str(cfg.timeouts.blast_k8s * 60),
            'ELB_RESULTS': results_path,
            'ELB_NUM_CPUS_REQ': str(cpu_req),
            'ELB_NUM_CPUS': str(cfg.cluster.num_cpus),
            'ELB_DB_MOL_TYPE': str(ElbSupportedPrograms().get_db_mol_type(program)),
            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,
            'ELB_TIMEFMT': '%s%N',
            'BLAST_ELB_JOB_ID': cfg.azure.elb_job_id,
            'BLAST_ELB_VERSION': VERSION,
            'BLAST_USAGE_REPORT': str(get_usage_reporting()).lower(),
            'K8S_JOB_GET_BLASTDB': K8S_JOB_GET_BLASTDB,
            'K8S_JOB_LOAD_BLASTDB_INTO_RAM': K8S_JOB_LOAD_BLASTDB_INTO_RAM,
            'K8S_JOB_IMPORT_QUERY_BATCHES': K8S_JOB_IMPORT_QUERY_BATCHES,
            'K8S_JOB_SUBMIT_JOBS': K8S_JOB_SUBMIT_JOBS,
            'K8S_JOB_BLAST': K8S_JOB_BLAST,
            'K8S_JOB_RESULTS_EXPORT': K8S_JOB_RESULTS_EXPORT,
            'ELB_AZURE_RESOURCE_GROUP': cfg.azure.resourcegroup,
            'ELB_METADATA_DIR': ELB_METADATA_DIR,
        }

    # Keep backward-compatible names
    def job_substitutions(self, query_batches) -> Dict[str, str]:
        return self._build_substitutions(query_batches)

    def _job_substitutions_for_partition(self, query_batches, partition_idx, partition_db, partition_db_name):
        return self._build_substitutions(
            query_batches, db=partition_db, db_label=f'part{partition_idx:02d}',
            results_path=os.path.join(self._results_path(), f'part_{partition_idx:02d}')
        )

    def _submit_finalizer_job(self) -> None:
        """Submit the finalizer Job that waits for BLAST completion and uploads status."""
        cfg = self.cfg
        if cfg.cluster.dry_run:
            logging.info('dry-run: would submit elb-finalizer job')
            return

        subs = {
            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,
            'ELB_RESULTS': self._results_path(),
            'ELB_AZURE_RESOURCE_GROUP': cfg.azure.resourcegroup,
            'ELB_CLUSTER_NAME': cfg.cluster.name,
            'ELB_REUSE_CLUSTER': 'true' if cfg.cluster.reuse else 'false',
            'ELB_METADATA_DIR': ELB_METADATA_DIR,
            'ELB_SERVICE_ACCOUNT': 'default',
            'ELB_DB_PARTITIONS': str(cfg.blast.db_partitions) if cfg.blast.db_partitions > 0 else '0',
            'ELB_BLAST_PROGRAM': cfg.blast.program,
        }

        from importlib.resources import files as pkg_files
        template = pkg_files('elastic_blast').joinpath(
            'templates/elb-finalizer-aks.yaml.template').read_text()
        rendered = substitute_params(template, subs)

        with TemporaryDirectory() as d:
            path = os.path.join(d, 'elb-finalizer.yaml')
            with open(path, 'w') as f:
                f.write(rendered)
            kubectl = self._kubectl()
            safe_exec(shlex.split(f'{kubectl} apply -f {path}'))
            logging.info('Submitted elb-finalizer job')

    def _show_optimization_prediction(self, query_batches: List[str], query_length: int) -> None:
        """Apply optimization profile and display time/cost prediction."""
        profile = get_profile()
        query_size_gb = query_length / 1e9 if query_length > 0 else 0.1

        # Get actual DB size from metadata if available
        db_size_gb = 10.0  # default
        if self.cfg.blast.db_metadata and hasattr(self.cfg.blast.db_metadata, 'bytes_to_cache'):
            db_size_gb = self.cfg.blast.db_metadata.bytes_to_cache / (1024 ** 3)
        elif os.environ.get('ELB_DB_SIZE_GB'):
            db_size_gb = float(os.environ['ELB_DB_SIZE_GB'])

        num_batches = len(query_batches) if query_batches else None

        # Show all profiles comparison
        comparison = predict_all_profiles(
            query_size_gb=query_size_gb, db_size_gb=db_size_gb,
            batch_len=self.cfg.blast.batch_len, num_batches=num_batches)
        logging.info(comparison)
        if os.isatty(1):
            print(comparison)

        pred = apply_profile(self.cfg, profile,
                             query_size_gb=query_size_gb,
                             db_size_gb=db_size_gb)

        # Detailed prediction log
        logging.info(f'\n{"=" * 60}\n'
                     f'  ElasticBLAST Execution Plan\n'
                     f'{"=" * 60}\n'
                     f'{pred}\n'
                     f'{"=" * 60}')

    def _check_job_number_limit(self, queries: Optional[List[str]], query_length) -> None:
        """Raise if the number of jobs exceeds the Kubernetes limit."""
        if not queries:
            return
        limit = kubernetes.get_maximum_number_of_allowed_k8s_jobs(self.dry_run)
        if len(queries) > limit:
            suggested = int(query_length / limit) + 1
            raise UserReportError(INPUT_ERROR,
                f'Batch size led to {len(queries)} jobs, exceeding limit of {limit}. '
                f'Increase batch-len to at least {suggested}.')

    # -- Cluster initialization ---------------------------------------------

    def _initialize_cluster(self, queries: Optional[List[str]] = None):
        """Create AKS cluster, configure IAM, initialize storage.

        Cluster creation runs asynchronously — query template upload happens
        in parallel while the cluster is provisioning.
        """
        cfg = self.cfg

        # Warm cluster shortcut
        if cfg.cluster.reuse:
            status = check_cluster(cfg)
            if status == AKS_PROVISIONING_STATE.SUCCEEDED.value and self._db_already_loaded():
                logging.info('Warm cluster reuse: skipping init')
                self._get_k8s_ctx()
                self._cleanup_stale_jobs()
                kubernetes.create_scripts_configmap(cfg.appstate.k8s_ctx, cfg.cluster.dry_run)
                self._upload_queries_only(queries)
                self.cleanup_stack.append(lambda: self._safe_collect_logs())
                return

        # Full initialization
        if not cfg.cluster.reuse:
            self.cleanup_stack.append(lambda: delete_cluster_with_cleanup(cfg, allow_missing=True))
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

        # Start cluster creation asynchronously
        aks_status = check_cluster(cfg)
        poller = None
        if not cfg.cluster.reuse or not aks_status:
            poller = start_cluster_async(cfg)

        # Do work while cluster is provisioning (saves 5-10 min)
        if self.cloud_job_submission:
            self._upload_job_template(queries)

        # Wait for cluster to be ready
        if poller:
            wait_for_cluster(cfg, poller)

        self._get_k8s_ctx()
        self._label_nodes()

        if not cfg.cluster.reuse or not aks_status:
            set_role_assignment(cfg)

        if self.cloud_job_submission or self.auto_shutdown:
            kubernetes.enable_service_account(cfg)

        kubernetes.create_scripts_configmap(cfg.appstate.k8s_ctx, cfg.cluster.dry_run)

        # Apply ANF StorageClass if configured (persistent DB across cluster lifecycles)
        if self.STORAGE_CLASS == 'azure-netapp-ultra':
            self._apply_anf_storage_class()

        logging.info('Initializing storage')
        wait_mode = ElbExecutionMode.NOWAIT if self.cloud_job_submission else ElbExecutionMode.WAIT
        kubernetes.initialize_storage(cfg, self.query_files, wait_mode)

        if cfg.cluster.reuse and not cfg.cluster.use_local_ssd:
            self._deploy_vmtouch_daemonset()

    def _initialize_cluster_partitioned(self, queries: Optional[List[str]]) -> None:
        """Initialize cluster for DB-partitioned search."""
        cfg = self.cfg

        if not cfg.cluster.reuse:
            self.cleanup_stack.append(lambda: delete_cluster_with_cleanup(cfg, allow_missing=True))
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

        aks_status = check_cluster(cfg)
        if not cfg.cluster.reuse or not aks_status:
            start_cluster(cfg)

        self._get_k8s_ctx()
        self._label_nodes()

        if not cfg.cluster.reuse or not aks_status:
            set_role_assignment(cfg)

        if self.cloud_job_submission or self.auto_shutdown:
            kubernetes.enable_service_account(cfg)

        kubernetes.create_scripts_configmap(cfg.appstate.k8s_ctx, cfg.cluster.dry_run)

        wait_mode = ElbExecutionMode.NOWAIT if self.cloud_job_submission else ElbExecutionMode.WAIT
        kubernetes.initialize_storage_partitioned(cfg, self.query_files, wait_mode)

    # -- Partitioned search -------------------------------------------------

    def _submit_partitioned(self, query_batches: List[str], query_length) -> None:
        """Submit jobs for DB-partitioned mode: P partitions x N queries.
        
        In local-SSD mode: each node downloads one shard, BLAST jobs are pinned
        to nodes via nodeSelector. Each shard × batch combination = one job.
        In PV mode: all partitions are downloaded to a shared PVC.
        """
        cfg = self.cfg
        num_partitions = cfg.blast.db_partitions

        if not self.cluster_initialized:
            if query_batches:
                total = len(query_batches) * num_partitions
                limit = kubernetes.get_maximum_number_of_allowed_k8s_jobs(self.dry_run)
                if total > limit:
                    raise UserReportError(INPUT_ERROR,
                        f'Partitioned search would create {total} jobs '
                        f'({num_partitions} x {len(query_batches)}), exceeding limit {limit}.')
            self.query_files = []
            if cfg.cluster.use_local_ssd:
                self._initialize_cluster_sharded(query_batches)
            else:
                self._initialize_cluster_partitioned(query_batches)
            self.cluster_initialized = True

        if cfg.cluster.use_local_ssd:
            self._generate_sharded_jobs(query_batches)
        else:
            self._generate_partitioned_jobs(query_batches)

        # Deploy finalizer
        if self.auto_shutdown:
            self._submit_finalizer_job()

        self.cleanup_stack.clear()
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

    def _initialize_cluster_sharded(self, queries: Optional[List[str]]) -> None:
        """Initialize cluster for local-SSD shard mode (one shard per node)."""
        cfg = self.cfg

        if not cfg.cluster.reuse:
            self.cleanup_stack.append(lambda: delete_cluster_with_cleanup(cfg, allow_missing=True))
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

        aks_status = check_cluster(cfg)
        poller = None
        if not cfg.cluster.reuse or not aks_status:
            poller = start_cluster_async(cfg)

        # Upload job template while cluster is creating
        if self.cloud_job_submission:
            self._upload_job_template(queries)

        if poller:
            wait_for_cluster(cfg, poller)

        self._get_k8s_ctx()
        self._label_nodes()

        if not cfg.cluster.reuse or not aks_status:
            set_role_assignment(cfg)

        if self.cloud_job_submission or self.auto_shutdown:
            kubernetes.enable_service_account(cfg)

        kubernetes.create_scripts_configmap(cfg.appstate.k8s_ctx, cfg.cluster.dry_run)

        # Initialize sharded storage: each node downloads its own shard
        # Always WAIT — BLAST jobs need init to complete before they can run
        kubernetes.initialize_local_ssd_sharded(cfg, self.query_files, ElbExecutionMode.WAIT)

    def _generate_sharded_jobs(self, query_batches: List[str]) -> None:
        """Generate BLAST jobs for local-SSD shard mode.
        
        Creates N_shards × N_batches jobs. Each job is pinned to the node
        that has its shard via nodeSelector ordinal.
        """
        cfg = self.cfg
        prefix = cfg.blast.db_partition_prefix
        base = self._results_path()
        num_shards = cfg.blast.db_partitions

        # Read shard-specific BLAST job template
        from importlib.resources import files as pkg_files
        ref = pkg_files('elastic_blast').joinpath('templates/blast-batch-job-shard-ssd-aks.yaml.template')
        template = ref.read_text()

        all_files = []
        with TemporaryDirectory() as job_path:
            for shard_idx in range(num_shards):
                shard_name = os.path.basename(f'{prefix}{shard_idx:02d}')
                shard_label = f's{shard_idx:02d}'

                subs = self._build_substitutions(
                    query_batches,
                    db=shard_name,
                    db_label=shard_label,
                    results_path=os.path.join(base, f'shard_{shard_idx:02d}'),
                )
                subs['ELB_SHARD_IDX'] = str(shard_idx)
                subs['ELB_RESULTS_BASE'] = base

                files_written = write_job_files(
                    job_path, f'{shard_label}_batch_',
                    template, query_batches, **subs)
                all_files.extend(files_written)

            total = len(all_files)
            logging.info(f'Submitting {total} sharded jobs ({num_shards} shards x {len(query_batches)} batches)')
            assert cfg.appstate.k8s_ctx
            start = timer()
            kubernetes.submit_jobs(cfg.appstate.k8s_ctx, Path(job_path), dry_run=self.dry_run)
            logging.debug(f'RUNTIME submit-sharded-jobs {timer() - start:.1f}s')

        with open_for_write_immediate(self._metadata_path(ELB_NUM_JOBS_SUBMITTED)) as f:
            f.write(str(total))

        track_search_submitted(
            job_id=cfg.azure.elb_job_id, program=cfg.blast.program,
            db=cfg.blast.db, num_jobs=total,
            num_nodes=cfg.cluster.num_nodes, machine_type=cfg.cluster.machine_type)

    def _generate_partitioned_jobs(self, query_batches: List[str]) -> None:
        """Generate and submit BLAST jobs for each DB partition."""
        cfg = self.cfg
        base = self._results_path()
        # Use actual DB name from config URL (basename of the blob path)
        # e.g. https://stgelb.blob.core.windows.net/blast-db/16S_ribosomal_RNA → 16S_ribosomal_RNA
        actual_db_name = os.path.basename(cfg.blast.db)

        all_files = []
        with TemporaryDirectory() as job_path:
            for i in range(cfg.blast.db_partitions):
                db_on_pvc = f'part_{i:02d}/{actual_db_name}'
                subs = self._build_substitutions(
                    query_batches,
                    db=db_on_pvc, db_label=f'part{i:02d}',
                    results_path=os.path.join(base, f'part_{i:02d}'),
                )
                template = read_job_template(cfg=cfg)
                files = write_job_files(job_path, f'part{i:02d}_batch_',
                                        template, query_batches, **subs)
                all_files.extend(files)

            total = len(all_files)
            logging.info(f'Submitting {total} partitioned jobs')
            assert cfg.appstate.k8s_ctx
            start = timer()
            kubernetes.submit_jobs(cfg.appstate.k8s_ctx, Path(job_path), dry_run=self.dry_run)
            logging.debug(f'RUNTIME submit-partitioned-jobs {timer() - start:.1f}s')

        with open_for_write_immediate(self._metadata_path(ELB_NUM_JOBS_SUBMITTED)) as f:
            f.write(str(total))

        track_search_submitted(
            job_id=cfg.azure.elb_job_id, program=cfg.blast.program,
            db=cfg.blast.db, num_jobs=total,
            num_nodes=cfg.cluster.num_nodes, machine_type=cfg.cluster.machine_type)

    # -- Standard job submission --------------------------------------------

    def _generate_and_submit_jobs(self, queries: List[str]):
        """Generate BLAST batch job YAMLs and submit to cluster."""
        cfg = self.cfg
        subs = self._build_substitutions(queries)
        template = read_job_template(cfg=cfg)

        with TemporaryDirectory() as job_path:
            job_files = write_job_files(job_path, 'batch_', template, queries, **subs)
            logging.info(f'Submitting {len(job_files)} jobs to cluster')

            assert cfg.appstate.k8s_ctx
            start = timer()
            job_names = kubernetes.submit_jobs(cfg.appstate.k8s_ctx, Path(job_path),
                                               dry_run=self.dry_run)
            elapsed = timer() - start
            logging.debug(f'RUNTIME submit-jobs {elapsed:.1f}s '
                         f'({len(job_names) / max(elapsed, 0.1):.0f} jobs/sec)')

            with open_for_write_immediate(self._metadata_path(ELB_NUM_JOBS_SUBMITTED)) as f:
                f.write(str(len(job_names)))

            track_search_submitted(
                job_id=cfg.azure.elb_job_id, program=cfg.blast.program,
                db=cfg.blast.db, num_jobs=len(job_names),
                num_nodes=cfg.cluster.num_nodes, machine_type=cfg.cluster.machine_type)

    def _save_persistent_disk_ids(self) -> None:
        """Save persistent disk IDs to Blob Storage metadata.
        
        Note: This is primarily a GCP concept (PD snapshots).
        On Azure, NFS PVCs (azureblob-nfs) don't have persistent disk IDs
        to save, so this method is a no-op for Azure.
        """
        pass

    def _upload_job_template(self, queries: Optional[List[str]]) -> None:
        """Upload rendered job template to Blob Storage."""
        subs = self._build_substitutions(queries or [])
        template = read_job_template(template_name=self._job_template_name(), cfg=self.cfg)
        rendered = substitute_params(template, subs)
        path = os.path.join(self._results_path(), ELB_METADATA_DIR, 'job.yaml.template')
        with open_for_write_immediate(path) as f:
            f.write(rendered)

    def _apply_anf_storage_class(self) -> None:
        """Apply Azure NetApp Files StorageClass to enable persistent DB volumes."""
        kubectl = self._kubectl()
        from importlib.resources import files as pkg_files
        sc_path = pkg_files('elastic_blast').joinpath('templates/storage-aks-anf.yaml')
        with TemporaryDirectory() as d:
            dest = os.path.join(d, 'storage-aks-anf.yaml')
            with open(dest, 'w') as f:
                f.write(sc_path.read_text())
            if self.cfg.cluster.dry_run:
                logging.info(f'Would apply ANF StorageClass from {dest}')
            else:
                safe_exec(shlex.split(f'{kubectl} apply -f {dest}'))
                logging.info('Applied azure-netapp-ultra StorageClass')

    # -- Status check -------------------------------------------------------

    def _check_status(self, extended=False) -> Tuple[ElbStatus, Dict[str, int], Dict[str, str]]:
        """Internal status check implementation."""
        if self.cached_status:
            result = {STATUS_MESSAGE_ERROR: self.cached_failure_message} if self.cached_failure_message else {}
            return self.cached_status, self.cached_counts, result

        status = self._status_from_results()
        if status != ElbStatus.UNKNOWN:
            result = {STATUS_MESSAGE_ERROR: self.cached_failure_message} if self.cached_failure_message else {}
            return status, self.cached_counts, result

        aks_status = check_cluster(self.cfg)
        if not aks_status:
            return (ElbStatus.UNKNOWN, {},
                    {STATUS_MESSAGE_ERROR: f'Cluster "{self.cfg.cluster.name}" was not found'})

        TRANSITIONAL = {AKS_PROVISIONING_STATE.UPDATING.value,
                        AKS_PROVISIONING_STATE.CREATING.value,
                        AKS_PROVISIONING_STATE.STARTING.value}
        if aks_status in TRANSITIONAL:
            return ElbStatus.SUBMITTING, {}, {}

        if aks_status != AKS_PROVISIONING_STATE.SUCCEEDED.value:
            raise UserReportError(returncode=CLUSTER_ERROR,
                message=f'Cluster "{self.cfg.cluster.name}" is not responding. '
                        'Try again in a few minutes.')

        counts = self._count_blast_jobs()
        status = self._derive_status(counts)

        # Track terminal states to App Insights
        if status in (ElbStatus.SUCCESS, ElbStatus.FAILURE):
            track_search_completed(
                job_id=self.cfg.azure.elb_job_id,
                succeeded=counts.get('succeeded', 0),
                failed=counts.get('failed', 0),
                program=self.cfg.blast.program,
                db=self.cfg.blast.db)

        return status, counts, {}

    def _count_blast_jobs(self) -> DefaultDict[str, int]:
        """Count BLAST job statuses via kubectl."""
        counts: DefaultDict[str, int] = defaultdict(int)
        kubectl = self._kubectl()

        if not self.dry_run:
            proc = safe_exec(f'{kubectl} get jobs -o custom-columns='
                            f'STATUS:.status.conditions[0].type -l app=blast'.split())
            for line in handle_error(proc.stdout).split('\n'):
                if not line or line.startswith('STATUS'):
                    continue
                if line.startswith('Complete'):
                    counts['succeeded'] += 1
                elif line.startswith('Failed'):
                    counts['failed'] += 1
                else:
                    counts['pending'] += 1

            proc = safe_exec(f'{kubectl} get pods -o custom-columns='
                            f'STATUS:.status.phase -l app=blast'.split())
            for line in handle_error(proc.stdout).split('\n'):
                if line == 'Running':
                    counts['running'] += 1

            counts['pending'] -= counts['running']
        return counts

    def _derive_status(self, counts: DefaultDict[str, int]) -> ElbStatus:
        """Derive overall status from job counts."""
        if counts['failed'] > 0:
            return ElbStatus.FAILURE
        if counts['running'] > 0 or counts['pending'] > 0:
            return ElbStatus.RUNNING
        if counts['succeeded'] > 0:
            return ElbStatus.SUCCESS

        # No blast jobs yet — check setup/submit
        for app in ('setup', 'submit'):
            _, _, failed = self._job_status_by_app(app)
            if failed > 0:
                return ElbStatus.FAILURE
        return ElbStatus.SUBMITTING

    def _job_status_by_app(self, app: str) -> Tuple[int, int, int]:
        """Count pending/succeeded/failed jobs for a given app label."""
        pending = succeeded = failed = 0
        if self.dry_run:
            return pending, succeeded, failed
        try:
            proc = safe_exec(f'{self._kubectl()} get jobs -o custom-columns='
                            f'STATUS:.status.conditions[0].type -l app={app}'.split())
            for line in handle_error(proc.stdout).split('\n'):
                if not line or line.startswith('STATUS'):
                    continue
                if line.startswith('Complete'):
                    succeeded += 1
                elif line.startswith('Failed'):
                    failed += 1
                else:
                    pending += 1
        except SafeExecError:
            pass
        return pending, succeeded, failed

    # -- Cleanup ------------------------------------------------------------

    def _cleanup_stale_jobs(self) -> None:
        """Delete all stale jobs before a warm-cluster 2nd submit.

        Removes previous blast, submit, setup, and finalizer jobs so that
        ``kubectl apply`` on the new submit-jobs job does not hit immutable
        field errors from leftover resources.
        """
        try:
            kubectl = self._kubectl()
            for label in ('app=blast', 'app=submit', 'app=setup', 'app=finalizer'):
                cmd = f'{kubectl} delete jobs -l {label} --ignore-not-found=true'
                if self.cfg.cluster.dry_run:
                    logging.info(cmd)
                else:
                    safe_exec(shlex.split(cmd))
            logging.info('Warm reuse: stale jobs cleaned up')
        except Exception as e:
            logging.warning(f'Warm reuse cleanup failed: {e}')

    def _cleanup_jobs_only(self) -> None:
        """In reuse mode: delete BLAST/submit jobs, preserve cluster and PVCs."""
        try:
            kubectl = self._kubectl()
            for label in ('app=blast', 'app=submit'):
                cmd = f'{kubectl} delete jobs -l {label} --ignore-not-found=true'
                if self.cfg.cluster.dry_run:
                    logging.info(cmd)
                else:
                    safe_exec(shlex.split(cmd))
            logging.info('Reuse cleanup: jobs deleted, cluster preserved')
        except Exception as e:
            logging.warning(f'Reuse cleanup failed: {e}')

    # -- Warm cluster -------------------------------------------------------

    def _db_already_loaded(self) -> bool:
        """Check if BLAST DB is already present on the cluster.

        For PV mode: checks if the PVC is bound and init-pv succeeded.
        For local-SSD mode: checks if the create-workspace DaemonSet
        exists (it persists across runs) as a proxy for DB presence on
        the nodes' hostPath.
        """
        if self.cfg.cluster.dry_run:
            return False
        try:
            kubectl = self._kubectl()
            if self.cfg.cluster.use_local_ssd:
                # Local-SSD: the create-workspace DaemonSet in kube-system
                # persists across runs and indicates that /workspace was
                # previously initialized with DB files.
                proc = safe_exec(shlex.split(
                    f'{kubectl} -n kube-system get daemonset create-workspace'
                    f' -o jsonpath={{.status.numberReady}}'))
                ready = self._decode(proc.stdout).strip()
                return ready != '' and int(ready) > 0
            else:
                proc = safe_exec(shlex.split(
                    f'{kubectl} get pvc blast-dbs-pvc-rwm -o jsonpath={{.status.phase}}'))
                if self._decode(proc.stdout).strip() != 'Bound':
                    return False
                proc = safe_exec(shlex.split(
                    f'{kubectl} get job init-pv -o jsonpath={{.status.succeeded}}'))
                return self._decode(proc.stdout).strip() == '1'
        except Exception:
            return False

    def _upload_queries_only(self, queries: Optional[List[str]]) -> None:
        """Warm cluster: upload queries without re-downloading DB.

        For PV mode: runs import_query_batches_only (PVC-based).
        For local-SSD mode: skips query import here — queries are
        uploaded to blob storage by _upload_job_template, and each
        BLAST pod's initContainer downloads its query batch from blob.
        """
        if self.cloud_job_submission:
            self._upload_job_template(queries)
        if not self.cfg.cluster.use_local_ssd:
            kubernetes.import_query_batches_only(self.cfg)

    def _deploy_vmtouch_daemonset(self) -> None:
        """Deploy DaemonSet to keep BLAST DB cached in RAM (80% of available)."""
        kubectl = self._kubectl()
        dry_run = self.cfg.cluster.dry_run

        # Skip if already deployed
        if not dry_run:
            try:
                proc = safe_exec(shlex.split(
                    f'{kubectl} get daemonset vmtouch-db-cache --ignore-not-found -o name'))
                if self._decode(proc.stdout).strip():
                    return
            except Exception:
                pass

        db, _, _ = get_blastdb_info(self.cfg.blast.db)
        subs = {
            'ELB_DOCKER_IMAGE': self.cfg.azure.elb_docker_image,
            'ELB_DB': db,
            'ELB_DB_MOL_TYPE': str(ElbSupportedPrograms().get_db_mol_type(self.cfg.blast.program)),
        }

        from importlib.resources import files as pkg_files
        template = pkg_files('elastic_blast').joinpath(
            'templates/vmtouch-daemonset-aks.yaml.template').read_text()
        yaml_content = substitute_params(template, subs)

        with TemporaryDirectory() as d:
            path = os.path.join(d, 'vmtouch-daemonset.yaml')
            with open(path, 'w') as f:
                f.write(yaml_content)
            if dry_run:
                logging.info(f'Would apply {path}')
            else:
                safe_exec(shlex.split(f'{kubectl} apply -f {path}'))

    def _label_nodes(self) -> None:
        """Label nodes with ordinal index for local-SSD affinity."""
        if not self.cfg.cluster.use_local_ssd:
            return
        kubectl = self._kubectl()
        if self.cfg.cluster.dry_run:
            return
        proc = safe_exec(f"{kubectl} get nodes -o jsonpath='{{.items[*].metadata.name}}'")
        for i, name in enumerate(handle_error(proc.stdout).replace("'", "").split()):
            safe_exec(f'{kubectl} label nodes {name} ordinal={i} --overwrite')


# ---------------------------------------------------------------------------
# Module-level functions (thin wrappers around azure_sdk)
# ---------------------------------------------------------------------------

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _retry_exec(cmd):
    """Execute a command with exponential backoff retry."""
    return safe_exec(cmd)


def set_gcp_project(project: str) -> None:
    """No-op for Azure. Retained for interface compatibility."""
    pass


def get_disks(cfg: ElasticBlastConfig, dry_run: bool = False) -> List[str]:
    return _sdk_get_disks(cfg.azure.resourcegroup, dry_run)


def get_snapshots(cfg: ElasticBlastConfig, dry_run: bool = False) -> List[str]:
    return _sdk_get_snapshots(cfg.azure.resourcegroup, dry_run)


def delete_disk(name: str, cfg: ElasticBlastConfig) -> None:
    if not name:
        raise ValueError('No disk name provided')
    if not cfg:
        raise ValueError('No application config provided')
    _sdk_delete_disk(cfg.azure.resourcegroup, name)


def delete_snapshot(name: str, cfg: ElasticBlastConfig) -> None:
    if not name:
        raise ValueError('No snapshot name provided')
    if not cfg:
        raise ValueError('No application config provided')
    _sdk_delete_snapshot(cfg.azure.resourcegroup, name)


def get_aks_clusters(cfg: ElasticBlastConfig) -> List[str]:
    return _sdk_get_aks_clusters(cfg.azure.resourcegroup, cfg.cluster.dry_run)


def get_aks_credentials(cfg: ElasticBlastConfig) -> str:
    return _sdk_get_aks_credentials(cfg.azure.resourcegroup, cfg.cluster.name,
                                   cfg.cluster.dry_run)


def set_role_assignment(cfg: ElasticBlastConfig):
    _sdk_set_role_assignments(cfg.azure.resourcegroup, cfg.cluster.name,
                             cfg.azure.storage_account,
                             cfg.azure.acr_name, cfg.azure.acr_resourcegroup,
                             cfg.cluster.dry_run)


def check_cluster(cfg: ElasticBlastConfig) -> str:
    return _sdk_check_cluster(cfg.azure.resourcegroup, cfg.cluster.name,
                             cfg.cluster.dry_run)


def start_cluster(cfg: ElasticBlastConfig) -> str:
    """Create AKS cluster. Blocks until provisioning completes."""
    poller = start_cluster_async(cfg)
    wait_for_cluster(cfg, poller)
    return cfg.cluster.name


def start_cluster_async(cfg: ElasticBlastConfig):
    """Start AKS cluster creation (non-blocking). Returns poller or None."""
    name = cfg.cluster.name
    if not name:
        raise ValueError('Missing cluster name')
    if not cfg.cluster.machine_type:
        raise ValueError('Missing machine-type')

    tags = {}
    if cfg.cluster.labels:
        for pair in cfg.cluster.labels.split(','):
            k, _, v = pair.partition('=')
            tags[k.strip()] = v.strip()

    return _sdk_start_cluster(
        resource_group=cfg.azure.resourcegroup, cluster_name=name,
        location=cfg.azure.region, machine_type=cfg.cluster.machine_type,
        num_nodes=cfg.cluster.num_nodes or 1,
        use_local_ssd=cfg.cluster.use_local_ssd,
        use_spot=cfg.cluster.use_preemptible, tags=tags,
        k8s_version=cfg.azure.aks_version or None,
        dry_run=cfg.cluster.dry_run,
    )


def wait_for_cluster(cfg: ElasticBlastConfig, poller) -> None:
    """Block until AKS cluster is ready."""
    if poller is None:
        return
    start = timer()
    logging.info(f'Waiting for AKS cluster {cfg.cluster.name}...')
    try:
        poller.result()
    except Exception as e:
        elapsed = timer() - start
        logging.debug(f'RUNTIME cluster-create-failed {elapsed:.1f}s')
        raise UserReportError(CLUSTER_ERROR,
            f'AKS cluster {cfg.cluster.name} creation failed after {elapsed:.0f}s: {e}') from e
    elapsed = timer() - start
    logging.debug(f'RUNTIME cluster-create {elapsed:.1f}s')
    track_cluster_created(
        cluster_name=cfg.cluster.name, duration_s=elapsed,
        num_nodes=cfg.cluster.num_nodes or 1, machine_type=cfg.cluster.machine_type)


def delete_cluster(cfg: ElasticBlastConfig) -> str:
    """Delete AKS cluster. Blocks until deletion completes."""
    name = cfg.cluster.name
    start = timer()
    poller = _sdk_delete_cluster(cfg.azure.resourcegroup, name, cfg.cluster.dry_run)
    if poller:
        poller.result()
    elapsed = timer() - start
    logging.debug(f'RUNTIME cluster-delete {elapsed:.1f}s')
    track_cluster_deleted(cluster_name=name, duration_s=elapsed,
                           num_nodes=cfg.cluster.num_nodes or 1,
                           machine_type=cfg.cluster.machine_type)
    return name


def check_prerequisites() -> None:
    _sdk_check_prerequisites()


# ---------------------------------------------------------------------------
# Resource cleanup
# ---------------------------------------------------------------------------

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _get_resource_ids(cfg: ElasticBlastConfig) -> ResourceIds:
    """Fetch persistent disk/snapshot IDs from Blob Storage metadata."""
    if cfg.appstate.resources.disks and cfg.appstate.resources.snapshots:
        return cfg.appstate.resources
    path = os.path.join(cfg.cluster.results, cfg.azure.elb_job_id,
                        ELB_METADATA_DIR, ELB_STATE_DISK_ID_FILE)
    try:
        from .filehelper import open_for_read
        with open_for_read(path) as f:
            retval = ResourceIds.from_json(f.read())
        if retval.disks or retval.snapshots:
            return retval
    except Exception as e:
        logging.debug(f'Unable to read {path}: {e}')
    return ResourceIds()


def delete_cluster_with_cleanup(cfg: ElasticBlastConfig, allow_missing: bool = False) -> None:
    """Delete AKS cluster and clean up persistent disks/snapshots.

    Args:
        allow_missing: If True, skip gracefully when cluster not found.
                       Used by cleanup stack to avoid hiding original errors.
    """
    dry_run = cfg.cluster.dry_run
    pds, snapshots = _collect_resource_ids(cfg)

    # Wait for cluster to reach a stable state
    try_kubernetes = _wait_for_cluster_ready(cfg, allow_missing=allow_missing)

    if try_kubernetes:
        pds, snapshots = _cleanup_k8s_resources(cfg, pds, snapshots, dry_run)

    _cleanup_leaked_disks(cfg, pds, snapshots, dry_run)
    remove_split_query(cfg)
    delete_cluster(cfg)


def _collect_resource_ids(cfg: ElasticBlastConfig) -> Tuple[List[str], List[str]]:
    """Gather disk and snapshot IDs from metadata."""
    try:
        res = _get_resource_ids(cfg)
        return res.disks, res.snapshots
    except Exception as e:
        logging.error(f'Unable to read resource IDs: {e}')
        return [], []


def _wait_for_cluster_ready(cfg: ElasticBlastConfig, allow_missing: bool = False) -> bool:
    """Wait for AKS cluster to reach a terminal state. Returns True if K8s is usable.

    Args:
        allow_missing: If True, return False instead of raising when cluster not found.
                       Used by cleanup stack where throwing would hide the original error.
    """
    while True:
        status = check_cluster(cfg)
        if not status:
            if cfg.cluster.dry_run:
                return False
            if allow_missing:
                logging.info(f'Cluster {cfg.cluster.name} not found, nothing to delete')
                return False
            elb = ElasticBlastAzure(cfg, False)
            result_status = elb._status_from_results()
            if result_status == ElbStatus.UNKNOWN:
                raise UserReportError(CLUSTER_ERROR,
                    f'Cluster {cfg.cluster.name} was not found')
            remove_split_query(cfg)
            return False

        if status == AKS_PROVISIONING_STATE.SUCCEEDED.value:
            return True
        if status in (AKS_PROVISIONING_STATE.FAILED.value,
                      AKS_PROVISIONING_STATE.STOPPING.value,
                      AKS_PROVISIONING_STATE.DELETING.value):
            return False
        if status in (AKS_PROVISIONING_STATE.STARTING.value,
                      AKS_PROVISIONING_STATE.UPDATING.value):
            time.sleep(10)
            continue

        logging.warning(f'Unrecognized cluster status: {status}')
        return True


def _cleanup_k8s_resources(cfg, pds, snapshots, dry_run):
    """Delete K8s objects and collect resource IDs."""
    try:
        cfg.appstate.k8s_ctx = get_aks_credentials(cfg)
        kubernetes.check_server(cfg.appstate.k8s_ctx, dry_run)
    except Exception as e:
        logging.warning(f'K8s connection failed: {e}')
        return pds, snapshots

    ctx = cfg.appstate.k8s_ctx
    assert ctx

    try:
        pds = kubernetes.get_persistent_disks(ctx, dry_run)
    except Exception as e:
        logging.warning(f'get_persistent_disks failed: {e}')

    try:
        snapshots = kubernetes.get_volume_snapshots(ctx, dry_run)
    except Exception as e:
        logging.warning(f'get_volume_snapshots failed: {e}')

    try:
        kubernetes.delete_all(ctx, dry_run)
    except Exception as e:
        logging.warning(f'kubernetes.delete_all failed: {e}')

    return pds, snapshots


def _cleanup_leaked_disks(cfg, pds, snapshots, dry_run):
    """Delete any disks/snapshots that survived K8s cleanup."""
    rg = cfg.azure.resourcegroup
    for disk in pds:
        try:
            if disk in get_disks(cfg, dry_run):
                delete_disk(disk, cfg)
        except Exception as e:
            logging.error(f'Failed to delete disk {disk}: {e}')
            _warn_leaked_resource('disk', disk, rg,
                f'az disk list --resource-group {rg} --query "[?name==\'{disk}\']" -o table',
                f'az disk delete -y --name {disk} --resource-group {rg}')

    for snap in snapshots:
        try:
            if snap in get_snapshots(cfg, dry_run):
                delete_snapshot(snap, cfg)
        except Exception as e:
            logging.error(f'Failed to delete snapshot {snap}: {e}')
            _warn_leaked_resource('snapshot', snap, rg,
                f'az snapshot list --resource-group {rg} --query "[?name==\'{snap}\']" -o table',
                f'az snapshot delete --name {snap} --resource-group {rg}')


def _warn_leaked_resource(kind: str, name: str, rg: str, check_cmd: str, delete_cmd: str):
    """Log a warning about a resource that couldn't be cleaned up."""
    logging.error(
        f'Could not delete {kind} "{name}". This may cause charges.\n'
        f'Verify: {check_cmd}\nDelete: {delete_cmd}')


def remove_split_query(cfg: ElasticBlastConfig) -> None:
    """Remove split query data from Blob Storage."""
    _remove_ancillary_data(cfg, ELB_QUERY_BATCH_DIR)


def _remove_ancillary_data(cfg: ElasticBlastConfig, bucket_prefix: str) -> None:
    """Remove data under a result subdirectory."""
    path = os.path.join(cfg.cluster.results, cfg.azure.elb_job_id, bucket_prefix)
    cmd = f'azcopy rm "{path}" --recursive=true'
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        try:
            safe_exec(shlex.split(cmd))
        except SafeExecError as e:
            logging.warning(e.message.strip().replace('\n', '|'))
