"""
elastic_blast/azure_sdk.py — Azure SDK clients for AKS resource management

Replaces az CLI subprocess calls with Azure SDK for Python.
Benefits: no subprocess fork overhead, type-safe responses, native async (LROPoller),
structured error handling, no az CLI binary dependency for ARM operations.

Authors: Moon Hyuk Choi moonchoi@microsoft.com
"""

import logging
import base64
import tempfile
import os
from typing import Any, Dict, List, Optional, Tuple
from timeit import default_timer as timer
from tenacity import retry, stop_after_attempt, wait_exponential

from azure.identity import DefaultAzureCredential  # type: ignore
from azure.mgmt.containerservice import ContainerServiceClient  # type: ignore
from azure.mgmt.compute import ComputeManagementClient  # type: ignore
from azure.mgmt.storage import StorageManagementClient  # type: ignore
from azure.mgmt.authorization import AuthorizationManagementClient  # type: ignore
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError  # type: ignore

from .util import UserReportError, safe_exec, handle_error
from .constants import CLUSTER_ERROR, DEPENDENCY_ERROR


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


# Module-level singleton, initialized on first use
_clients: Optional[AzureClients] = None


def _get_subscription_id() -> str:
    """Get Azure subscription ID from az CLI profile or Azure REST API."""
    # Primary: az CLI (fast, no extra dependencies)
    import subprocess
    try:
        result = subprocess.run(
            ['az', 'account', 'show', '--query', 'id', '-o', 'tsv'],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    # Fallback: use azure.identity to list subscriptions via REST
    import requests
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/.default")
    resp = requests.get(
        "https://management.azure.com/subscriptions?api-version=2022-12-01",
        headers={"Authorization": f"Bearer {token.token}"}
    )
    resp.raise_for_status()
    subs = resp.json().get("value", [])
    if subs:
        return subs[0]["subscriptionId"]
    raise RuntimeError("No Azure subscription found")


def get_clients() -> AzureClients:
    """Get or create the module-level AzureClients singleton."""
    global _clients
    if _clients is None:
        _clients = AzureClients(_get_subscription_id())
    return _clients


def init_clients(subscription_id: str) -> AzureClients:
    """Initialize clients with a known subscription ID (for testing or explicit config)."""
    global _clients
    _clients = AzureClients(subscription_id)
    return _clients


# ---------------------------------------------------------------------------
# AKS Cluster Operations
# ---------------------------------------------------------------------------

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def check_cluster(resource_group: str, cluster_name: str, dry_run: bool = False) -> str:
    """Check AKS cluster provisioning state.

    Returns provisioning state string ('Succeeded', 'Creating', etc.),
    or empty string if cluster not found.
    """
    if dry_run:
        logging.info(f'SDK: check_cluster({resource_group}, {cluster_name})')
        return ''
    try:
        clients = get_clients()
        cluster = clients.aks.managed_clusters.get(resource_group, cluster_name)
        return cluster.provisioning_state or ''
    except ResourceNotFoundError:
        return ''
    except HttpResponseError as e:
        logging.warning(f'Error checking cluster {cluster_name}: {e.message}')
        return ''


def start_cluster(resource_group: str, cluster_name: str, *,
                  location: str,
                  machine_type: str,
                  num_nodes: int,
                  use_local_ssd: bool = False,
                  use_spot: bool = False,
                  tags: Optional[Dict[str, str]] = None,
                  k8s_version: Optional[str] = None,
                  dry_run: bool = False) -> Optional[Any]:
    """Create AKS cluster using Azure SDK. Returns LROPoller (non-blocking).

    Call poller.result() to wait for completion, or poll later for async.
    """
    # AKS cluster name validation: lowercase alphanumeric + hyphens, 1-63 chars
    import re
    if not re.match(r'^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$', cluster_name):
        raise ValueError(
            f'Invalid AKS cluster name "{cluster_name}". '
            'Must be 1-63 lowercase alphanumeric characters or hyphens, '
            'start/end with alphanumeric.')

    agent_pool_profile = {
        "name": "nodepool1",
        "count": num_nodes,
        "vm_size": machine_type,
        "os_disk_type": "Managed",
        "mode": "System",
        "enable_auto_scaling": True,
        "min_count": 1,             # System pool requires min 1
        "max_count": max(num_nodes * 3, 3),
        "type": "VirtualMachineScaleSets",
    }

    if use_spot:
        # System node pools cannot use Spot VMs (Azure limitation).
        # Spot requires a separate "User" node pool added after cluster creation.
        logging.warning('Spot VM requested but system pool does not support it. '
                        'Ignoring Spot for system pool. Add a user pool for Spot VMs.')

    # Filter None/empty kubernetes_version
    k8s_ver = k8s_version if k8s_version else None

    cluster_params: Dict[str, Any] = {
        "location": location,
        "tags": tags or {},
        "identity": {"type": "SystemAssigned"},
        "dns_prefix": cluster_name,
        "kubernetes_version": k8s_ver,
        "auto_upgrade_profile": {"upgrade_channel": "none"},
        "agent_pool_profiles": [agent_pool_profile],
        "network_profile": {"load_balancer_sku": "standard"},
        "storage_profile": {
            "blob_csi_driver": {"enabled": not use_local_ssd},
        },
    }

    if dry_run:
        logging.info(f'SDK: start_cluster({resource_group}, {cluster_name}, vm={machine_type}, nodes={num_nodes})')
        return None

    try:
        clients = get_clients()
        start = timer()
        logging.info(f'Creating AKS cluster {cluster_name} in {resource_group} ({machine_type} x {num_nodes})')
        poller = clients.aks.managed_clusters.begin_create_or_update(
            resource_group, cluster_name, cluster_params
        )
        end = timer()
        logging.debug(f'RUNTIME cluster-create-request {end - start:.1f} seconds (async poller returned)')
        return poller
    except HttpResponseError as e:
        raise UserReportError(CLUSTER_ERROR,
            f'Failed to create AKS cluster {cluster_name}: {e.message}') from e
    except ResourceNotFoundError as e:
        raise UserReportError(CLUSTER_ERROR,
            f'Resource not found when creating cluster {cluster_name}: {e.message}') from e
    except Exception as e:
        raise UserReportError(CLUSTER_ERROR,
            f'Azure SDK error creating cluster {cluster_name}: {e}') from e


def delete_cluster(resource_group: str, cluster_name: str, dry_run: bool = False) -> Optional[Any]:
    """Delete AKS cluster. Returns LROPoller (non-blocking)."""
    if dry_run:
        logging.info(f'SDK: delete_cluster({resource_group}, {cluster_name})')
        return None

    try:
        clients = get_clients()
        start = timer()
        poller = clients.aks.managed_clusters.begin_delete(resource_group, cluster_name)
        end = timer()
        logging.debug(f'RUNTIME cluster-delete-request {end - start:.1f} seconds')
        return poller
    except ResourceNotFoundError:
        logging.info(f'Cluster {cluster_name} not found, nothing to delete')
        return None
    except HttpResponseError as e:
        logging.warning(f'Failed to delete cluster {cluster_name}: {e.message}')
        return None


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def get_aks_clusters(resource_group: str, dry_run: bool = False) -> List[str]:
    """List AKS cluster names in a resource group."""
    if dry_run:
        logging.info(f'SDK: get_aks_clusters({resource_group})')
        return []
    clients = get_clients()
    clusters = clients.aks.managed_clusters.list_by_resource_group(resource_group)
    return [c.name for c in clusters]


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def get_aks_credentials(resource_group: str, cluster_name: str, dry_run: bool = False) -> str:
    """Get AKS credentials and write kubeconfig. Returns kubectl context name.

    Uses SDK to fetch kubeconfig, then writes to ~/.kube/config and returns the context.
    Still uses kubectl for context switching (upstream compatibility).
    """
    if dry_run:
        logging.info(f'SDK: get_aks_credentials({resource_group}, {cluster_name})')
        return 'k8s-uninitialized-context'

    try:
        clients = get_clients()
        cred_result = clients.aks.managed_clusters.list_cluster_user_credentials(
            resource_group, cluster_name
        )
    except ResourceNotFoundError:
        raise UserReportError(CLUSTER_ERROR,
            f'Cluster {cluster_name} not found in resource group {resource_group}. '
            'It may not have been created yet.')
    except HttpResponseError as e:
        raise UserReportError(CLUSTER_ERROR,
            f'Failed to get credentials for cluster {cluster_name}: {e.message}')

    if not cred_result.kubeconfigs:
        raise UserReportError(CLUSTER_ERROR, f'No kubeconfig returned for cluster {cluster_name}')

    kubeconfig_bytes = cred_result.kubeconfigs[0].value  # type: ignore[index]

    # Merge into ~/.kube/config
    kube_dir = os.path.expanduser('~/.kube')
    os.makedirs(kube_dir, exist_ok=True)
    kube_config_path = os.path.join(kube_dir, 'config')

    # Write to temp file then use kubectl to merge (safest approach with existing kubeconfigs)
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.yaml', delete=False) as tmp:
        tmp.write(kubeconfig_bytes)
        tmp_path = tmp.name

    try:
        # Merge kubeconfig using KUBECONFIG env var trick
        env = os.environ.copy()
        existing = env.get('KUBECONFIG', kube_config_path)
        env['KUBECONFIG'] = f'{existing}:{tmp_path}'
        import subprocess
        subprocess.run(
            ['kubectl', 'config', 'view', '--flatten'],
            stdout=open(kube_config_path, 'w'),
            env=env, check=True
        )
    finally:
        os.unlink(tmp_path)

    # Get current context
    p = safe_exec('kubectl config current-context'.split())
    return handle_error(p.stdout).strip()


def scale_node_pool(resource_group: str, cluster_name: str,
                    pool_name: str, node_count: int,
                    dry_run: bool = False) -> Optional[Any]:
    """Scale AKS node pool. Returns LROPoller."""
    if dry_run:
        logging.info(f'SDK: scale_node_pool({cluster_name}/{pool_name} -> {node_count})')
        return None

    clients = get_clients()
    logging.info(f'Scaling AKS node pool {pool_name} to {node_count} nodes')
    poller = clients.aks.agent_pools.begin_create_or_update(
        resource_group, cluster_name, pool_name,
        {"count": node_count}
    )
    return poller


# ---------------------------------------------------------------------------
# IAM / Role Assignments
# ---------------------------------------------------------------------------

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def set_role_assignments(resource_group: str, cluster_name: str,
                         storage_account: str,
                         acr_name: str, acr_resource_group: str,
                         dry_run: bool = False) -> None:
    """Assign all required roles to AKS kubelet managed identity using SDK."""
    if dry_run:
        logging.info(f'SDK: set_role_assignments({cluster_name})')
        return

    clients = get_clients()
    sub_id = clients.subscription_id

    # 1. Get kubelet identity principal ID
    cluster = clients.aks.managed_clusters.get(resource_group, cluster_name)
    kubelet_id = cluster.identity_profile["kubeletidentity"].object_id
    logging.debug(f'Kubelet identity: {kubelet_id}')

    # 2. Get storage account resource ID
    sa = clients.storage.storage_accounts.get_properties(resource_group, storage_account)
    sa_id = sa.id

    # 3. Get ACR resource ID
    # ACR doesn't have a dedicated mgmt client in basic SDK, use resource ID pattern
    acr_id = f'/subscriptions/{sub_id}/resourceGroups/{acr_resource_group}/providers/Microsoft.ContainerRegistry/registries/{acr_name}'

    import uuid
    role_assignments = [
        ('Storage Blob Data Contributor', sa_id),
        ('AcrPull', acr_id),
        ('Contributor', f'/subscriptions/{sub_id}'),
    ]

    # Role definition IDs (well-known)
    ROLE_DEFS = {
        'Storage Blob Data Contributor': 'ba92f5b4-2d11-453d-a403-e96b0029c9fe',
        'AcrPull': '7f951dda-4ed3-4680-a7ca-43fe172d538d',
        'Contributor': 'b24988ac-6180-42a0-ab88-20f7382dd24c',
    }

    for role_name, scope in role_assignments:
        role_def_id = f'/subscriptions/{sub_id}/providers/Microsoft.Authorization/roleDefinitions/{ROLE_DEFS[role_name]}'
        assignment_id = str(uuid.uuid4())
        try:
            clients.auth.role_assignments.create(
                scope, assignment_id,
                {
                    "role_definition_id": role_def_id,
                    "principal_id": kubelet_id,
                    "principal_type": "ServicePrincipal",
                }
            )
            logging.debug(f'Assigned {role_name} on {scope}')
        except HttpResponseError as e:
            if 'RoleAssignmentExists' in str(e):
                logging.debug(f'Role {role_name} already assigned, skipping')
            else:
                raise


# ---------------------------------------------------------------------------
# Disk / Snapshot Operations
# ---------------------------------------------------------------------------

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def get_disks(resource_group: str, dry_run: bool = False) -> List[str]:
    """List Azure managed disk names in a resource group."""
    if dry_run:
        logging.info(f'SDK: get_disks({resource_group})')
        return []
    clients = get_clients()
    return [d.name for d in clients.compute.disks.list_by_resource_group(resource_group)]


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def get_snapshots(resource_group: str, dry_run: bool = False) -> List[str]:
    """List Azure snapshot names in a resource group."""
    if dry_run:
        logging.info(f'SDK: get_snapshots({resource_group})')
        return []
    clients = get_clients()
    return [s.name for s in clients.compute.snapshots.list_by_resource_group(resource_group)]


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def delete_disk(resource_group: str, disk_name: str) -> None:
    """Delete an Azure managed disk."""
    if not disk_name:
        raise ValueError('No disk name provided')
    clients = get_clients()
    clients.compute.disks.begin_delete(resource_group, disk_name).result()
    logging.debug(f'Deleted disk {disk_name}')


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))  # type: ignore
def delete_snapshot(resource_group: str, snapshot_name: str) -> None:
    """Delete an Azure snapshot."""
    if not snapshot_name:
        raise ValueError('No snapshot name provided')
    clients = get_clients()
    clients.compute.snapshots.begin_delete(resource_group, snapshot_name).result()
    logging.debug(f'Deleted snapshot {snapshot_name}')


# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

def check_prerequisites() -> None:
    """Check that kubectl and azcopy are available.
    Azure SDK replaces az CLI — no longer needed for ARM operations.
    az CLI is still checked for backward compatibility but not strictly required.
    """
    from .util import SafeExecError
    import shutil

    # Check kubectl (still needed for K8s operations)
    try:
        p = safe_exec('kubectl version --output=json --client=true')
        logging.debug(f'{":".join(p.stdout.decode().split())}')
    except SafeExecError as e:
        raise UserReportError(DEPENDENCY_ERROR,
            f"Required pre-requisite 'kubectl' doesn't work. Details: {e.message}")

    # Check azcopy (still needed for blob transfers)
    if shutil.which('azcopy') is None:
        raise UserReportError(DEPENDENCY_ERROR,
            "Required pre-requisite 'azcopy' is not installed. "
            "Please install from https://aka.ms/downloadazcopy-v10-linux")

    # Check Azure SDK authentication (replaces az login check)
    try:
        credential = DefaultAzureCredential()
        credential.get_token("https://management.azure.com/.default")
        logging.debug('Azure SDK authentication: OK')
    except Exception as e:
        raise UserReportError(DEPENDENCY_ERROR,
            f"Azure authentication failed. Run 'az login' or configure Managed Identity. Details: {e}")
