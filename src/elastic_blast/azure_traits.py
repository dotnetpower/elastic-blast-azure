# for azure

"""
elb/azure_traits.py - helper module for Azure VM info, pricing, and DB partitioning

Author: Victor Joukov joukovv@ncbi.nlm.nih.gov
        Moon Hyuk Choi moonchoi@microsoft.com
"""

import logging
import math
import subprocess
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from .base import InstanceProperties
from .util import UserReportError, safe_exec
from .constants import DEPENDENCY_ERROR, SYSTEM_MEMORY_RESERVE
from datetime import datetime, timedelta, timezone
from azure.identity import DefaultAzureCredential # type: ignore
from azure.storage.blob import BlobServiceClient  # type: ignore

AZURE_HPC_MACHINES = {
    'Standard_HB120rs_v3': {'cpu': 120, 'memory': 480},  # 120 vCPU, 480 GB RAM
    'Standard_HC44rs': {'cpu': 44, 'memory': 352},  # 44 vCPU, 352 GB RAM
    'Standard_HB60rs': {'cpu': 60, 'memory': 240},  # 60 vCPU, 240 GB RAM
    'Standard_D16s_v3': {'cpu': 16, 'memory': 64},  # 16 vCPU, 64 GB RAM
    'Standard_D32s_v3': {'cpu': 32, 'memory': 128},  # 32 vCPU, 128 GB RAM
    'Standard_D64s_v3': {'cpu': 64, 'memory': 256},  # 64 vCPU, 256 GB RAM
    'Standard_E64s_v3': {'cpu': 64, 'memory': 432},  # 64 vCPU, 432 GB RAM
    'Standard_E64is_v3': {'cpu': 64, 'memory': 504},  # 64 vCPU, 504 GB RAM
    'Standard_D8s_v3': {'cpu': 8, 'memory': 32},  # 8 vCPU, 32 GB RAM
    # E-series v5 (memory-optimized, newer generation)
    'Standard_E16s_v5': {'cpu': 16, 'memory': 128},   # 16 vCPU, 128 GB RAM
    'Standard_E32s_v5': {'cpu': 32, 'memory': 256},    # 32 vCPU, 256 GB RAM
    'Standard_E48s_v5': {'cpu': 48, 'memory': 384},    # 48 vCPU, 384 GB RAM
    'Standard_E64s_v5': {'cpu': 64, 'memory': 512},    # 64 vCPU, 512 GB RAM
    'Standard_E96s_v5': {'cpu': 96, 'memory': 672},    # 96 vCPU, 672 GB RAM
    # E-series v5 with NVMe (bs = burstable storage)
    'Standard_E16bs_v5': {'cpu': 16, 'memory': 128},   # 16 vCPU, 128 GB RAM, 600GB NVMe
    'Standard_E32bs_v5': {'cpu': 32, 'memory': 256},    # 32 vCPU, 256 GB RAM, 1.2TB NVMe
    'Standard_E48bs_v5': {'cpu': 48, 'memory': 384},    # 48 vCPU, 384 GB RAM, 1.8TB NVMe
    'Standard_E64bs_v5': {'cpu': 64, 'memory': 512},    # 64 vCPU, 512 GB RAM, 2.4TB NVMe
    'Standard_E96bs_v5': {'cpu': 96, 'memory': 672},    # 96 vCPU, 672 GB RAM, 3.6TB NVMe
    # L-series v3 (storage-optimized, large NVMe for TB-scale BLAST DB)
    'Standard_L8s_v3': {'cpu': 8, 'memory': 64},     # 8 vCPU, 64 GB RAM, 1.9TB NVMe
    'Standard_L16s_v3': {'cpu': 16, 'memory': 128},   # 16 vCPU, 128 GB RAM, 3.8TB NVMe
    'Standard_L32s_v3': {'cpu': 32, 'memory': 256},   # 32 vCPU, 256 GB RAM, 3.8TB NVMe
    'Standard_L48s_v3': {'cpu': 48, 'memory': 384},   # 48 vCPU, 384 GB RAM, 5.7TB NVMe
    'Standard_L64s_v3': {'cpu': 64, 'memory': 512},   # 64 vCPU, 512 GB RAM, 7.6TB NVMe
    'Standard_L80s_v3': {'cpu': 80, 'memory': 640},   # 80 vCPU, 640 GB RAM, 9.6TB NVMe
    # L-series v3 AMD (storage-optimized, NVMe, AMD EPYC)
    'Standard_L8as_v3': {'cpu': 8, 'memory': 64},     # 8 vCPU, 64 GB RAM, 1.9TB NVMe
    'Standard_L16as_v3': {'cpu': 16, 'memory': 128},   # 16 vCPU, 128 GB RAM, 3.8TB NVMe
    'Standard_L32as_v3': {'cpu': 32, 'memory': 256},   # 32 vCPU, 256 GB RAM, 3.8TB NVMe
    'Standard_L48as_v3': {'cpu': 48, 'memory': 384},   # 48 vCPU, 384 GB RAM, 5.7TB NVMe
    'Standard_L64as_v3': {'cpu': 64, 'memory': 512},   # 64 vCPU, 512 GB RAM, 7.6TB NVMe
    'Standard_L80as_v3': {'cpu': 80, 'memory': 640},   # 80 vCPU, 640 GB RAM, 9.6TB NVMe
}

MIN_PROCESSORS = 8
MIN_MEMORY = 24 # GB


def get_sas_token(storage_account: str, storage_account_container: str, storage_account_key: str) -> str:
    """Return empty string — using Managed Identity (azcopy login --identity) instead of SAS tokens.
    Retained for interface compatibility with elb_config.py."""
    return ''


def get_blob_service_client(storage_account: str) -> BlobServiceClient:
    """Create Azure Blob Service Client using DefaultAzureCredential (Managed Identity).
    
    This uses the Azure Identity chain which supports:
    - Managed Identity (in AKS pods)
    - Azure CLI credentials (local development)
    - Environment variables (service principal)
    """
    account_url = f"https://{storage_account}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)


def parse_blob_url(blob_url: str):
    """Parse https://<account>.blob.core.windows.net/<container>/<path>.

    Returns (account, container, blob_name) or raises ValueError.
    """
    from urllib.parse import urlparse
    if not blob_url.startswith('https://'):
        raise ValueError(f'not an Azure blob URL: {blob_url}')
    u = urlparse(blob_url)
    host = u.netloc
    if not host.endswith('.blob.core.windows.net'):
        raise ValueError(f'not an Azure blob URL: {blob_url}')
    account = host.split('.', 1)[0]
    parts = u.path.lstrip('/').split('/', 1)
    if len(parts) < 2 or not parts[1]:
        raise ValueError(f'blob URL missing container/name: {blob_url}')
    return account, parts[0], parts[1]


def azure_blob_exists(blob_url: str) -> bool:
    """Return True if the blob at the given Azure URL exists.

    Uses Managed Identity / DefaultAzureCredential. Best-effort: any error
    other than the explicit "not found" surfaces as False so callers can
    treat the probe as advisory (e.g., idempotency checks).
    """
    try:
        from azure.core.exceptions import ResourceNotFoundError  # type: ignore
        account, container, name = parse_blob_url(blob_url)
        client = get_blob_service_client(account)
        blob = client.get_blob_client(container=container, blob=name)
        try:
            blob.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False
    except Exception as e:  # noqa: BLE001
        logging.debug(f'azure_blob_exists({blob_url}) error: {e}')
        return False


def get_latest_dir(storage_account: str, storage_account_container: str, storage_account_key: str = '') -> str:
    """Get the latest directory from Azure Blob Storage using Managed Identity."""
    try:
        client = get_blob_service_client(storage_account)
        container = client.get_container_client(storage_account_container)

        # get all folders
        folder_list = []
        for blob in container.walk_blobs(name_starts_with='/'):
            if blob.name.endswith('/'):
                folder_list.append(blob.name[:-1]) # remove trailing slash and add to list

        # get the latest folder
        latest_dir = ''
        latest_time = datetime.min.replace(tzinfo=timezone.utc)
        for blob in container.list_blobs():
            if blob.name in folder_list:
                if blob.last_modified > latest_time:
                    latest_time = blob.last_modified
                    latest_dir = blob.name
        return latest_dir
    except Exception as e:
        logging.warning(f'get_latest_dir failed (not needed for custom DB URLs): {e}')
        return ''
    
    

def get_machine_properties(machineType: str) -> InstanceProperties:
    """Given the Azure VM size, returns a tuple of number of CPUs and amount of RAM in GB."""
    if machineType in AZURE_HPC_MACHINES:
        properties = AZURE_HPC_MACHINES[machineType]
        ncpu = properties['cpu']
        nram = properties['memory']
    else:
        err = f'Cannot get properties for {machineType}'
        raise NotImplementedError(err)
    
    return InstanceProperties(ncpu, nram)

def get_instance_type_offerings(region: str) -> List[Dict[str, Any]]:
    """Get a list of instance types offered in an Azure region"""
    try:
        jmespath_query = f'[?numberOfCores >= `{MIN_PROCESSORS}` && memoryInMB >= `{MIN_MEMORY*1024}`]'
        result = subprocess.run(
            ['az', 'vm', 'list-sizes', '--location', region,
             '--query', jmespath_query, '-o', 'json'],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        
        vm_list = json.loads(result.stdout)
        
        if not vm_list:
            raise ValueError(f"VM size '{vm_list}' not found in location '{region}'")
        
        # return [vm['name'] for vm in vm_list]
        return vm_list
        
    except subprocess.CalledProcessError as e:
        logging.error(f'Error getting instance types in region {region}: {e.stderr}')
        raise UserReportError(returncode=DEPENDENCY_ERROR, message=f'Error getting instance types in region {region}')


# ---------------------------------------------------------------------------
# Azure VM Pricing
# ---------------------------------------------------------------------------

# Azure VM hourly pricing (pay-as-you-go, East US region)
# Source: https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/
# These are approximate on-demand prices; actual prices vary by region.
AZURE_VM_HOURLY_PRICES = {
    # D-series v3 (general purpose)
    'Standard_D8s_v3': 0.384,
    'Standard_D16s_v3': 0.768,
    'Standard_D32s_v3': 1.536,
    'Standard_D64s_v3': 3.072,
    # E-series v3 (memory-optimized)
    'Standard_E64s_v3': 3.629,
    'Standard_E64is_v3': 3.629,
    # E-series v5 (memory-optimized, newer generation)
    'Standard_E16s_v5': 1.008,
    'Standard_E32s_v5': 2.016,
    'Standard_E48s_v5': 3.024,
    'Standard_E64s_v5': 4.032,
    'Standard_E96s_v5': 6.048,
    # E-series v5 with NVMe
    'Standard_E16bs_v5': 1.192,
    'Standard_E32bs_v5': 2.432,
    'Standard_E48bs_v5': 3.576,
    'Standard_E64bs_v5': 4.864,
    'Standard_E96bs_v5': 7.296,
    # L-series v3 (storage-optimized, Intel)
    'Standard_L8s_v3': 0.624,
    'Standard_L16s_v3': 1.248,
    'Standard_L32s_v3': 2.496,
    'Standard_L48s_v3': 3.744,
    'Standard_L64s_v3': 4.992,
    'Standard_L80s_v3': 6.240,
    # L-series v3 (storage-optimized, AMD)
    'Standard_L8as_v3': 0.624,
    'Standard_L16as_v3': 1.248,
    'Standard_L32as_v3': 2.496,
    'Standard_L48as_v3': 3.744,
    'Standard_L64as_v3': 4.992,
    'Standard_L80as_v3': 6.240,
    # HPC
    'Standard_HB120rs_v3': 3.600,
    'Standard_HC44rs': 3.168,
    'Standard_HB60rs': 2.280,
}

# Spot VM discount (typically 60-90% off on-demand)
SPOT_DISCOUNT_FACTOR = 0.3  # 70% discount on average


@dataclass
class CostEstimate:
    """Cost estimate for an ElasticBLAST search."""
    compute_per_hour: float
    estimated_hours: float
    total_compute: float
    storage_cost: float
    total: float
    vm_type: str
    num_nodes: int
    is_spot: bool

    def __str__(self) -> str:
        spot_label = ' (Spot)' if self.is_spot else ''
        return (
            f'Cost Estimate{spot_label}:\n'
            f'  VM: {self.vm_type} x {self.num_nodes} nodes\n'
            f'  Compute: ${self.compute_per_hour:.2f}/hr x {self.estimated_hours:.1f}hr = ${self.total_compute:.2f}\n'
            f'  Storage: ${self.storage_cost:.2f}\n'
            f'  Total: ${self.total:.2f}'
        )


def estimate_cost(machine_type: str, num_nodes: int,
                  estimated_hours: float = 2.0,
                  db_size_gb: float = 0.0,
                  use_spot: bool = False) -> CostEstimate:
    """Estimate Azure cost for an ElasticBLAST search."""
    hourly_rate = AZURE_VM_HOURLY_PRICES.get(machine_type)
    if hourly_rate is None:
        logging.warning(f'No pricing data for {machine_type}, using $2.00/hr estimate')
        hourly_rate = 2.00

    if use_spot:
        hourly_rate *= SPOT_DISCOUNT_FACTOR

    compute_per_hour = hourly_rate * num_nodes
    total_compute = compute_per_hour * estimated_hours

    # Azure Blob Storage: ~$0.018/GB/month for Hot tier
    storage_monthly = db_size_gb * 0.018
    storage_cost = storage_monthly * (estimated_hours / 720)  # Prorate to runtime

    return CostEstimate(
        compute_per_hour=compute_per_hour,
        estimated_hours=estimated_hours,
        total_compute=total_compute,
        storage_cost=storage_cost,
        total=total_compute + storage_cost,
        vm_type=machine_type,
        num_nodes=num_nodes,
        is_spot=use_spot,
    )


# ---------------------------------------------------------------------------
# Auto DB Partitioning (pure calculation, no ElasticBlastConfig dependency)
# ---------------------------------------------------------------------------

# Fraction of node RAM usable for DB caching (leave room for OS + BLAST overhead)
DB_RAM_FRACTION = 0.75

# Minimum / maximum auto-partition count
MIN_PARTITIONS = 2
MAX_PARTITIONS = 100

# Minimum DB size (GB) to trigger auto-partitioning
MIN_DB_SIZE_FOR_PARTITION_GB = 10.0


@dataclass
class PartitionPlan:
    """Result of auto-partition calculation."""
    db_partitions: int
    db_partition_prefix: str
    db_size_gb: float
    per_node_gb: float
    node_ram_gb: float
    reason: str

    def __str__(self) -> str:
        return (f'Auto-partition plan: {self.db_partitions} partitions '
                f'(DB {self.db_size_gb:.1f} GB, {self.per_node_gb:.1f} GB/node, '
                f'node RAM {self.node_ram_gb:.0f} GB). {self.reason}')


def compute_partition_plan(
    db_size_gb: float,
    node_ram_gb: float,
    num_nodes: int,
    db_url: str,
    use_local_ssd: bool = True,
    partition_prefix_override: Optional[str] = None,
) -> PartitionPlan:
    """Calculate optimal partition count and prefix.

    Args:
        db_size_gb: Total DB size in GB (from db_metadata.bytes_to_cache).
        node_ram_gb: RAM per node in GB (from machine type properties).
        num_nodes: Number of AKS nodes.
        db_url: Original DB URL.
        use_local_ssd: Whether local-SSD mode is enabled.
        partition_prefix_override: If set, use this prefix instead of deriving from db_url.

    Returns:
        PartitionPlan with computed values.

    Raises:
        ValueError: If inputs are invalid.
    """
    if db_size_gb <= 0:
        raise ValueError(f'DB size must be positive, got {db_size_gb:.1f} GB')
    if node_ram_gb <= 0:
        raise ValueError(f'Node RAM must be positive, got {node_ram_gb:.1f} GB')
    if num_nodes <= 0:
        raise ValueError(f'Number of nodes must be positive, got {num_nodes}')

    usable_ram_gb = (node_ram_gb - SYSTEM_MEMORY_RESERVE) * DB_RAM_FRACTION
    if usable_ram_gb <= 0:
        raise ValueError(
            f'Node RAM ({node_ram_gb:.1f} GB) is too small after reserving '
            f'{SYSTEM_MEMORY_RESERVE} GB for system. Cannot compute partition plan.')

    if db_size_gb < MIN_DB_SIZE_FOR_PARTITION_GB:
        return PartitionPlan(
            db_partitions=0, db_partition_prefix='',
            db_size_gb=db_size_gb, per_node_gb=db_size_gb,
            node_ram_gb=node_ram_gb,
            reason=f'DB too small ({db_size_gb:.1f} GB < {MIN_DB_SIZE_FOR_PARTITION_GB} GB), no partitioning needed.',
        )

    if db_size_gb <= usable_ram_gb:
        return PartitionPlan(
            db_partitions=0, db_partition_prefix='',
            db_size_gb=db_size_gb, per_node_gb=db_size_gb,
            node_ram_gb=node_ram_gb,
            reason=f'DB fits in single node RAM ({db_size_gb:.1f} GB <= {usable_ram_gb:.1f} GB usable).',
        )

    # DB exceeds single-node RAM: calculate minimum partitions
    min_partitions = math.ceil(db_size_gb / usable_ram_gb)

    if use_local_ssd:
        partitions = max(min_partitions, num_nodes)
    else:
        partitions = min_partitions

    partitions = max(partitions, MIN_PARTITIONS)
    partitions = min(partitions, MAX_PARTITIONS)

    per_node_gb = round(db_size_gb / partitions, 1)

    if per_node_gb > usable_ram_gb:
        logging.warning(
            f'Auto-partition capped at {MAX_PARTITIONS} but each shard '
            f'({per_node_gb:.1f} GB) still exceeds node usable RAM '
            f'({usable_ram_gb:.1f} GB). Consider using larger VMs or more nodes.')

    prefix = partition_prefix_override or _derive_partition_prefix(db_url, partitions)
    if not prefix:
        raise ValueError(
            f'Cannot derive partition prefix from DB URL: {db_url}. '
            f'Set db-partition-prefix manually in the config.')

    reason = (f'DB ({db_size_gb:.1f} GB) exceeds node RAM ({usable_ram_gb:.1f} GB usable). '
              f'Splitting into {partitions} partitions ({per_node_gb:.1f} GB each).')

    return PartitionPlan(
        db_partitions=partitions, db_partition_prefix=prefix,
        db_size_gb=db_size_gb, per_node_gb=per_node_gb,
        node_ram_gb=node_ram_gb, reason=reason,
    )


def _derive_partition_prefix(db_url: str, num_partitions: int) -> str:
    """Derive partition prefix from the original DB URL.

    Convention: shards are stored in a sibling directory named `{N}shards/`
    with files named `{db_basename}_shard_`.

    Example:
        db_url = 'https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt'
        num_partitions = 10
        -> 'https://stgelb.blob.core.windows.net/blast-db/10shards/core_nt_shard_'
    """
    parts = db_url.rsplit('/', 1)
    if len(parts) != 2:
        logging.warning(f'Cannot derive partition prefix from DB URL: {db_url}')
        return ''

    db_basename = parts[1]

    parent_parts = parts[0].rsplit('/', 1)
    if len(parent_parts) == 2:
        container_base = parent_parts[0]
    else:
        container_base = parts[0]

    prefix = f'{container_base}/{num_partitions}shards/{db_basename}_shard_'
    return prefix


def apply_auto_partition(cfg) -> Optional[PartitionPlan]:
    """Apply auto-partitioning to an ElasticBlastConfig if enabled.

    Modifies cfg.blast.db_partitions and cfg.blast.db_partition_prefix in-place
    if auto-partitioning determines sharding is needed.

    Returns the PartitionPlan if partitioning was applied, None otherwise.
    """
    blast = cfg.blast
    cluster = cfg.cluster

    # Skip if manual partitions are already set
    if blast.db_partitions > 0:
        logging.debug('Manual db-partitions already set, skipping auto-partition.')
        return None

    # Need DB metadata to know DB size
    if not blast.db_metadata:
        logging.warning('No DB metadata available for auto-partition calculation. '
                        'Set db-partitions manually or provide a DB with metadata.')
        return None

    db_size_gb = blast.db_metadata.bytes_to_cache / (1024 ** 3)

    try:
        props = get_machine_properties(cluster.machine_type)
        node_ram_gb = props.memory
    except Exception as e:
        logging.warning(f'Cannot get machine properties for {cluster.machine_type}: {e}. '
                        'Skipping auto-partition.')
        return None

    try:
        plan = compute_partition_plan(
            db_size_gb=db_size_gb,
            node_ram_gb=node_ram_gb,
            num_nodes=cluster.num_nodes,
            db_url=blast.db,
            use_local_ssd=cluster.use_local_ssd,
            partition_prefix_override=blast.db_partition_prefix or None,
        )
    except ValueError as e:
        logging.warning(f'Auto-partition calculation failed: {e}')
        return None

    if plan.db_partitions > 0:
        blast.db_partitions = plan.db_partitions
        blast.db_partition_prefix = plan.db_partition_prefix
        logging.info(str(plan))
    else:
        logging.info(f'Auto-partition: {plan.reason}')

    return plan

