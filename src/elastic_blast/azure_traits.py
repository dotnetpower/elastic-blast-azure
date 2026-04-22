# for azure

"""
elb/azure_traits.py - helper module for Azure VM info

Author: Victor Joukov joukovv@ncbi.nlm.nih.gov
"""

import logging
import subprocess
import json
from typing import Any, Dict, List
from .base import InstanceProperties
from .util import UserReportError, safe_exec
from .constants import DEPENDENCY_ERROR
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
    'Standard_E16s_v3': {'cpu': 16, 'memory': 128},  # 16 vCPU, 128 GB RAM
    'Standard_E32s_v3': {'cpu': 32, 'memory': 256},  # 32 vCPU, 256 GB RAM
    'Standard_E48s_v3': {'cpu': 48, 'memory': 384},  # 48 vCPU, 384 GB RAM
    'Standard_E64s_v3': {'cpu': 64, 'memory': 432},  # 64 vCPU, 432 GB RAM
    'Standard_E64is_v3': {'cpu': 64, 'memory': 504},  # 64 vCPU, 504 GB RAM
    'Standard_D8s_v3': {'cpu': 8, 'memory': 32},  # 8 vCPU, 32 GB RAM
    # E-series v5 (memory-optimized, newer generation)
    'Standard_E32bs_v5': {'cpu': 32, 'memory': 256},  # 32 vCPU, 256 GB RAM, 1.2TB NVMe
    'Standard_E64bs_v5': {'cpu': 64, 'memory': 512},  # 64 vCPU, 512 GB RAM, 2.4TB NVMe
    'Standard_E96bs_v5': {'cpu': 96, 'memory': 672},  # 96 vCPU, 672 GB RAM, 3.6TB NVMe
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
        jmespath_query = f'"[?numberOfCores >= `{MIN_PROCESSORS}` && memoryInMB >= `{MIN_MEMORY*1024}`]"'
        cmd = f'az vm list-sizes --location {region} --query {jmespath_query} -o json'
        result = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        vm_list = json.loads(result.stdout)
        
        if not vm_list:
            raise ValueError(f"VM size '{vm_list}' not found in location '{region}'")
        
        # return [vm['name'] for vm in vm_list]
        return vm_list
        
    except subprocess.CalledProcessError as e:
        logging.error(f'Error getting instance types in region {region}: {e.stderr}')
        raise UserReportError(returncode=DEPENDENCY_ERROR, message=f'Error getting instance types in region {region}')
    


