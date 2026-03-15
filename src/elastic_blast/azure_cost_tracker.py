"""
elastic_blast/azure_cost_tracker.py — Azure cost estimation for ElasticBLAST searches

Provides pre-execution cost estimates based on VM type, node count, and expected
runtime. Helps SaaS operators and users understand costs before committing.

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import logging
from dataclasses import dataclass
from typing import Optional


# Azure VM hourly pricing (pay-as-you-go, East US region)
# Source: https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/
# These are approximate on-demand prices; actual prices vary by region.
AZURE_VM_HOURLY_PRICES = {
    'Standard_D8s_v3': 0.384,
    'Standard_D16s_v3': 0.768,
    'Standard_D32s_v3': 1.536,
    'Standard_D64s_v3': 3.072,
    'Standard_E16s_v3': 1.008,
    'Standard_E32s_v3': 2.016,
    'Standard_E64s_v3': 3.629,
    'Standard_E64is_v3': 3.629,
    'Standard_E32bs_v5': 2.432,
    'Standard_E64bs_v5': 4.864,
    'Standard_E96bs_v5': 7.296,
    'Standard_L8s_v3': 0.624,
    'Standard_L16s_v3': 1.248,
    'Standard_L32s_v3': 2.496,
    'Standard_L48s_v3': 3.744,
    'Standard_L64s_v3': 4.992,
    'Standard_L80s_v3': 6.240,
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
    """Estimate Azure cost for an ElasticBLAST search.

    Args:
        machine_type: Azure VM size (e.g., 'Standard_E32s_v3')
        num_nodes: Number of AKS nodes
        estimated_hours: Expected runtime in hours
        db_size_gb: Database size in GB (for storage cost)
        use_spot: Whether Spot VMs are used

    Returns:
        CostEstimate with breakdown
    """
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
