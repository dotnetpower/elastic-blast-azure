"""
elastic_blast/azure_optimizer.py — Optimization profiles for Azure ElasticBLAST

Provides three optimization profiles (cost/balanced/performance) that auto-tune
cluster parameters based on query size and DB size. Shows predicted time and cost
before execution, giving users informed control.

Usage:
  CLI:  ELB_OPTIMIZATION=cost elastic-blast submit --cfg config.ini
  API:  optimizer = AzureOptimizer(cfg); optimizer.apply()

Authors: Moon Hyuk Choi moonchoi@microsoft.com
"""

import logging
import math
import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from .azure_cost_tracker import AZURE_VM_HOURLY_PRICES, SPOT_DISCOUNT_FACTOR


class OptimizationProfile(str, Enum):
    COST = 'cost'
    BALANCED = 'balanced'
    PERFORMANCE = 'performance'


# VM recommendations per profile
_VM_PROFILES = {
    OptimizationProfile.COST: {
        'default_vm': 'Standard_D8s_v3',
        'large_db_vm': 'Standard_E16s_v3',    # DB > 100GB
        'spot': True,
        'vmtouch_pct': 0.5,
        'azcopy_concurrency': 16,
        'skip_db_verify': True,
        'min_count_zero': True,
    },
    OptimizationProfile.BALANCED: {
        'default_vm': 'Standard_E32s_v3',
        'large_db_vm': 'Standard_E32s_v3',
        'spot': True,
        'vmtouch_pct': 0.8,
        'azcopy_concurrency': 64,
        'skip_db_verify': True,
        'min_count_zero': True,
    },
    OptimizationProfile.PERFORMANCE: {
        'default_vm': 'Standard_E64bs_v5',
        'large_db_vm': 'Standard_E64bs_v5',
        'spot': False,
        'vmtouch_pct': 0.9,
        'azcopy_concurrency': 128,
        'skip_db_verify': False,
        'min_count_zero': False,
    },
}

# VM specs: {vm_type: (vcpu, ram_gb, hourly_price)}
_VM_SPECS = {
    'Standard_D8s_v3':   (8,   32,   0.384),
    'Standard_D16s_v3':  (16,  64,   0.768),
    'Standard_D32s_v3':  (32,  128,  1.536),
    'Standard_E16s_v3':  (16,  128,  1.008),
    'Standard_E32s_v3':  (32,  256,  2.016),
    'Standard_E64s_v3':  (64,  432,  3.629),
    'Standard_E32bs_v5': (32,  256,  2.432),
    'Standard_E64bs_v5': (64,  512,  4.864),
    'Standard_E96bs_v5': (96,  672,  7.296),
    'Standard_L32s_v3':  (32,  256,  2.496),
    'Standard_L64s_v3':  (64,  512,  4.992),
}


@dataclass
class Prediction:
    """Predicted time and cost for a search."""
    profile: str
    vm_type: str
    num_nodes: int
    use_spot: bool
    estimated_hours: float
    estimated_cost: float
    overhead_minutes: float
    blast_minutes: float
    db_cached_pct: float

    def __str__(self) -> str:
        spot_label = ' (Spot)' if self.use_spot else ''
        return (
            f'  Profile: {self.profile.upper()}\n'
            f'  VM: {self.vm_type}{spot_label} x {self.num_nodes} nodes\n'
            f'  DB RAM cache: {self.db_cached_pct:.0f}%\n'
            f'  Estimated time: {self.estimated_hours:.1f} hours '
            f'(overhead {self.overhead_minutes:.0f}min + BLAST {self.blast_minutes:.0f}min)\n'
            f'  Estimated cost: ${self.estimated_cost:.2f}'
        )


def get_profile() -> OptimizationProfile:
    """Get optimization profile from environment variable."""
    value = os.environ.get('ELB_OPTIMIZATION', 'balanced').lower()
    try:
        return OptimizationProfile(value)
    except ValueError:
        logging.warning(f'Unknown optimization profile "{value}", using balanced')
        return OptimizationProfile.BALANCED


def predict(profile: OptimizationProfile, *,
            query_size_gb: float,
            db_size_gb: float,
            batch_len: int = 100000,
            num_nodes: Optional[int] = None,
            vm_type: Optional[str] = None) -> Prediction:
    """Predict execution time and cost for a given profile and workload.

    Args:
        profile: optimization profile
        query_size_gb: total query size in GB
        db_size_gb: BLAST database size in GB
        batch_len: batch size for query splitting
        num_nodes: override node count (None = auto)
        vm_type: override VM type (None = auto from profile)
    """
    cfg = _VM_PROFILES[profile]

    # Auto-select VM
    if not vm_type:
        vm_type = cfg['large_db_vm'] if db_size_gb > 100 else cfg['default_vm']
    vcpu, ram_gb, hourly = _VM_SPECS.get(vm_type, (32, 256, 2.0))

    # Auto-select node count
    if not num_nodes:
        # Heuristic: enough nodes to hold DB in RAM across cluster
        min_nodes_for_db = max(1, math.ceil(db_size_gb / (ram_gb * cfg['vmtouch_pct'])))
        # Heuristic: enough nodes for parallelism (1 node per ~20 batches)
        num_batches = max(1, int(query_size_gb * 1024 * 1024 / batch_len))
        min_nodes_for_queries = max(1, math.ceil(num_batches / 20))
        # Performance: allow more nodes (1 per 5 batches)
        if profile == OptimizationProfile.PERFORMANCE:
            min_nodes_for_queries = max(1, math.ceil(num_batches / 5))
        max_nodes = 10 if profile == OptimizationProfile.COST else 50 if profile == OptimizationProfile.BALANCED else 200
        num_nodes = max(min_nodes_for_db, min(min_nodes_for_queries, max_nodes))

    use_spot = cfg['spot']

    # Estimate overhead (minutes)
    cluster_create = 15  # AKS provisioning (parallel with query split)
    query_split = max(5, query_size_gb * 30)  # ~30 min/TB
    overhead = max(cluster_create, query_split) + 2  # IAM + storage init

    # Estimate BLAST time (minutes)
    db_cache_pct = min(100, (ram_gb * cfg['vmtouch_pct'] * num_nodes / max(db_size_gb, 1)) * 100)
    # Phase 1 (DB read): 29min baseline, reduced by cache
    phase1_per_batch = 29 * (1 - db_cache_pct / 100) + 1  # min 1 min even with full cache
    # Phase 2 (compute): ~14 min, scales with CPU
    phase2_per_batch = 14
    # Phase 3 (write): ~1 min
    phase3_per_batch = 1
    batch_time = phase1_per_batch + phase2_per_batch + phase3_per_batch

    num_batches = max(1, int(query_size_gb * 1024 * 1024 / batch_len))
    blast_minutes = (num_batches * batch_time) / num_nodes

    total_hours = (overhead + blast_minutes) / 60
    price = hourly * (SPOT_DISCOUNT_FACTOR if use_spot else 1.0)
    cost = price * num_nodes * total_hours

    return Prediction(
        profile=profile.value,
        vm_type=vm_type,
        num_nodes=num_nodes,
        use_spot=use_spot,
        estimated_hours=total_hours,
        estimated_cost=cost,
        overhead_minutes=overhead,
        blast_minutes=blast_minutes,
        db_cached_pct=db_cache_pct,
    )


def predict_all_profiles(*, query_size_gb: float, db_size_gb: float,
                          batch_len: int = 100000) -> str:
    """Generate comparison table of all 3 profiles."""
    lines = [
        '',
        '╔══════════════════════════════════════════════════════════════════╗',
        '║           ElasticBLAST Azure — Optimization Profiles           ║',
        '╠══════════════════════════════════════════════════════════════════╣',
        f'║  Query: {query_size_gb:.1f} GB  |  DB: {db_size_gb:.1f} GB' +
        ' ' * max(0, 36 - len(f'{query_size_gb:.1f}') - len(f'{db_size_gb:.1f}')) + '║',
        '╠══════════════════════════════════════════════════════════════════╣',
    ]

    for profile in OptimizationProfile:
        p = predict(profile, query_size_gb=query_size_gb, db_size_gb=db_size_gb,
                    batch_len=batch_len)
        spot = 'Spot' if p.use_spot else 'On-demand'
        lines.append(f'║  [{profile.value.upper():^11s}]  '
                     f'{p.vm_type} x {p.num_nodes} ({spot})')
        lines.append(f'║    Time: ~{p.estimated_hours:.1f}h  '
                     f'Cost: ~${p.estimated_cost:.0f}  '
                     f'DB cache: {p.db_cached_pct:.0f}%')
        lines.append('║')

    lines.append('║  * Estimates may vary ±30% based on data complexity,')
    lines.append('║    network conditions, and Spot VM availability.')
    lines.append('╚══════════════════════════════════════════════════════════════════╝')
    lines.append('')
    return '\n'.join(lines)


def apply_profile(cfg, profile: Optional[OptimizationProfile] = None) -> Prediction:
    """Apply optimization profile to ElasticBlastConfig.

    Modifies cfg in-place based on the profile. Returns the prediction.
    Only overrides values that the user hasn't explicitly set.
    """
    if profile is None:
        profile = get_profile()

    p_cfg = _VM_PROFILES[profile]

    # Estimate sizes (use defaults if actual sizes unknown at this point)
    query_size_gb = float(os.environ.get('ELB_QUERY_SIZE_GB', '0.1'))
    db_size_gb = float(os.environ.get('ELB_DB_SIZE_GB', '10'))

    pred = predict(profile, query_size_gb=query_size_gb, db_size_gb=db_size_gb,
                   batch_len=cfg.blast.batch_len,
                   num_nodes=cfg.cluster.num_nodes if cfg.cluster.num_nodes > 1 else None,
                   vm_type=cfg.cluster.machine_type if cfg.cluster.machine_type != 'Standard_E32s_v3' else None)

    # Spot VMs: disabled for now — system pool doesn't support Spot.
    # TODO: Re-enable when user nodepool support is added.
    # if not cfg.cluster.use_preemptible and p_cfg['spot']:
    #     cfg.cluster.use_preemptible = True

    # Set environment variables for shell scripts
    os.environ['AZCOPY_CONCURRENCY_VALUE'] = str(p_cfg['azcopy_concurrency'])
    if p_cfg['skip_db_verify']:
        os.environ['ELB_SKIP_DB_VERIFY'] = 'true'

    # Always use reuse for cost/balanced
    if profile in (OptimizationProfile.COST, OptimizationProfile.BALANCED):
        cfg.cluster.reuse = True

    logging.info(f'Applied optimization profile: {profile.value}')
    return pred
