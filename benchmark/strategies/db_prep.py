#!/usr/bin/env python3
"""DB preparation tools for benchmark strategies.

Provides functions to:
- Shard a BLAST DB into N pieces using blastdb_aliastool
- Create taxonomy-based subset DBs using blastdbcmd + makeblastdb
- Query DB metadata (size, volume count, total letters)
"""

import json
import subprocess
import os
import shutil
import logging
from pathlib import Path
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class DBInfo:
    """BLAST database metadata."""
    name: str
    path: str
    db_type: str          # 'nucl' or 'prot'
    num_volumes: int
    total_letters: int
    total_sequences: int
    size_bytes: int       # total file size on disk


def get_db_info(db_path: str) -> DBInfo:
    """Get BLAST DB metadata using blastdbcmd."""
    result = subprocess.run(
        ['blastdbcmd', '-db', db_path, '-info', '-json'],
        capture_output=True, text=True, check=True
    )
    info = json.loads(result.stdout)

    # Get total file size
    db_dir = os.path.dirname(db_path) or '.'
    db_name = os.path.basename(db_path)
    size = sum(
        f.stat().st_size
        for f in Path(db_dir).glob(f'{db_name}*')
        if f.is_file()
    )

    return DBInfo(
        name=db_name,
        path=db_path,
        db_type=info.get('db-type', 'nucl'),
        num_volumes=info.get('num-volumes', 1),
        total_letters=info.get('total-letters', 0),
        total_sequences=info.get('total-sequences', 0),
        size_bytes=size,
    )


def shard_db(db_path: str, num_shards: int, output_dir: str) -> list[str]:
    """Shard a BLAST DB into N alias databases.

    Uses blastdb_aliastool to create virtual shards by grouping existing
    volumes. No data duplication — alias files point to original volumes.

    Args:
        db_path: Path to the BLAST database
        num_shards: Number of shards to create
        output_dir: Directory to write shard alias files

    Returns:
        List of shard DB paths (e.g., ['/out/db_shard_00', ...])
    """
    os.makedirs(output_dir, exist_ok=True)

    info = get_db_info(db_path)
    db_name = info.name
    db_type = info.db_type

    # Discover existing volume files
    db_dir = os.path.dirname(db_path) or '.'
    volumes = sorted([
        f.stem for f in Path(db_dir).glob(f'{db_name}.[0-9][0-9].*')
    ])
    # Deduplicate (each volume has multiple extension files)
    volume_names = sorted(set(volumes))

    if not volume_names:
        # Single-volume DB
        volume_names = [db_name]

    num_shards = min(num_shards, len(volume_names))
    log.info(f'Sharding {db_name} ({len(volume_names)} volumes) into {num_shards} shards')

    # Distribute volumes across shards (round-robin)
    shard_volumes: list[list[str]] = [[] for _ in range(num_shards)]
    for i, vol in enumerate(volume_names):
        shard_volumes[i % num_shards].append(vol)

    shard_paths = []
    for shard_idx, vols in enumerate(shard_volumes):
        shard_name = f'{db_name}_shard_{shard_idx:02d}'
        shard_path = os.path.join(output_dir, shard_name)

        # Create alias DB
        vol_list = ' '.join(os.path.join(db_dir, v) for v in vols)
        cmd = [
            'blastdb_aliastool',
            '-dblist', vol_list,
            '-dbtype', db_type,
            '-out', shard_path,
            '-title', f'{db_name} shard {shard_idx}',
        ]
        log.info(f'  Shard {shard_idx}: {len(vols)} volumes -> {shard_name}')
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        shard_paths.append(shard_path)

    log.info(f'Created {len(shard_paths)} shards in {output_dir}')
    return shard_paths


def create_taxonomy_subset(
    db_path: str,
    taxids: list[int],
    output_path: str,
    db_type: str = 'nucl',
    exclude: bool = False,
) -> str:
    """Create a taxonomy-based subset of a BLAST DB.

    Args:
        db_path: Path to source BLAST database
        taxids: List of NCBI taxonomy IDs to include/exclude
        output_path: Path for the output subset DB
        db_type: 'nucl' or 'prot'
        exclude: If True, exclude these taxids instead of including

    Returns:
        Path to the created subset DB
    """
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    # Write taxid list to temp file
    taxid_file = output_path + '.taxids'
    with open(taxid_file, 'w') as f:
        for tid in taxids:
            f.write(f'{tid}\n')

    # Extract sequences
    fasta_file = output_path + '.fa'
    taxid_flag = '-negative_taxidlist' if exclude else '-taxidlist'
    log.info(f'Extracting {"excluded" if exclude else "included"} taxids: {taxids}')

    subprocess.run([
        'blastdbcmd', '-db', db_path,
        taxid_flag, taxid_file,
        '-out', fasta_file,
    ], check=True, capture_output=True, text=True)

    # Build new DB
    log.info(f'Building subset DB: {output_path}')
    subprocess.run([
        'makeblastdb',
        '-in', fasta_file,
        '-dbtype', db_type,
        '-out', output_path,
        '-title', f'{os.path.basename(output_path)} taxonomy subset',
        '-parse_seqids',
        '-taxid_map', taxid_file,
    ], check=True, capture_output=True, text=True)

    # Cleanup temp files
    os.remove(fasta_file)
    os.remove(taxid_file)

    log.info(f'Subset DB created: {output_path}')
    return output_path


# Well-known taxonomy IDs for pathogen detection
TAXIDS = {
    'virus': 10239,
    'bacteria': 2,
    'archaea': 2157,
    'eukaryota': 2759,
    'plasmodium': 5820,         # Malaria parasites
    'sars_cov_2': 2697049,      # SARS-CoV-2
    'orthopoxvirus': 10242,     # Monkeypox, Smallpox, etc.
    'human': 9606,
}


def create_pathogen_subset(db_path: str, output_dir: str) -> str:
    """Create a pathogen-optimized subset DB for the customer's 3 pathogens.

    Includes: Virus (10239) + Plasmodium (5820)
    Covers: SARS-CoV-2, Monkeypox, P. falciparum
    """
    return create_taxonomy_subset(
        db_path=db_path,
        taxids=[TAXIDS['virus'], TAXIDS['plasmodium']],
        output_path=os.path.join(output_dir, 'core_nt_pathogen'),
        db_type='nucl',
    )
