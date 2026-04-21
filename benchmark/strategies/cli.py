#!/usr/bin/env python3
"""CLI tool to compare and select BLAST distribution strategies.

Usage:
    # Compare all strategies for a given workload
    python -m benchmark.strategies.cli compare \
        --db core_nt --db-size 500 --queries 300 --query-bases 900000

    # Show detailed plan for a specific strategy
    python -m benchmark.strategies.cli plan --strategy db_shard \
        --db core_nt --db-size 500 --queries 10 --query-bases 37746

    # Prepare DB for a strategy (shard or taxonomy subset)
    python -m benchmark.strategies.cli prepare --strategy taxonomy \
        --db-path /path/to/core_nt --output-dir /path/to/output

    # Generate INI configs for a strategy
    python -m benchmark.strategies.cli generate --strategy db_shard \
        --db core_nt --db-url https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt \
        --queries-url https://stgelb.blob.core.windows.net/queries/pathogen-10.fa \
        --output-dir benchmark/configs/v2/strategies
"""

import argparse
import logging
import os
import sys

from .blast_strategies import (
    BlastConfig,
    STRATEGIES,
    get_strategy,
    compare_strategies,
)

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)


def cmd_compare(args):
    """Compare all strategies side by side."""
    cfg = BlastConfig(
        program=args.program,
        db_name=args.db,
        db_size_gb=args.db_size,
        db_total_letters=args.db_letters,
        query_count=args.queries,
        query_total_bases=args.query_bases,
        batch_len=args.batch_len,
        num_nodes=args.nodes,
    )
    compare_strategies(cfg)


def cmd_plan(args):
    """Show detailed execution plan for a strategy."""
    cfg = BlastConfig(
        program=args.program,
        db_name=args.db,
        db_url=args.db_url,
        db_size_gb=args.db_size,
        db_total_letters=args.db_letters,
        queries_url=args.queries_url,
        query_count=args.queries,
        query_total_bases=args.query_bases,
        batch_len=args.batch_len,
        num_nodes=args.nodes,
        options=args.options,
    )

    kwargs = {}
    if args.strategy in ('db_shard', 'hybrid', 'preloaded'):
        kwargs['num_shards'] = args.shards

    strategy = get_strategy(args.strategy, **kwargs)
    plan = strategy.plan(cfg)

    print(f'\n{"="*60}')
    print(f' Strategy: {plan.name}')
    print(f' {plan.description}')
    print(f'{"="*60}')
    print(f' Nodes required: {plan.num_nodes}')
    print(f' Total jobs: {len(plan.jobs)}')
    print(f' Total DB I/O: {plan.total_db_io_gb:.0f} GB')
    print(f' Needs merge: {plan.needs_merge} ({plan.merge_type})')
    print(f'\n DB Downloads:')
    for node, url in plan.db_downloads.items():
        print(f'   {node}: {url}')
    print(f'\n Jobs:')
    for job in plan.jobs:
        extra = ''
        if job.dbsize:
            extra = f' -dbsize={job.dbsize}'
        if job.node_selector:
            extra += f' @{job.node_selector}'
        print(f'   {job.job_id}: {job.blast_program} '
              f'-query {os.path.basename(job.query_file)} '
              f'-db {job.db_path}{extra}')
    print()


def cmd_prepare(args):
    """Prepare DB for a strategy (shard or taxonomy)."""
    if args.strategy == 'taxonomy':
        from .db_prep import create_taxonomy_subset, TAXIDS
        taxids = [int(t) for t in args.taxids.split(',')] if args.taxids else [
            TAXIDS['virus'], TAXIDS['plasmodium']
        ]
        result = create_taxonomy_subset(
            db_path=args.db_path,
            taxids=taxids,
            output_path=os.path.join(args.output_dir, f'{os.path.basename(args.db_path)}_pathogen'),
        )
        print(f'Created taxonomy subset: {result}')

    elif args.strategy in ('db_shard', 'hybrid', 'preloaded'):
        from .db_prep import shard_db
        shards = shard_db(
            db_path=args.db_path,
            num_shards=args.shards,
            output_dir=args.output_dir,
        )
        print(f'Created {len(shards)} shards:')
        for s in shards:
            print(f'  {s}')

    else:
        print(f'Strategy "{args.strategy}" does not require DB preparation.')


def cmd_generate(args):
    """Generate INI config files for a strategy."""
    cfg = BlastConfig(
        program=args.program,
        db_name=args.db,
        db_url=args.db_url,
        db_size_gb=args.db_size,
        db_total_letters=args.db_letters,
        queries_url=args.queries_url,
        results_url=args.results_url,
        query_count=args.queries,
        query_total_bases=args.query_bases,
        batch_len=args.batch_len,
        num_nodes=args.nodes,
        machine_type=args.machine_type,
        options=args.options,
    )

    kwargs = {}
    if args.strategy in ('db_shard', 'hybrid', 'preloaded'):
        kwargs['num_shards'] = args.shards

    strategy = get_strategy(args.strategy, **kwargs)
    plan = strategy.plan(cfg)

    os.makedirs(args.output_dir, exist_ok=True)

    # Generate one INI per job
    for job in plan.jobs:
        ini_path = os.path.join(args.output_dir, f'{plan.name}-{job.job_id}.ini')

        ini_content = f"""[cloud-provider]
azure-region = {args.region}
azure-acr-resource-group = {args.acr_rg}
azure-acr-name = {args.acr_name}
azure-resource-group = {args.rg}
azure-storage-account = {args.storage}
azure-storage-account-container = blast-db

[cluster]
name = elb-v2-{plan.name}
machine-type = {cfg.machine_type}
num-nodes = {plan.num_nodes}
exp-use-local-ssd = true
reuse = {'true' if plan.name == 'preloaded' else 'false'}

[blast]
program = {job.blast_program}
db = {job.db_source}
queries = {job.query_file}
results = {cfg.results_url}/{plan.name}/{job.job_id}
options = {job.blast_options}
batch-len = {cfg.batch_len}
mem-limit = {cfg.mem_limit}
"""
        with open(ini_path, 'w') as f:
            f.write(ini_content)

    print(f'Generated {len(plan.jobs)} INI configs in {args.output_dir}/')
    print(f'Strategy: {plan.name}')
    print(f'Merge required: {plan.needs_merge} ({plan.merge_type})')


def main():
    parser = argparse.ArgumentParser(
        description='BLAST Distribution Strategy Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Common args
    def add_blast_args(p):
        p.add_argument('--db', default='core_nt', help='DB name')
        p.add_argument('--db-size', type=float, default=500, help='DB size in GB')
        p.add_argument('--db-letters', type=int, default=0, help='Total letters in DB')
        p.add_argument('--queries', type=int, default=10, help='Number of query sequences')
        p.add_argument('--query-bases', type=int, default=37746, help='Total query bases')
        p.add_argument('--batch-len', type=int, default=100000, help='Batch length')
        p.add_argument('--nodes', type=int, default=3, help='Number of nodes')
        p.add_argument('--program', default='blastn', help='BLAST program')
        p.add_argument('--shards', type=int, default=10, help='Number of DB shards')
        p.add_argument('--options', default='-max_target_seqs 500 -evalue 0.05 -outfmt "6 std"')

    # compare
    p_compare = subparsers.add_parser('compare', help='Compare all strategies')
    add_blast_args(p_compare)

    # plan
    p_plan = subparsers.add_parser('plan', help='Show strategy execution plan')
    p_plan.add_argument('--strategy', required=True, choices=list(STRATEGIES.keys()))
    p_plan.add_argument('--db-url', default='')
    p_plan.add_argument('--queries-url', default='')
    add_blast_args(p_plan)

    # prepare
    p_prepare = subparsers.add_parser('prepare', help='Prepare DB for strategy')
    p_prepare.add_argument('--strategy', required=True, choices=list(STRATEGIES.keys()))
    p_prepare.add_argument('--db-path', required=True, help='Local path to BLAST DB')
    p_prepare.add_argument('--output-dir', required=True, help='Output directory')
    p_prepare.add_argument('--shards', type=int, default=10)
    p_prepare.add_argument('--taxids', default='', help='Comma-separated taxids')

    # generate
    p_generate = subparsers.add_parser('generate', help='Generate INI configs')
    p_generate.add_argument('--strategy', required=True, choices=list(STRATEGIES.keys()))
    p_generate.add_argument('--db-url', required=True)
    p_generate.add_argument('--queries-url', required=True)
    p_generate.add_argument('--results-url', default='https://stgelb.blob.core.windows.net/results')
    p_generate.add_argument('--output-dir', default='benchmark/configs/v2/strategies')
    p_generate.add_argument('--region', default='koreacentral')
    p_generate.add_argument('--rg', default='rg-elb-koc')
    p_generate.add_argument('--storage', default='stgelb')
    p_generate.add_argument('--acr-rg', default='rg-elbacr')
    p_generate.add_argument('--acr-name', default='elbacr')
    p_generate.add_argument('--machine-type', default='Standard_E64s_v3')
    add_blast_args(p_generate)

    args = parser.parse_args()

    if args.command == 'compare':
        cmd_compare(args)
    elif args.command == 'plan':
        cmd_plan(args)
    elif args.command == 'prepare':
        cmd_prepare(args)
    elif args.command == 'generate':
        cmd_generate(args)


if __name__ == '__main__':
    main()
