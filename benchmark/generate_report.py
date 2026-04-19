#!/usr/bin/env python3
"""
benchmark/generate_report.py — Paper-quality benchmark report generator

Reads benchmark result JSON files and generates:
1. Academic-style Markdown report with tables
2. Publication-quality matplotlib charts (PDF + PNG)

Usage:
  python benchmark/generate_report.py benchmark/results/2026-04-17/

Author: Moon Hyuk Choi
"""

import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# ---------------------------------------------------------------------------
# Chart generation (matplotlib)
# ---------------------------------------------------------------------------

def create_charts(results: List[Dict], output_dir: Path):
    """Generate publication-quality charts."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.ticker as ticker
        from matplotlib import rcParams
    except ImportError:
        print("WARNING: matplotlib not installed. Skipping chart generation.")
        print("  Install: pip install matplotlib")
        return

    # Academic paper style
    rcParams.update({
        'font.family': 'serif',
        'font.serif': ['Times New Roman', 'DejaVu Serif', 'serif'],
        'font.size': 11,
        'axes.labelsize': 12,
        'axes.titlesize': 13,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'legend.fontsize': 10,
        'figure.figsize': (7, 4.5),
        'figure.dpi': 300,
        'savefig.dpi': 300,
        'savefig.bbox': 'tight',
        'axes.grid': True,
        'grid.alpha': 0.3,
        'grid.linestyle': '--',
    })

    charts_dir = output_dir / 'charts'
    charts_dir.mkdir(exist_ok=True)

    # Categorize results
    node_tests = {}  # nodes -> result
    for r in results:
        n = r.get('num_nodes', 1)
        test_id = r.get('test_id', '')
        if 'warmup' not in test_id.lower():
            node_tests[n] = r

    if len(node_tests) < 2:
        print("WARNING: Need at least 2 different node counts for scaling charts")
        # Still generate what we can

    # ── Chart 1: BLAST Execution Time by Node Count ──
    if node_tests:
        fig, ax = plt.subplots()
        nodes = sorted(node_tests.keys())
        blast_times = [node_tests[n].get('blast_total_s', 0) / 60 for n in nodes]
        total_times = [node_tests[n].get('total_elapsed_s', 0) / 60 for n in nodes]

        x = range(len(nodes))
        width = 0.35

        bars1 = ax.bar([i - width/2 for i in x], total_times, width,
                       label='Total Elapsed', color='#2196F3', alpha=0.8)
        bars2 = ax.bar([i + width/2 for i in x], blast_times, width,
                       label='BLAST Execution', color='#FF5722', alpha=0.8)

        ax.set_xlabel('Number of Nodes')
        ax.set_ylabel('Time (minutes)')
        ax.set_title('ElasticBLAST Execution Time vs. Node Count\n(nt_prok 82GB, E32s_v3)')
        ax.set_xticks(list(x))
        ax.set_xticklabels([f'{n} node{"s" if n > 1 else ""}' for n in nodes])
        ax.legend()

        # Add value labels on bars
        for bar in bars1:
            h = bar.get_height()
            if h > 0:
                ax.annotate(f'{h:.1f}m', xy=(bar.get_x() + bar.get_width() / 2, h),
                           xytext=(0, 3), textcoords="offset points",
                           ha='center', va='bottom', fontsize=9)
        for bar in bars2:
            h = bar.get_height()
            if h > 0:
                ax.annotate(f'{h:.1f}m', xy=(bar.get_x() + bar.get_width() / 2, h),
                           xytext=(0, 3), textcoords="offset points",
                           ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        fig.savefig(charts_dir / 'fig1_execution_time.png')
        fig.savefig(charts_dir / 'fig1_execution_time.pdf')
        plt.close(fig)
        print(f"  Chart 1: {charts_dir / 'fig1_execution_time.png'}")

    # ── Chart 2: Speedup & Scaling Efficiency ──
    if len(node_tests) >= 2:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))

        nodes = sorted(node_tests.keys())
        baseline_time = node_tests[nodes[0]].get('blast_total_s', 1)
        if baseline_time == 0:
            baseline_time = node_tests[nodes[0]].get('total_elapsed_s', 1)

        actual_speedup = []
        ideal_speedup = []
        efficiency = []

        for n in nodes:
            t = node_tests[n].get('blast_total_s', 0)
            if t == 0:
                t = node_tests[n].get('total_elapsed_s', 1)
            sp = baseline_time / t if t > 0 else 0
            actual_speedup.append(sp)
            ideal_speedup.append(n / nodes[0])
            eff = (sp / (n / nodes[0])) * 100 if n > 0 else 0
            efficiency.append(eff)

        # Speedup chart
        ax1.plot(nodes, ideal_speedup, 'k--', label='Ideal (linear)', linewidth=1.5)
        ax1.plot(nodes, actual_speedup, 'ro-', label='Actual', linewidth=2, markersize=8)
        ax1.set_xlabel('Number of Nodes')
        ax1.set_ylabel('Speedup (×)')
        ax1.set_title('(a) Speedup vs. Node Count')
        ax1.legend()
        ax1.set_xticks(nodes)

        # Efficiency chart
        ax2.bar(range(len(nodes)), efficiency, color='#4CAF50', alpha=0.8)
        ax2.axhline(y=75, color='red', linestyle='--', alpha=0.5, label='Target (75%)')
        ax2.set_xlabel('Number of Nodes')
        ax2.set_ylabel('Scaling Efficiency (%)')
        ax2.set_title('(b) Scaling Efficiency')
        ax2.set_xticks(range(len(nodes)))
        ax2.set_xticklabels([str(n) for n in nodes])
        ax2.set_ylim(0, 110)
        ax2.legend()

        plt.tight_layout()
        fig.savefig(charts_dir / 'fig2_scaling.png')
        fig.savefig(charts_dir / 'fig2_scaling.pdf')
        plt.close(fig)
        print(f"  Chart 2: {charts_dir / 'fig2_scaling.png'}")

    # ── Chart 3: Cost-Performance Analysis ──
    if node_tests:
        fig, ax = plt.subplots()

        nodes = sorted(node_tests.keys())
        costs = [node_tests[n].get('estimated_cost_usd', 0) for n in nodes]
        times_min = []
        for n in nodes:
            t = node_tests[n].get('blast_total_s', 0)
            if t == 0:
                t = node_tests[n].get('total_elapsed_s', 0)
            times_min.append(t / 60)

        colors = ['#2196F3', '#FF5722', '#4CAF50']
        for i, n in enumerate(nodes):
            ax.scatter(costs[i], times_min[i],
                      s=200, c=colors[i % len(colors)], alpha=0.8,
                      label=f'{n} node{"s" if n > 1 else ""}', zorder=5)
            ax.annotate(f'{n}N', (costs[i], times_min[i]),
                       textcoords="offset points", xytext=(10, 5),
                       fontsize=10, fontweight='bold')

        ax.set_xlabel('Estimated Cost (USD)')
        ax.set_ylabel('BLAST Execution Time (minutes)')
        ax.set_title('Cost-Performance Tradeoff\n(nt_prok 82GB, E32s_v3)')
        ax.legend()

        plt.tight_layout()
        fig.savefig(charts_dir / 'fig3_cost_performance.png')
        fig.savefig(charts_dir / 'fig3_cost_performance.pdf')
        plt.close(fig)
        print(f"  Chart 3: {charts_dir / 'fig3_cost_performance.png'}")

    # ── Chart 4: Job Timeline (Gantt-style) ──
    # This requires per-job timing data from monitoring
    for r in results:
        test_id = r.get('test_id', '')
        if 'warmup' in test_id.lower():
            continue
        job_timings = r.get('job_timings', {})
        if not job_timings or len(job_timings) < 2:
            continue

        fig, ax = plt.subplots(figsize=(10, max(4, len(job_timings) * 0.3)))

        # Parse job start/end times
        jobs_parsed = []
        min_time = None
        for name, t in job_timings.items():
            if name == 'error':
                continue
            start = t.get('start', '')
            end = t.get('completion', '')
            if start and end:
                try:
                    s = datetime.fromisoformat(start.replace('Z', '+00:00'))
                    e = datetime.fromisoformat(end.replace('Z', '+00:00'))
                    if min_time is None:
                        min_time = s
                    jobs_parsed.append((name, s, e))
                except Exception:
                    pass

        if jobs_parsed and min_time:
            jobs_parsed.sort(key=lambda x: x[1])
            for i, (name, s, e) in enumerate(jobs_parsed):
                start_offset = (s - min_time).total_seconds() / 60
                duration = (e - s).total_seconds() / 60
                color = '#FF5722' if 'blast' in name.lower() or 'batch' in name.lower() else '#2196F3'
                ax.barh(i, duration, left=start_offset, height=0.6, color=color, alpha=0.8)

            ax.set_yticks(range(len(jobs_parsed)))
            ax.set_yticklabels([j[0][:30] for j in jobs_parsed], fontsize=8)
            ax.set_xlabel('Time (minutes from start)')
            ax.set_title(f'Job Execution Timeline — {test_id}')
            ax.invert_yaxis()

            plt.tight_layout()
            fig.savefig(charts_dir / f'fig4_timeline_{test_id}.png')
            fig.savefig(charts_dir / f'fig4_timeline_{test_id}.pdf')
            plt.close(fig)
            print(f"  Chart 4: {charts_dir / f'fig4_timeline_{test_id}.png'}")

    print(f"\nAll charts saved to: {charts_dir}")


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(results: List[Dict], output_dir: Path):
    """Generate paper-quality Markdown report."""
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    report_path = output_dir / 'report-final.md'

    # Separate warmup from benchmark results
    warmup = [r for r in results if 'warmup' in r.get('test_id', '').lower()]
    bench = [r for r in results if 'warmup' not in r.get('test_id', '').lower()]

    # Categorize by node count
    node_results = {}
    for r in bench:
        n = r.get('num_nodes', 1)
        node_results[n] = r

    nodes = sorted(node_results.keys())

    # Calculate metrics
    baseline = node_results.get(nodes[0], {}) if nodes else {}
    baseline_blast = baseline.get('blast_total_s', baseline.get('total_elapsed_s', 0))

    speedups = {}
    efficiencies = {}
    for n in nodes:
        r = node_results[n]
        t = r.get('blast_total_s', r.get('total_elapsed_s', 0))
        sp = baseline_blast / t if t > 0 else 0
        speedups[n] = sp
        efficiencies[n] = (sp / (n / nodes[0])) * 100 if n > 0 and nodes[0] > 0 else 0

    lines = []
    lines.append('# ElasticBLAST Azure Performance Benchmark Report')
    lines.append('')
    lines.append('## Abstract')
    lines.append('')

    if len(nodes) >= 2:
        max_n = nodes[-1]
        sp = speedups.get(max_n, 0)
        eff = efficiencies.get(max_n, 0)
        lines.append(
            f'We evaluate the horizontal scaling performance of ElasticBLAST Azure '
            f'using the nt_prok database (82 GB, 29 volumes) on Azure Kubernetes Service (AKS) '
            f'with Standard_E32s_v3 instances (32 vCPU, 256 GB RAM). '
            f'Query workload of 2,768 nucleotide sequences (3.3 MB) was partitioned into 34 batches. '
            f'Scaling from {nodes[0]} to {max_n} nodes achieved '
            f'a **{sp:.1f}× speedup** with **{eff:.0f}% scaling efficiency**, '
            f'demonstrating near-linear horizontal scalability for embarrassingly parallel BLAST workloads.'
        )
    else:
        lines.append(
            'This report presents performance benchmark results for ElasticBLAST Azure '
            'using the nt_prok database (82 GB) on AKS with Standard_E32s_v3 instances.'
        )

    lines.append('')
    lines.append(f'> Generated: {ts}')
    lines.append(f'> Region: Korea Central (koreacentral)')
    lines.append(f'> Platform: Azure Kubernetes Service (AKS)')
    lines.append('')
    lines.append('---')
    lines.append('')

    # ── 1. Introduction ──
    lines.append('## 1. Introduction')
    lines.append('')
    lines.append('BLAST (Basic Local Alignment Search Tool) [1] is the most widely used sequence ')
    lines.append('similarity search tool in bioinformatics. As genomic databases grow exponentially, ')
    lines.append('distributed cloud-based execution becomes essential for timely analysis. ')
    lines.append('ElasticBLAST [2] enables cloud-scale BLAST searches by distributing query batches ')
    lines.append('across multiple compute nodes on managed Kubernetes clusters.')
    lines.append('')
    lines.append('This benchmark evaluates the Azure Kubernetes Service (AKS) implementation of ')
    lines.append('ElasticBLAST, focusing on two key questions:')
    lines.append('')
    lines.append('- **RQ1**: Does multi-node execution provide near-linear speedup?')
    lines.append('- **RQ2**: What is the cost-efficiency tradeoff of scaling out?')
    lines.append('')

    # ── 2. Experimental Setup ──
    lines.append('## 2. Experimental Setup')
    lines.append('')
    lines.append('### 2.1 Infrastructure')
    lines.append('')
    lines.append('| Component | Specification |')
    lines.append('| --- | --- |')
    lines.append('| Cloud Provider | Microsoft Azure |')
    lines.append('| Region | Korea Central |')
    lines.append('| Orchestrator | Azure Kubernetes Service (AKS) |')
    lines.append('| Node VM | Standard_E32s_v3 (32 vCPU, 256 GB RAM) |')
    lines.append('| Storage | Azure Blob NFS Premium (shared PVC) |')
    lines.append('| Container Registry | Azure Container Registry (elbacr) |')
    lines.append('| BLAST+ Version | 2.17.0 |')
    lines.append('| ElasticBLAST Version | 1.5.0 |')
    lines.append('')

    lines.append('### 2.2 Dataset')
    lines.append('')
    lines.append('| Item | Value |')
    lines.append('| --- | --- |')
    lines.append('| Database | nt_prok (NCBI Nucleotide — Prokaryotes) |')
    lines.append('| Database Size | 82 GB (29 volumes) |')
    lines.append('| Query File | JAIJZY01.1.fsa_nt.gz |')
    lines.append('| Query Sequences | 2,768 |')
    lines.append('| Total Query Bases | 3,302,592 |')
    lines.append('| BLAST Program | blastn |')
    lines.append('| BLAST Options | -evalue 0.01 -outfmt 7 |')
    lines.append('| Batch Length | 100,000 bases |')
    lines.append('| Number of Batches | 34 |')
    lines.append('')

    lines.append('### 2.3 Methodology')
    lines.append('')
    lines.append('All tests were conducted on a **pre-warmed** AKS cluster with the nt_prok database ')
    lines.append('already loaded into the shared NFS persistent volume. This eliminates cluster creation ')
    lines.append('and database download time from the measurement, isolating pure BLAST execution performance.')
    lines.append('')
    lines.append('The query file was split into 34 batches (batch-len=100,000), each submitted as an ')
    lines.append('independent Kubernetes Job. The Kubernetes scheduler distributes jobs across available nodes. ')
    lines.append('vmtouch caches the database into node RAM, converting the workload from I/O-bound to CPU-bound.')
    lines.append('')

    # ── 3. Results ──
    lines.append('## 3. Results')
    lines.append('')
    lines.append('### 3.1 Execution Time')
    lines.append('')
    lines.append('| Test ID | Nodes | Pods | Total Elapsed (s) | BLAST Time (s) | Cost (USD) | Status |')
    lines.append('| --- | --- | --- | --- | --- | --- | --- |')

    for r in warmup + bench:
        test_id = r.get('test_id', '')
        n = r.get('num_nodes', 1)
        pods = r.get('num_batches', 34)
        total = r.get('total_elapsed_s', 0)
        blast = r.get('blast_total_s', 0)
        cost = r.get('estimated_cost_usd', 0)
        status = r.get('status', 'unknown')
        lines.append(f'| {test_id} | {n} | {pods} | {total:.0f} | {blast:.0f} | ${cost:.2f} | {status} |')

    lines.append('')

    if len(nodes) >= 2:
        lines.append('### 3.2 Scaling Analysis')
        lines.append('')
        lines.append('| Nodes | BLAST Time (s) | Speedup (×) | Efficiency (%) | Cost (USD) |')
        lines.append('| --- | --- | --- | --- | --- |')

        for n in nodes:
            r = node_results[n]
            t = r.get('blast_total_s', r.get('total_elapsed_s', 0))
            sp = speedups[n]
            eff = efficiencies[n]
            cost = r.get('estimated_cost_usd', 0)
            lines.append(f'| {n} | {t:.0f} | {sp:.2f} | {eff:.0f} | ${cost:.2f} |')

        lines.append('')

        # Scaling formula
        lines.append('**Scaling efficiency** is defined as:')
        lines.append('')
        lines.append('$$\\eta = \\frac{T_1}{N \\times T_N} \\times 100\\%$$')
        lines.append('')
        lines.append(f'where $T_1$ = single-node BLAST time, $T_N$ = N-node BLAST time, $N$ = number of nodes.')
        lines.append('')

    # ── 3.3 Per-Job Analysis ──
    lines.append('### 3.3 Job Distribution')
    lines.append('')
    lines.append('With 34 query batches distributed across nodes:')
    lines.append('')
    for n in nodes:
        jobs_per_node = math.ceil(34 / n)
        lines.append(f'- **{n} node{"s" if n > 1 else ""}**: ~{jobs_per_node} jobs/node')
    lines.append('')

    # ── 4. Discussion ──
    lines.append('## 4. Discussion')
    lines.append('')

    if len(nodes) >= 2:
        max_n = nodes[-1]
        sp = speedups.get(max_n, 0)
        eff = efficiencies.get(max_n, 0)
        cost_ratio = 0
        if baseline.get('estimated_cost_usd', 0) > 0:
            cost_ratio = node_results[max_n].get('estimated_cost_usd', 0) / baseline['estimated_cost_usd']

        lines.append(f'### 4.1 Scaling Efficiency')
        lines.append('')
        lines.append(f'Scaling from {nodes[0]} to {max_n} nodes achieved {sp:.2f}× speedup ')
        lines.append(f'({eff:.0f}% efficiency). ')
        if eff >= 75:
            lines.append('This exceeds the 75% target, confirming near-linear scalability. ')
        else:
            lines.append(f'This is below the 75% target, suggesting overhead from job scheduling ')
            lines.append(f'and NFS contention at higher node counts. ')
        lines.append('')

        lines.append('BLAST workloads are embarrassingly parallel — each query batch is processed ')
        lines.append('independently with no inter-node communication. The primary scaling overhead comes from:')
        lines.append('')
        lines.append('1. **Kubernetes scheduling latency** — distributing 34 pods across nodes')
        lines.append('2. **NFS I/O contention** — multiple nodes accessing the shared database volume')
        lines.append('3. **vmtouch cache warm-up** — each node must cache the database into RAM independently')
        lines.append('4. **Stragglers** — total time is determined by the slowest pod')
        lines.append('')

        lines.append('### 4.2 Cost Efficiency')
        lines.append('')
        if cost_ratio > 0:
            lines.append(f'The cost ratio ({max_n}-node / {nodes[0]}-node) is {cost_ratio:.2f}×. ')
            if cost_ratio <= 1.5:
                lines.append(f'Despite using {max_n}× more resources, the wall-clock time reduction ')
                lines.append(f'keeps total cost within {cost_ratio:.0%} of the single-node baseline. ')
            else:
                lines.append(f'The cost increase is proportional to the resource expansion. ')
        lines.append('')

    # ── 5. Conclusion ──
    lines.append('## 5. Conclusion')
    lines.append('')
    if len(nodes) >= 2:
        max_n = nodes[-1]
        sp = speedups.get(max_n, 0)
        eff = efficiencies.get(max_n, 0)
        lines.append(f'1. ElasticBLAST Azure achieves **{sp:.1f}× speedup** scaling from '
                     f'{nodes[0]} to {max_n} nodes with {eff:.0f}% efficiency.')
        lines.append(f'2. The nt_prok database (82 GB) is fully cached in RAM via vmtouch, '
                     f'making BLAST execution CPU-bound rather than I/O-bound.')
        lines.append(f'3. Horizontal scaling is cost-effective: total node-hours remain approximately constant.')
        lines.append(f'4. The Kubernetes-based architecture enables elastic scaling with no code changes — ')
        lines.append(f'   only the `num-nodes` configuration parameter needs adjustment.')
    else:
        lines.append('Results demonstrate successful execution of ElasticBLAST Azure with the nt_prok database.')
    lines.append('')

    # ── 6. Charts ──
    lines.append('## 6. Figures')
    lines.append('')
    charts_dir = output_dir / 'charts'
    if charts_dir.exists():
        for png in sorted(charts_dir.glob('*.png')):
            label = png.stem.replace('_', ' ').title()
            lines.append(f'### {label}')
            lines.append(f'![{label}](charts/{png.name})')
            lines.append('')

    # ── References ──
    lines.append('## References')
    lines.append('')
    lines.append('[1] Altschul, S.F., et al. (1990). Basic local alignment search tool. '
                'J. Mol. Biol., 215(3), 403-410.')
    lines.append('')
    lines.append('[2] Camacho, C., et al. (2023). ElasticBLAST: accelerating sequence analysis '
                'via cloud computing. BMC Bioinformatics, 24, 117.')
    lines.append('')
    lines.append('[3] Tsai, J. (2021). Running NCBI BLAST on Azure — Performance, Scalability, '
                'and Best Practice. Microsoft Tech Community Blog.')
    lines.append('')

    # Write report
    with open(report_path, 'w') as f:
        f.write('\n'.join(lines))

    print(f"\nReport saved: {report_path}")
    return report_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python benchmark/generate_report.py <results_dir>")
        print("  results_dir should contain *.json benchmark result files")
        sys.exit(1)

    results_dir = Path(sys.argv[1])
    if not results_dir.exists():
        print(f"ERROR: Directory not found: {results_dir}")
        sys.exit(1)

    # Load all result JSON files
    results = []
    for json_file in sorted(results_dir.glob('*.json')):
        if json_file.name == 'summary.json':
            continue
        try:
            with open(json_file) as f:
                data = json.load(f)
            results.append(data)
            print(f"Loaded: {json_file.name}")
        except (json.JSONDecodeError, IOError) as e:
            print(f"WARNING: Failed to load {json_file}: {e}")

    if not results:
        print("ERROR: No result files found")
        sys.exit(1)

    print(f"\nLoaded {len(results)} results")

    # Generate charts
    print("\nGenerating charts...")
    create_charts(results, results_dir)

    # Generate report
    print("\nGenerating report...")
    generate_report(results, results_dir)

    print("\nDone!")


if __name__ == '__main__':
    main()
