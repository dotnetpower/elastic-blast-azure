#!/usr/bin/env python3
"""Generate publication-quality charts for v3 benchmark report.

Author: Moon Hyuk Choi
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import os

OUT_DIR = os.path.join(os.path.dirname(__file__), 'charts')
os.makedirs(OUT_DIR, exist_ok=True)

# Publication style
plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['DejaVu Sans', 'Arial', 'Helvetica'],
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 12,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'figure.dpi': 150,
    'savefig.dpi': 200,
    'savefig.bbox': 'tight',
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

COLORS = {
    'blue': '#2563eb',
    'red': '#dc2626',
    'green': '#16a34a',
    'orange': '#ea580c',
    'purple': '#9333ea',
    'gray': '#6b7280',
    'teal': '#0d9488',
    'amber': '#d97706',
}


def fig1_shard_blast_times():
    """Fig 1: Per-shard BLAST execution time (bar chart)."""
    shards = [f'S{i:02d}' for i in range(10)]
    blast_times = [38, 32, 39, 40, 33, 40, 31, 35, 35, 6]
    vols = [9, 9, 9, 9, 9, 9, 9, 9, 9, 2]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(shards, blast_times, color=COLORS['blue'], edgecolor='white', linewidth=0.5)
    bars[-1].set_color(COLORS['orange'])  # Shard 09 is smaller

    # Add value labels
    for bar, t, v in zip(bars, blast_times, vols):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{t}s', ha='center', va='bottom', fontsize=9, fontweight='bold')
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()/2,
                f'{v} vols', ha='center', va='center', fontsize=8, color='white')

    # Reference line
    ax.axhline(y=533, color=COLORS['red'], linestyle='--', linewidth=1.5, alpha=0.7)
    ax.text(9.5, 533, 'Full DB (1N): 533s', ha='right', va='bottom',
            color=COLORS['red'], fontsize=9, style='italic')

    ax.set_xlabel('Shard')
    ax.set_ylabel('BLAST Time (seconds)')
    ax.set_title('Per-Shard BLAST Execution Time\n10-shard × E16s_v3, pathogen-10.fa vs core_nt (269 GB)')
    ax.set_ylim(0, 580)

    fig.savefig(os.path.join(OUT_DIR, 'fig1-shard-blast-times.png'))
    plt.close(fig)
    print('  fig1-shard-blast-times.png')


def fig2_shard_download_times():
    """Fig 2: Per-shard download time (stacked bar)."""
    shards = [f'S{i:02d}' for i in range(10)]
    download = [195, 185, 191, 195, 183, 196, 189, 174, 171, 54]
    blast = [38, 32, 39, 40, 33, 40, 31, 35, 35, 6]
    total = [d + b for d, b in zip(download, blast)]

    fig, ax = plt.subplots(figsize=(10, 5))
    b1 = ax.bar(shards, download, color=COLORS['teal'], label='DB Download', edgecolor='white', linewidth=0.5)
    b2 = ax.bar(shards, blast, bottom=download, color=COLORS['blue'], label='BLAST Search', edgecolor='white', linewidth=0.5)

    for i, t in enumerate(total):
        ax.text(i, t + 3, f'{t}s', ha='center', va='bottom', fontsize=8)

    ax.set_xlabel('Shard')
    ax.set_ylabel('Time (seconds)')
    ax.set_title('Per-Shard Total Execution Time Breakdown\n(Download + BLAST)')
    ax.legend(loc='upper right')
    ax.set_ylim(0, 280)

    fig.savefig(os.path.join(OUT_DIR, 'fig2-shard-time-breakdown.png'))
    plt.close(fig)
    print('  fig2-shard-time-breakdown.png')


def fig3_speedup_comparison():
    """Fig 3: Speedup comparison — full DB vs sharded."""
    configs = ['Full DB\n1×E64s\n(reference)', 'Full DB\n2×E64s\n(v2 best)', '10-Shard\n10×E16s\n(v3)']
    blast_times = [533, 533/5.8, 40]  # 533s, ~92s (v2 2N speedup), 40s
    wall_clocks = [533+146, 92+180, 40+196]  # blast + download
    colors = [COLORS['gray'], COLORS['blue'], COLORS['green']]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # BLAST time
    bars1 = ax1.bar(configs, blast_times, color=colors, edgecolor='white', width=0.6)
    for bar, t in zip(bars1, blast_times):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10,
                f'{t:.0f}s\n({t/60:.1f} min)', ha='center', va='bottom', fontsize=9, fontweight='bold')
    ax1.set_ylabel('BLAST Time (seconds)')
    ax1.set_title('BLAST Execution Time')
    ax1.set_ylim(0, 650)

    # Speedup
    speedups = [1.0, 533/blast_times[1], 533/40]
    bars2 = ax2.bar(configs, speedups, color=colors, edgecolor='white', width=0.6)
    for bar, s in zip(bars2, speedups):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                f'{s:.1f}x', ha='center', va='bottom', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Speedup vs Full DB 1N')
    ax2.set_title('Speedup Factor')
    ax2.set_ylim(0, 16)
    ax2.axhline(y=1, color='gray', linestyle='-', linewidth=0.5)

    fig.suptitle('DB Sharding Performance: core_nt (269 GB, 978B bases)', fontsize=14, fontweight='bold')
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(os.path.join(OUT_DIR, 'fig3-speedup-comparison.png'))
    plt.close(fig)
    print('  fig3-speedup-comparison.png')


def fig4_cost_comparison():
    """Fig 4: Cost per run comparison."""
    configs = ['Full DB\n1×E64s', 'Full DB\n2×E64s\n(v2 best)', '10-Shard\n10×E16s']
    # Cost = nodes × $/hr × duration_hr
    costs = [
        1 * 4.032 * (533+146)/3600,   # 1×E64s, ~11.3 min
        2 * 4.032 * (92+180)/3600,     # 2×E64s, ~4.5 min
        10 * 1.008 * (40+196)/3600,    # 10×E16s, ~3.9 min
    ]
    blast_only_costs = [
        1 * 4.032 * 533/3600,
        2 * 4.032 * 92/3600,
        10 * 1.008 * 40/3600,
    ]
    colors = [COLORS['gray'], COLORS['blue'], COLORS['green']]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(configs, costs, color=colors, edgecolor='white', width=0.5)
    for bar, c in zip(bars, costs):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                f'${c:.2f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax.set_ylabel('Cost per Run (USD)')
    ax.set_title('Cost Comparison: Full DB vs Sharded\n(cold start, including DB download)')
    ax.set_ylim(0, max(costs) * 1.3)

    fig.savefig(os.path.join(OUT_DIR, 'fig4-cost-comparison.png'))
    plt.close(fig)
    print('  fig4-cost-comparison.png')


def fig5_data_io():
    """Fig 5: Data I/O per node — full DB vs sharded."""
    configs = ['Full DB\n(each node\ndownloads all)', 'Sharded\n(each node\ndownloads 1/10)']
    per_node_gb = [269, 27 + 7.2]  # full DB vs shard volumes + taxonomy files
    total_gb = [269 * 1, (27 + 7.2) * 10]  # 1 node vs 10 nodes
    download_time = [146, 196]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 5))

    # Per-node data
    bars1 = ax1.bar(configs, per_node_gb, color=[COLORS['red'], COLORS['green']], width=0.5, edgecolor='white')
    for bar, g in zip(bars1, per_node_gb):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{g:.0f} GB', ha='center', va='bottom', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Data Downloaded per Node (GB)')
    ax1.set_title('Per-Node I/O')
    ax1.set_ylim(0, 320)

    # Download time
    bars2 = ax2.bar(configs, download_time, color=[COLORS['red'], COLORS['green']], width=0.5, edgecolor='white')
    for bar, t in zip(bars2, download_time):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
                f'{t}s ({t/60:.1f} min)', ha='center', va='bottom', fontsize=10, fontweight='bold')
    ax2.set_ylabel('Download Time (seconds)')
    ax2.set_title('Download Duration')
    ax2.set_ylim(0, 250)

    fig.suptitle('Data I/O Reduction via Sharding', fontsize=14, fontweight='bold')
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(os.path.join(OUT_DIR, 'fig5-data-io.png'))
    plt.close(fig)
    print('  fig5-data-io.png')


def fig6_correctness():
    """Fig 6: Correctness validation — top-1 hit comparison."""
    queries = [
        'Monkeypox F3L\n(NC_003310)',
        'SARS-CoV-2 RdRP\n(NC_045512:RdRP)',
        'SARS-CoV-2 orf1ab\n(NC_045512:orf1ab)',
        'SARS-CoV-2 N\n(NC_045512:N)',
        'Monkeypox F3L\n(NC_063383)',
        'P.falciparum 18S\n(NC_004325)',
        'P.falciparum 18S\n(NC_004326)',
        'P.falciparum 18S\n(NC_004328)',
        'P.falciparum 18S\n(NC_004331)',
        'P.falciparum 18S\n(NC_037282)',
    ]
    # 1 = exact match, 0.5 = same score different accession
    match_status = [0.5, 0.5, 1.0, 0.5, 0.5, 1.0, 1.0, 1.0, 1.0, 1.0]
    colors_bar = [COLORS['green'] if m == 1.0 else COLORS['amber'] for m in match_status]

    fig, ax = plt.subplots(figsize=(10, 6))
    y_pos = np.arange(len(queries))
    bars = ax.barh(y_pos, match_status, color=colors_bar, edgecolor='white', height=0.6)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(queries, fontsize=9)
    ax.set_xlim(0, 1.2)
    ax.set_xticks([0, 0.5, 1.0])
    ax.set_xticklabels(['No Match', 'Same Score\n(tie-break)', 'Exact Match'])
    ax.set_title('Top-1 Hit Correctness: Sharded vs Full-DB Reference\n(all 10 queries match at score level)')
    ax.invert_yaxis()

    # Add labels
    for bar, m in zip(bars, match_status):
        label = 'EXACT MATCH' if m == 1.0 else 'SAME E-val & BS'
        ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height()/2,
                label, va='center', fontsize=9,
                color=COLORS['green'] if m == 1.0 else COLORS['amber'])

    fig.savefig(os.path.join(OUT_DIR, 'fig6-correctness.png'))
    plt.close(fig)
    print('  fig6-correctness.png')


def fig7_timeline():
    """Fig 7: Parallel execution timeline — all 10 shards."""
    fig, ax = plt.subplots(figsize=(12, 5))

    download = [195, 185, 191, 195, 183, 196, 189, 174, 171, 54]
    blast = [38, 32, 39, 40, 33, 40, 31, 35, 35, 6]

    for i in range(10):
        # Download phase
        ax.barh(i, download[i], left=0, height=0.6, color=COLORS['teal'], alpha=0.8)
        # BLAST phase
        ax.barh(i, blast[i], left=download[i], height=0.6, color=COLORS['blue'])
        # Total label
        total = download[i] + blast[i]
        ax.text(total + 3, i, f'{total}s', va='center', fontsize=8)

    # Max wall clock line
    max_total = max(d + b for d, b in zip(download, blast))
    ax.axvline(x=max_total, color=COLORS['red'], linestyle='--', linewidth=1.5)
    ax.text(max_total + 2, -0.5, f'Wall clock: {max_total}s ({max_total/60:.1f} min)',
            color=COLORS['red'], fontsize=10, fontweight='bold')

    ax.set_yticks(range(10))
    ax.set_yticklabels([f'Shard {i:02d} ({9 if i < 9 else 2} vols)' for i in range(10)], fontsize=9)
    ax.set_xlabel('Time (seconds)')
    ax.set_title('Parallel Execution Timeline: 10-Shard BLAST\n(all shards run simultaneously on separate nodes)')
    ax.invert_yaxis()
    ax.set_xlim(0, max_total + 40)

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLORS['teal'], alpha=0.8, label='DB Download'),
        Patch(facecolor=COLORS['blue'], label='BLAST Search'),
    ]
    ax.legend(handles=legend_elements, loc='lower right')

    fig.savefig(os.path.join(OUT_DIR, 'fig7-timeline.png'))
    plt.close(fig)
    print('  fig7-timeline.png')


def fig8_hit_distribution():
    """Fig 8: Hit distribution across shards."""
    shards = [f'S{i:02d}' for i in range(10)]
    hits = [7065, 6892, 6990, 6977, 6937, 7008, 6810, 7025, 7094, 7578]
    avg = np.mean(hits)

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(shards, hits, color=COLORS['purple'], edgecolor='white', linewidth=0.5)
    bars[-1].set_color(COLORS['orange'])

    ax.axhline(y=avg, color=COLORS['red'], linestyle='--', linewidth=1, alpha=0.7)
    ax.text(9.5, avg + 50, f'Mean: {avg:.0f}', ha='right', color=COLORS['red'], fontsize=9)

    for bar, h in zip(bars, hits):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
                f'{h:,}', ha='center', fontsize=8)

    ax.set_xlabel('Shard')
    ax.set_ylabel('Number of Hits')
    ax.set_title('Hit Distribution Across Shards\n(uniform distribution confirms balanced sharding)')
    ax.set_ylim(6500, 8000)

    fig.savefig(os.path.join(OUT_DIR, 'fig8-hit-distribution.png'))
    plt.close(fig)
    print('  fig8-hit-distribution.png')


if __name__ == '__main__':
    print('Generating v3 benchmark charts...')
    fig1_shard_blast_times()
    fig2_shard_download_times()
    fig3_speedup_comparison()
    fig4_cost_comparison()
    fig5_data_io()
    fig6_correctness()
    fig7_timeline()
    fig8_hit_distribution()
    print(f'\nAll charts saved to {OUT_DIR}/')
