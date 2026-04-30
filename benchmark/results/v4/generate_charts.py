#!/usr/bin/env python3
"""Generate publication-quality charts for v4 SARS-CoV-2 ORF1ab benchmark report.

V4 benchmark: single query (21,290 bp) × 10 shards × 5 BLAST repetitions.
Measures per-phase timing and BLAST execution variance.

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
    'indigo': '#4f46e5',
    'pink': '#db2777',
}

# ═══════════════════════════════════════════════════
# V4 Benchmark Data
# ═══════════════════════════════════════════════════

SHARD_LABELS = [f'S{i:02d}' for i in range(10)]
VOLUMES = [9, 9, 9, 9, 9, 9, 9, 9, 9, 2]
DB_SIZES_GB = [36, 36, 36, 36, 36, 36, 36, 36, 36, 12]

# Phase timings (seconds)
AUTH = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
DB_DOWNLOAD = [187, 191, 192, 188, 194, 192, 199, 228, 193, 57]
QUERY_DOWNLOAD = [3, 2, 3, 2, 2, 2, 3, 2, 2, 2]
UPLOAD = [11, 10, 10, 10, 10, 10, 10, 11, 10, 10]

# BLAST times: 5 runs per shard
BLAST_RUNS = [
    [25, 26, 25, 25, 24],  # S00
    [31, 32, 31, 30, 31],  # S01
    [31, 33, 31, 31, 32],  # S02
    [30, 32, 30, 31, 30],  # S03
    [36, 37, 37, 36, 36],  # S04
    [31, 31, 31, 30, 31],  # S05
    [34, 34, 34, 34, 34],  # S06
    [60, 60, 58, 58, 58],  # S07
    [26, 26, 26, 27, 26],  # S08
    [6, 5, 5, 6, 5],      # S09
]

HITS = [501, 500, 501, 500, 500, 500, 500, 500, 501, 500]

# K8s wall clock (seconds)
K8S_DURATION = [329, 361, 366, 357, 391, 361, 384, 539, 338, 99]

# v3 reference data
V3_BLAST = [36, 45, 39, 37, 45, 44, 34, 39, 34, 8]  # 10 queries, 1 run


def fig1_blast_times_5runs():
    """Fig 1: Per-shard BLAST execution time with 5 repetitions (box + scatter)."""
    fig, ax = plt.subplots(figsize=(12, 6))

    positions = np.arange(len(SHARD_LABELS))
    bp = ax.boxplot(BLAST_RUNS, positions=positions, widths=0.5,
                    patch_artist=True, showmeans=True,
                    meanprops=dict(marker='D', markerfacecolor=COLORS['red'], markersize=5),
                    medianprops=dict(color=COLORS['red'], linewidth=2),
                    flierprops=dict(marker='o', markersize=4))

    for i, patch in enumerate(bp['boxes']):
        color = COLORS['orange'] if i == 9 else (COLORS['purple'] if i == 7 else COLORS['blue'])
        patch.set_facecolor(color)
        patch.set_alpha(0.6)

    # Scatter individual points
    for i, runs in enumerate(BLAST_RUNS):
        jitter = np.random.normal(0, 0.05, len(runs))
        ax.scatter([i + j for j in jitter], runs, color='black', s=15, alpha=0.5, zorder=5)

    # Add mean labels
    for i, runs in enumerate(BLAST_RUNS):
        mean_val = np.mean(runs)
        ax.text(i, max(runs) + 2, f'{mean_val:.1f}s', ha='center', fontsize=8, fontweight='bold')

    ax.set_xticks(positions)
    ax.set_xticklabels([f'{l}\n({v} vols, {s}GB)' for l, v, s in
                        zip(SHARD_LABELS, VOLUMES, DB_SIZES_GB)], fontsize=9)
    ax.set_ylabel('BLAST Time (seconds)')
    ax.set_title('Per-Shard BLAST Execution Time (5 Repetitions)\nSARS-CoV-2 ORF1ab (21,290 bp) vs core_nt 10-shard')
    ax.set_ylim(0, 70)

    # Add CV annotation
    for i, runs in enumerate(BLAST_RUNS):
        mean_val = np.mean(runs)
        std_val = np.std(runs)
        cv = std_val / mean_val * 100 if mean_val > 0 else 0
        ax.text(i, -4, f'CV={cv:.1f}%', ha='center', fontsize=7, color=COLORS['gray'])

    fig.savefig(os.path.join(OUT_DIR, 'fig1-blast-5runs.png'))
    plt.close(fig)
    print('  fig1-blast-5runs.png')


def fig2_phase_breakdown():
    """Fig 2: Per-shard phase breakdown (stacked bar)."""
    fig, ax = plt.subplots(figsize=(12, 6))

    blast_avg = [np.mean(runs) for runs in BLAST_RUNS]

    b1 = ax.bar(SHARD_LABELS, DB_DOWNLOAD, color=COLORS['teal'], label='DB Download', edgecolor='white')
    b2 = ax.bar(SHARD_LABELS, blast_avg, bottom=DB_DOWNLOAD, color=COLORS['blue'],
                label='BLAST (avg of 5)', edgecolor='white')
    b3 = ax.bar(SHARD_LABELS, QUERY_DOWNLOAD, bottom=[d + b for d, b in zip(DB_DOWNLOAD, blast_avg)],
                color=COLORS['amber'], label='Query Download', edgecolor='white')
    b4 = ax.bar(SHARD_LABELS, UPLOAD, bottom=[d + b + q for d, b, q in zip(DB_DOWNLOAD, blast_avg, QUERY_DOWNLOAD)],
                color=COLORS['purple'], label='Upload', edgecolor='white')

    totals = [d + b + q + u for d, b, q, u in zip(DB_DOWNLOAD, blast_avg, QUERY_DOWNLOAD, UPLOAD)]
    for i, t in enumerate(totals):
        ax.text(i, t + 3, f'{t:.0f}s', ha='center', fontsize=8, fontweight='bold')

    # Percentage labels for DB download
    for i, (d, t) in enumerate(zip(DB_DOWNLOAD, totals)):
        pct = d / t * 100
        ax.text(i, d / 2, f'{pct:.0f}%', ha='center', va='center', fontsize=8, color='white', fontweight='bold')

    ax.set_xlabel('Shard')
    ax.set_ylabel('Time (seconds)')
    ax.set_title('Per-Shard Execution Time Breakdown\nSARS-CoV-2 ORF1ab × 10-shard (BLAST avg of 5 runs)')
    ax.legend(loc='upper right')
    ax.set_ylim(0, 360)

    fig.savefig(os.path.join(OUT_DIR, 'fig2-phase-breakdown.png'))
    plt.close(fig)
    print('  fig2-phase-breakdown.png')


def fig3_timeline():
    """Fig 3: Parallel execution timeline — Gantt chart with all phases."""
    fig, ax = plt.subplots(figsize=(14, 6))

    blast_avg = [np.mean(runs) for runs in BLAST_RUNS]

    for i in range(10):
        x = 0
        # Download phase
        ax.barh(i, DB_DOWNLOAD[i], left=x, height=0.6, color=COLORS['teal'], alpha=0.85)
        x += DB_DOWNLOAD[i]
        # Query download
        ax.barh(i, QUERY_DOWNLOAD[i], left=x, height=0.6, color=COLORS['amber'])
        x += QUERY_DOWNLOAD[i]
        # BLAST (avg)
        ax.barh(i, blast_avg[i] * 5, left=x, height=0.6, color=COLORS['blue'])
        # Mark individual runs
        for r in range(5):
            run_start = x + sum(BLAST_RUNS[i][:r])
            ax.barh(i, BLAST_RUNS[i][r], left=run_start, height=0.6,
                    color=COLORS['blue'], alpha=0.6 + 0.08 * r, edgecolor='white', linewidth=0.3)
        x += sum(BLAST_RUNS[i])
        # Upload
        ax.barh(i, UPLOAD[i], left=x, height=0.6, color=COLORS['purple'])
        x += UPLOAD[i]
        # Total label
        ax.text(x + 5, i, f'{K8S_DURATION[i]}s ({K8S_DURATION[i]/60:.1f}m)', va='center', fontsize=8)

    # Max wall clock line
    max_dur = max(K8S_DURATION)
    ax.axvline(x=max_dur, color=COLORS['red'], linestyle='--', linewidth=1.5)
    ax.text(max_dur + 5, -0.7, f'Wall clock: {max_dur}s ({max_dur/60:.1f} min)',
            color=COLORS['red'], fontsize=10, fontweight='bold')

    ax.set_yticks(range(10))
    ax.set_yticklabels([f'S{i:02d} ({VOLUMES[i]} vols)' for i in range(10)], fontsize=9)
    ax.set_xlabel('Time (seconds)')
    ax.set_title('Parallel Execution Timeline: V4 SARS-CoV-2 ORF1ab\n(all shards run simultaneously, 5 BLAST runs each)')
    ax.invert_yaxis()
    ax.set_xlim(0, max_dur + 80)

    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLORS['teal'], alpha=0.85, label='DB Download'),
        Patch(facecolor=COLORS['amber'], label='Query Download'),
        Patch(facecolor=COLORS['blue'], label='BLAST × 5 runs'),
        Patch(facecolor=COLORS['purple'], label='Upload'),
    ]
    ax.legend(handles=legend_elements, loc='lower right')

    fig.savefig(os.path.join(OUT_DIR, 'fig3-timeline.png'))
    plt.close(fig)
    print('  fig3-timeline.png')


def fig4_blast_variance():
    """Fig 4: BLAST execution variance analysis — per-shard CV and run-to-run stability."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Left: per-run averages across shards
    run_avgs = []
    run_stds = []
    for r in range(5):
        run_times = [BLAST_RUNS[s][r] for s in range(10)]
        run_avgs.append(np.mean(run_times))
        run_stds.append(np.std(run_times))

    runs_x = [f'Run {r+1}' for r in range(5)]
    bars1 = ax1.bar(runs_x, run_avgs, yerr=run_stds, color=COLORS['blue'],
                    edgecolor='white', capsize=5, error_kw={'linewidth': 1.5})
    for bar, avg in zip(bars1, run_avgs):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                f'{avg:.1f}s', ha='center', fontsize=10, fontweight='bold')
    ax1.set_ylabel('Average BLAST Time (seconds)')
    ax1.set_title('Cross-Shard Average per Run\n(error bars = 1 stddev)')
    ax1.set_ylim(0, 50)

    # Right: per-shard CV (coefficient of variation)
    cvs = []
    means = []
    for i, runs in enumerate(BLAST_RUNS):
        mean_val = np.mean(runs)
        std_val = np.std(runs)
        cv = std_val / mean_val * 100 if mean_val > 0 else 0
        cvs.append(cv)
        means.append(mean_val)

    colors_cv = [COLORS['green'] if cv < 5 else COLORS['amber'] for cv in cvs]
    bars2 = ax2.bar(SHARD_LABELS, cvs, color=colors_cv, edgecolor='white')
    for bar, cv in zip(bars2, cvs):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                f'{cv:.1f}%', ha='center', fontsize=9, fontweight='bold')

    ax2.axhline(y=5, color=COLORS['red'], linestyle='--', linewidth=1, alpha=0.5)
    ax2.text(9.5, 5.3, 'CV=5% threshold', ha='right', color=COLORS['red'], fontsize=8)
    ax2.set_ylabel('Coefficient of Variation (%)')
    ax2.set_title('Per-Shard BLAST Time Variability\n(CV < 5% = highly stable)')
    ax2.set_ylim(0, 10)

    fig.suptitle('V4 BLAST Execution Variance Analysis (5 Repetitions)', fontsize=14, fontweight='bold')
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(os.path.join(OUT_DIR, 'fig4-blast-variance.png'))
    plt.close(fig)
    print('  fig4-blast-variance.png')


def fig5_v3_vs_v4_comparison():
    """Fig 5: V3 vs V4 BLAST time comparison — 10 queries vs 1 query."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    blast_v4_avg = [np.mean(runs) for runs in BLAST_RUNS]

    # Left: side-by-side bars per shard
    x = np.arange(10)
    w = 0.35
    bars1 = ax1.bar(x - w/2, V3_BLAST, w, label='v3: 10 queries (37 KB)', color=COLORS['blue'], edgecolor='white')
    bars2 = ax1.bar(x + w/2, blast_v4_avg, w, label='v4: 1 query (21 KB)', color=COLORS['green'], edgecolor='white')

    for bar, t in zip(bars1, V3_BLAST):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, f'{t}', ha='center', fontsize=7)
    for bar, t in zip(bars2, blast_v4_avg):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, f'{t:.0f}', ha='center', fontsize=7)

    ax1.set_xticks(x)
    ax1.set_xticklabels(SHARD_LABELS)
    ax1.set_ylabel('BLAST Time (seconds)')
    ax1.set_title('Per-Shard BLAST: v3 vs v4')
    ax1.legend(loc='upper left')
    ax1.set_ylim(0, 70)

    # Right: ratio (v3/v4)
    ratios = [v3 / v4 if v4 > 0 else 0 for v3, v4 in zip(V3_BLAST, blast_v4_avg)]
    colors_ratio = [COLORS['green'] if r >= 1 else COLORS['red'] for r in ratios]
    bars3 = ax2.bar(SHARD_LABELS, ratios, color=colors_ratio, edgecolor='white')
    for bar, r in zip(bars3, ratios):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.03,
                f'{r:.2f}x', ha='center', fontsize=9, fontweight='bold')
    ax2.axhline(y=1, color='gray', linestyle='-', linewidth=0.5)
    ax2.set_ylabel('Ratio (v3 / v4)')
    ax2.set_title('Speed Ratio: v3(10q) / v4(1q)\n(>1 means v4 is faster)')
    ax2.set_ylim(0, 2)

    fig.suptitle('Query Count Impact: 10 Queries (v3) vs 1 Query (v4)', fontsize=14, fontweight='bold')
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(os.path.join(OUT_DIR, 'fig5-v3-vs-v4.png'))
    plt.close(fig)
    print('  fig5-v3-vs-v4.png')


def fig6_download_vs_blast():
    """Fig 6: Download time dominance — pie chart per shard category."""
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 5))

    def make_pie(ax, shard_idx, title):
        blast_avg = np.mean(BLAST_RUNS[shard_idx]) * 5  # total for 5 runs
        phases = [DB_DOWNLOAD[shard_idx], blast_avg, QUERY_DOWNLOAD[shard_idx], UPLOAD[shard_idx]]
        labels = ['DB Download', 'BLAST × 5', 'Query DL', 'Upload']
        colors = [COLORS['teal'], COLORS['blue'], COLORS['amber'], COLORS['purple']]
        explode = (0.05, 0, 0, 0)
        wedges, texts, autotexts = ax.pie(phases, labels=labels, colors=colors,
                                          autopct='%1.0f%%', startangle=90, explode=explode,
                                          textprops={'fontsize': 9})
        ax.set_title(f'{title}\n(total: {sum(phases):.0f}s)', fontsize=11)

    make_pie(ax1, 0, 'S00 (9 vols, 36 GB)')
    make_pie(ax2, 7, 'S07 (9 vols, 36 GB)')
    make_pie(ax3, 9, 'S09 (2 vols, 12 GB)')

    fig.suptitle('Time Allocation by Phase (representative shards)', fontsize=14, fontweight='bold')
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(os.path.join(OUT_DIR, 'fig6-phase-pies.png'))
    plt.close(fig)
    print('  fig6-phase-pies.png')


def fig7_blast_heatmap():
    """Fig 7: BLAST time heatmap — shards × runs."""
    fig, ax = plt.subplots(figsize=(8, 7))

    data = np.array(BLAST_RUNS)
    im = ax.imshow(data, cmap='YlOrRd', aspect='auto', interpolation='nearest')

    ax.set_xticks(range(5))
    ax.set_xticklabels([f'Run {r+1}' for r in range(5)])
    ax.set_yticks(range(10))
    ax.set_yticklabels([f'S{i:02d} ({VOLUMES[i]}v)' for i in range(10)])

    # Add text annotations
    for i in range(10):
        for j in range(5):
            color = 'white' if data[i, j] > 40 else 'black'
            ax.text(j, i, f'{data[i,j]}s', ha='center', va='center', fontsize=9,
                    color=color, fontweight='bold')

    cbar = plt.colorbar(im, ax=ax, label='BLAST Time (seconds)')
    ax.set_title('BLAST Execution Time Heatmap\n(Shard × Run)')
    ax.set_xlabel('Run')
    ax.set_ylabel('Shard')

    fig.savefig(os.path.join(OUT_DIR, 'fig7-blast-heatmap.png'))
    plt.close(fig)
    print('  fig7-blast-heatmap.png')


def fig8_cost_analysis():
    """Fig 8: Cost analysis — per-run and projected warm."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Left: cold cost breakdown
    configs = ['v3 Full DB\n1×E64s', 'v3 10-shard\n10×E16s', 'v4 10-shard\n10×E16s\n(5 runs)']
    # v3 full DB: 1 * 4.032 * 679/3600 = $0.76
    # v3 shard: 10 * 1.008 * 236/3600 = $0.66
    # v4 shard: 10 * 1.008 * 539/3600 = $1.51 (but per BLAST run = $1.51/5)
    costs_total = [0.76, 0.66, 1.51]
    costs_per_blast = [0.76, 0.66, 0.30]  # v4 amortized over 5 runs
    colors_cost = [COLORS['gray'], COLORS['blue'], COLORS['green']]

    bars1 = ax1.bar(configs, costs_total, color=colors_cost, edgecolor='white', width=0.5)
    for bar, c in zip(bars1, costs_total):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.03,
                f'${c:.2f}', ha='center', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Total Cost (USD)')
    ax1.set_title('Total Run Cost (cold start)')
    ax1.set_ylim(0, 2.0)

    # Right: warm cluster cost (BLAST-only)
    warm_configs = ['v3 Full DB\n1×E64s', 'v4 single\nBLAST run', 'v4 amortized\nper BLAST run']
    warm_costs = [
        1 * 4.032 * 533/3600,     # v3 full: $0.60
        10 * 1.008 * 60/3600,     # v4 single warm: $0.17
        10 * 1.008 * 60/3600 / 5, # v4 per-run: $0.03
    ]
    colors_warm = [COLORS['gray'], COLORS['blue'], COLORS['green']]

    bars2 = ax2.bar(warm_configs, warm_costs, color=colors_warm, edgecolor='white', width=0.5)
    for bar, c in zip(bars2, warm_costs):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                f'${c:.3f}', ha='center', fontsize=10, fontweight='bold')
    ax2.set_ylabel('Cost (USD)')
    ax2.set_title('Projected Warm Cluster Cost\n(DB pre-loaded, BLAST only)')
    ax2.set_ylim(0, 0.8)

    fig.suptitle('V4 Cost Analysis', fontsize=14, fontweight='bold')
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(os.path.join(OUT_DIR, 'fig8-cost-analysis.png'))
    plt.close(fig)
    print('  fig8-cost-analysis.png')


if __name__ == '__main__':
    print('Generating v4 benchmark charts...')
    fig1_blast_times_5runs()
    fig2_phase_breakdown()
    fig3_timeline()
    fig4_blast_variance()
    fig5_v3_vs_v4_comparison()
    fig6_download_vs_blast()
    fig7_blast_heatmap()
    fig8_cost_analysis()
    print(f'\nAll charts saved to {OUT_DIR}/')
