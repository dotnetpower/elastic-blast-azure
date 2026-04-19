#!/usr/bin/env python3
"""Generate benchmark report and publication-quality charts from results."""

import json
import os
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import rcParams
import numpy as np

# Academic paper style
rcParams.update({
    'font.family': 'serif',
    'font.serif': ['DejaVu Serif', 'Times New Roman', 'serif'],
    'font.size': 11,
    'axes.labelsize': 13,
    'axes.titlesize': 14,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'legend.fontsize': 11,
    'figure.figsize': (8, 5),
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.linestyle': '--',
    'axes.spines.top': False,
    'axes.spines.right': False,
})

RESULTS_DIR = Path('benchmark/results/2026-04-17')
CHARTS_DIR = RESULTS_DIR / 'charts'
CHARTS_DIR.mkdir(exist_ok=True)

# Load results
with open(RESULTS_DIR / 'bench-1node.json') as f:
    r1 = json.load(f)
with open(RESULTS_DIR / 'bench-3node.json') as f:
    r3 = json.load(f)
with open(RESULTS_DIR / 'bench-5node.json') as f:
    r5 = json.load(f)

times_1 = r1['blast_per_job_s']
times_3 = r3['blast_per_job_s']
times_5 = r5['blast_per_job_s']

# ── Figure 1: Scaling Comparison Bar Chart (3 configs) ──
fig, ax = plt.subplots(figsize=(10, 5))

categories = ['1 Node\n(15/34 jobs)', '3 Nodes\n(34/34 jobs)', '5 Nodes\n(34/34 jobs)']
avg_times = [np.mean(times_1), np.mean(times_3), np.mean(times_5)]
max_times = [max(times_1), max(times_3), max(times_5)]

x = np.arange(len(categories))
width = 0.35

bars1 = ax.bar(x - width/2, avg_times, width, label='Average per-job', color='#1976D2', alpha=0.85)
bars2 = ax.bar(x + width/2, max_times, width, label='Wall-clock (slowest job)', color='#E53935', alpha=0.85)

ax.set_ylabel('Time (seconds)')
ax.set_title('Figure 1: BLAST Execution Time — Scaling Comparison\n(nt_prok 82 GB, Standard_E32s_v3, Local SSD)')
ax.set_xticks(x)
ax.set_xticklabels(categories)
ax.legend()

for bar in bars1:
    h = bar.get_height()
    ax.annotate(f'{h:.0f}s', xy=(bar.get_x() + bar.get_width() / 2, h),
               xytext=(0, 5), textcoords="offset points", ha='center', fontsize=11, fontweight='bold')
for bar in bars2:
    h = bar.get_height()
    ax.annotate(f'{h:.0f}s', xy=(bar.get_x() + bar.get_width() / 2, h),
               xytext=(0, 5), textcoords="offset points", ha='center', fontsize=11, fontweight='bold')

# Speedup annotations
ax.annotate('9.1×', xy=(2.3, avg_times[0]/2), fontsize=16, fontweight='bold', color='#1B5E20',
           ha='center', bbox=dict(boxstyle='round,pad=0.3', facecolor='#C8E6C9', alpha=0.8))

plt.tight_layout()
fig.savefig(CHARTS_DIR / 'fig1_scaling_comparison.png')
fig.savefig(CHARTS_DIR / 'fig1_scaling_comparison.pdf')
plt.close(fig)
print(f"  Figure 1: {CHARTS_DIR / 'fig1_scaling_comparison.png'}")

# ── Figure 2: Speedup & Scaling Efficiency ──
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

nodes = [1, 3, 5]
avgs = [np.mean(times_1), np.mean(times_3), np.mean(times_5)]
baseline = avgs[0]
actual_speedup = [baseline / a for a in avgs]
ideal_speedup = [n for n in nodes]
efficiency = [(s / n) * 100 for s, n in zip(actual_speedup, nodes)]

# Speedup
ax1.plot(nodes, ideal_speedup, 'k--', label='Ideal (linear)', linewidth=1.5, zorder=3)
ax1.plot(nodes, actual_speedup, 'ro-', label='Actual', linewidth=2, markersize=10, zorder=4)
ax1.fill_between(nodes, ideal_speedup, actual_speedup, alpha=0.15, color='green',
                 label='Super-linear gain')
ax1.set_xlabel('Number of Nodes')
ax1.set_ylabel('Speedup (×)')
ax1.set_title('(a) Speedup vs. Node Count')
ax1.legend(loc='upper left')
ax1.set_xticks(nodes)
ax1.set_ylim(0, 12)
for i, (n, s) in enumerate(zip(nodes, actual_speedup)):
    ax1.annotate(f'{s:.1f}×', (n, s), textcoords="offset points",
                xytext=(10, -5), fontsize=11, fontweight='bold', color='#D32F2F')

# Efficiency
colors = ['#E53935', '#1976D2', '#388E3C']
bars = ax2.bar(range(len(nodes)), efficiency, 0.5, color=colors, alpha=0.85)
ax2.axhline(y=100, color='black', linestyle='--', linewidth=1, alpha=0.5, label='Linear (100%)')
ax2.set_xlabel('Number of Nodes')
ax2.set_ylabel('Scaling Efficiency (%)')
ax2.set_title('(b) Scaling Efficiency (super-linear = >100%)')
ax2.set_xticks(range(len(nodes)))
ax2.set_xticklabels([str(n) for n in nodes])
ax2.set_ylim(0, 250)
ax2.legend()
for bar, eff in zip(bars, efficiency):
    ax2.annotate(f'{eff:.0f}%', xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                xytext=(0, 5), textcoords="offset points", ha='center', fontsize=12, fontweight='bold')

plt.suptitle('Figure 2: Horizontal Scaling Analysis', fontsize=14, y=1.02)
plt.tight_layout()
fig.savefig(CHARTS_DIR / 'fig2_scaling_analysis.png')
fig.savefig(CHARTS_DIR / 'fig2_scaling_analysis.pdf')
plt.close(fig)
print(f"  Figure 2: {CHARTS_DIR / 'fig2_scaling_analysis.png'}")

# ── Figure 3: Per-Job Time Distribution (box plot style) ──
fig, ax = plt.subplots(figsize=(10, 5))

data = [times_1, times_3, times_5]
labels = [f'1 Node\n(n={len(times_1)})', f'3 Nodes\n(n={len(times_3)})', f'5 Nodes\n(n={len(times_5)})']
colors = ['#E53935', '#1976D2', '#388E3C']

bp = ax.boxplot(data, labels=labels, patch_artist=True, widths=0.5,
               medianprops=dict(color='black', linewidth=2))
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)

# Overlay individual points
for i, (d, c) in enumerate(zip(data, colors)):
    jitter = np.random.normal(0, 0.04, len(d))
    ax.scatter([i + 1 + j for j in jitter], d, c=c, alpha=0.5, s=20, zorder=5)

ax.set_ylabel('BLAST Execution Time per Job (seconds)')
ax.set_title('Figure 3: Per-Job Time Distribution by Node Count\n(nt_prok 82 GB, batch-len=100K)')
ax.set_ylim(0, 250)

plt.tight_layout()
fig.savefig(CHARTS_DIR / 'fig3_distribution.png')
fig.savefig(CHARTS_DIR / 'fig3_distribution.pdf')
plt.close(fig)
print(f"  Figure 3: {CHARTS_DIR / 'fig3_distribution.png'}")

# ── Figure 4: Pipeline Phase Breakdown (stacked bar) ──
fig, ax = plt.subplots(figsize=(10, 5))

configs = ['1 Node', '3 Nodes', '5 Nodes']
cluster_create = [451, 271, 240]
db_download = [275, 285, 280]
job_submit = [120, 60, 60]
blast_exec = [216, 39, 27]

x = np.arange(len(configs))
width = 0.5

b1 = ax.bar(x, cluster_create, width, label='Cluster Create', color='#78909C', alpha=0.85)
b2 = ax.bar(x, db_download, width, bottom=cluster_create, label='DB Download (82 GB)', color='#FFA726', alpha=0.85)
b3 = ax.bar(x, job_submit, width, bottom=[c+d for c,d in zip(cluster_create, db_download)],
           label='Job Submit', color='#42A5F5', alpha=0.85)
b4 = ax.bar(x, blast_exec, width, bottom=[c+d+j for c,d,j in zip(cluster_create, db_download, job_submit)],
           label='BLAST Execution', color='#EF5350', alpha=0.85)

ax.set_ylabel('Time (seconds)')
ax.set_title('Figure 4: End-to-End Pipeline Breakdown')
ax.set_xticks(x)
ax.set_xticklabels(configs)
ax.legend(loc='upper right')

# Total time labels
for i, (c, d, j, b) in enumerate(zip(cluster_create, db_download, job_submit, blast_exec)):
    total = c + d + j + b
    ax.text(i, total + 10, f'{total}s\n({total/60:.1f}m)', ha='center', fontsize=10, fontweight='bold')
    ax.text(i, c + d + j + b/2, f'{b}s', ha='center', fontsize=9, color='white', fontweight='bold')

plt.tight_layout()
fig.savefig(CHARTS_DIR / 'fig4_pipeline_breakdown.png')
fig.savefig(CHARTS_DIR / 'fig4_pipeline_breakdown.pdf')
plt.close(fig)
print(f"  Figure 4: {CHARTS_DIR / 'fig4_pipeline_breakdown.png'}")

# ── Figure 5: Cost-Performance Scatter ──
fig, ax = plt.subplots(figsize=(8, 5))

configs_data = [
    ('1 Node\n(partial)', r1['estimated_cost_usd'], max(times_1)/60, '#E53935', 15),
    ('3 Nodes', r3['estimated_cost_usd'], max(times_3)/60, '#1976D2', 34),
    ('5 Nodes', r5['estimated_cost_usd'], max(times_5)/60, '#388E3C', 34),
]

for label, cost, time_min, color, jobs in configs_data:
    ax.scatter(cost, time_min, s=jobs*15, c=color, alpha=0.85, zorder=5, edgecolors='black', linewidth=0.5)
    ax.annotate(label, (cost, time_min), textcoords="offset points",
               xytext=(15, 5), fontsize=11, fontweight='bold')

ax.set_xlabel('Estimated Cost (USD)')
ax.set_ylabel('BLAST Wall-Clock Time (minutes)')
ax.set_title('Figure 5: Cost-Performance Tradeoff')
ax.set_xlim(0, 2.5)
ax.set_ylim(0, 5)
ax.axhline(y=1, color='gray', linestyle=':', alpha=0.3)

plt.tight_layout()
fig.savefig(CHARTS_DIR / 'fig5_cost_performance.png')
fig.savefig(CHARTS_DIR / 'fig5_cost_performance.pdf')
plt.close(fig)
print(f"  Figure 5: {CHARTS_DIR / 'fig5_cost_performance.png'}")

print(f"\nAll charts saved to: {CHARTS_DIR}")
print("Done!")
