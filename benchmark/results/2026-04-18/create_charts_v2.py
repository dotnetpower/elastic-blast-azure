#!/usr/bin/env python3
"""Generate publication-quality benchmark charts for ElasticBLAST Azure report."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os

OUT_DIR = os.path.join(os.path.dirname(__file__), 'charts-v2')
os.makedirs(OUT_DIR, exist_ok=True)

# Style
plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 12,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'figure.dpi': 150,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

C_SSD = '#2196F3'    # Blue
C_NVME = '#FF9800'   # Orange
C_NFS = '#9E9E9E'    # Gray
C_WARM = '#BDBDBD'   # Light gray

def save(fig, name):
    for ext in ['png', 'pdf']:
        fig.savefig(os.path.join(OUT_DIR, f'{name}.{ext}'))
    plt.close(fig)
    print(f'  Saved {name}')


# ── Fig 1: Storage Backend Comparison (82 GB) ──
fig, ax = plt.subplots(figsize=(7, 4))
backends = ['Local SSD\n(E32s_v3)', 'NVMe\n(L32as_v3)', 'NFS + vmtouch\n(E32s_v3)', 'Blob NFS\n(E32s_v3)']
medians = [679, 1066, 1740, 1916]
colors = [C_SSD, C_NVME, C_WARM, C_NFS]
bars = ax.bar(backends, medians, color=colors, edgecolor='white', linewidth=0.5, width=0.6)
for bar, val in zip(bars, medians):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 30,
            f'{val}s', ha='center', va='bottom', fontweight='bold', fontsize=10)
ax.set_ylabel('Median Per-Job Time (seconds)')
ax.set_title('Fig 1. Storage Backend Performance — nt_prok 82 GB, 1 Node')
ax.set_ylim(0, 2200)
# Add speedup annotations
ax.annotate('2.8× faster', xy=(0, 679), xytext=(1.5, 400),
            fontsize=9, color=C_SSD, fontweight='bold',
            arrowprops=dict(arrowstyle='->', color=C_SSD, lw=1.5))
save(fig, 'fig1-storage-backends')


# ── Fig 2: Multi-Node Scaling (SSD vs NVMe, grouped bar) ──
fig, ax = plt.subplots(figsize=(7, 4.5))
nodes = ['1 Node', '3 Nodes', '5 Nodes']
ssd_vals = [679, 292, 259]
nvme_vals = [1066, 244, None]  # 5N not tested

x = np.arange(len(nodes))
w = 0.35
bars1 = ax.bar(x - w/2, ssd_vals, w, label='Local SSD (E32s_v3)', color=C_SSD, edgecolor='white')
nvme_plot = [v if v is not None else 0 for v in nvme_vals]
bars2 = ax.bar(x + w/2, nvme_plot, w, label='NVMe (L32as_v3)', color=C_NVME, edgecolor='white')

# Labels
for bar, val in zip(bars1, ssd_vals):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 15,
            f'{val}s', ha='center', va='bottom', fontsize=9, color=C_SSD)
for bar, val in zip(bars2, nvme_vals):
    if val is not None:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 15,
                f'{val}s', ha='center', va='bottom', fontsize=9, color=C_NVME)
    else:
        ax.text(bar.get_x() + bar.get_width()/2, 30,
                'N/A\n(quota)', ha='center', va='bottom', fontsize=8, color='#999')

# Scaling annotations
ax.annotate('2.3×', xy=(1 - w/2, 292), xytext=(0.3, 500),
            fontsize=9, color=C_SSD,
            arrowprops=dict(arrowstyle='->', color=C_SSD, lw=1))
ax.annotate('4.4×', xy=(1 + w/2, 244), xytext=(1.7, 500),
            fontsize=9, color=C_NVME, fontweight='bold',
            arrowprops=dict(arrowstyle='->', color=C_NVME, lw=1))

ax.set_ylabel('Median Per-Job Time (seconds)')
ax.set_title('Fig 2. Multi-Node Scaling — nt_prok 82 GB')
ax.set_xticks(x)
ax.set_xticklabels(nodes)
ax.set_ylim(0, 1200)
ax.legend(loc='upper right')
save(fig, 'fig2-scaling-grouped')


# ── Fig 3: Cost vs Performance ──
fig, ax = plt.subplots(figsize=(7, 4.5))
configs = ['SSD 1N', 'SSD 3N', 'SSD 5N', 'NVMe 1N', 'NVMe 3N', 'NFS 1N']
costs = [0.90, 0.70, 1.26, 1.59, 0.84, 2.23]
perfs = [679, 292, 259, 1066, 244, 1916]
colors_scatter = [C_SSD, C_SSD, C_SSD, C_NVME, C_NVME, C_NFS]
markers = ['o', 's', 'D', 'o', 's', '^']

for i, (c, p, col, m, label) in enumerate(zip(costs, perfs, colors_scatter, markers, configs)):
    ax.scatter(c, p, c=col, marker=m, s=120, zorder=5, edgecolors='black', linewidth=0.5)
    offset_x = 0.05 if i != 1 else -0.15
    offset_y = 30 if i != 4 else -60
    ax.annotate(label, (c, p), textcoords="offset points", xytext=(15, offset_y),
                fontsize=9, color=col)

# Highlight best
ax.scatter([0.70], [292], c='none', s=250, edgecolors='green', linewidth=2, zorder=6)
ax.annotate('Best value', xy=(0.70, 292), xytext=(0.3, 100),
            fontsize=9, color='green', fontweight='bold',
            arrowprops=dict(arrowstyle='->', color='green', lw=1.5))

ax.set_xlabel('Estimated Cost per Run (USD)')
ax.set_ylabel('Median Per-Job Time (seconds)')
ax.set_title('Fig 3. Cost-Performance Tradeoff — nt_prok 82 GB')
ax.set_xlim(0, 2.5)
ax.set_ylim(0, 2100)

# Legend
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], marker='o', color='w', markerfacecolor=C_SSD, markersize=10, label='Local SSD'),
    Line2D([0], [0], marker='o', color='w', markerfacecolor=C_NVME, markersize=10, label='NVMe'),
    Line2D([0], [0], marker='^', color='w', markerfacecolor=C_NFS, markersize=10, label='Blob NFS'),
]
ax.legend(handles=legend_elements, loc='upper right')
save(fig, 'fig3-cost-performance')


# ── Fig 4: DB Size Effect on NFS Penalty ──
fig, ax = plt.subplots(figsize=(6, 4))
db_sizes = ['260_part_aa\n(~2 GB)', 'nt_prok\n(82 GB)']
ssd_med = [54, 679]
nfs_med = [64, 1916]

x = np.arange(len(db_sizes))
w = 0.3
bars1 = ax.bar(x - w/2, ssd_med, w, label='Local SSD', color=C_SSD, edgecolor='white')
bars2 = ax.bar(x + w/2, nfs_med, w, label='Blob NFS', color=C_NFS, edgecolor='white')

for bar, val in zip(bars1, ssd_med):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 20,
            f'{val}s', ha='center', va='bottom', fontsize=9, color=C_SSD)
for bar, val in zip(bars2, nfs_med):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 20,
            f'{val}s', ha='center', va='bottom', fontsize=9, color='#555')

# Penalty labels
ax.annotate('1.2× penalty', xy=(0 + w/2, 64), xytext=(0.5, 200),
            fontsize=9, color='#666',
            arrowprops=dict(arrowstyle='->', color='#999', lw=1))
ax.annotate('2.8× penalty', xy=(1 + w/2, 1916), xytext=(0.5, 1600),
            fontsize=9, color='red', fontweight='bold',
            arrowprops=dict(arrowstyle='->', color='red', lw=1.5))

ax.set_ylabel('Median Per-Job Time (seconds)')
ax.set_title('Fig 4. NFS Penalty Scales with Database Size')
ax.set_xticks(x)
ax.set_xticklabels(db_sizes)
ax.set_ylim(0, 2200)
ax.legend()
save(fig, 'fig4-db-size-nfs-penalty')


# ── Fig 5: Pod Tuning (mem-limit) ──
fig, ax = plt.subplots(figsize=(6, 3.5))
configs = ['Default\n(254G)', 'mem-limit\n= 4G']
medians = [54, 44]
colors = ['#BBDEFB', C_SSD]
bars = ax.bar(configs, medians, color=colors, edgecolor='white', width=0.5)
for bar, val in zip(bars, medians):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
            f'{val}s', ha='center', va='bottom', fontweight='bold', fontsize=11)
ax.set_ylabel('Median Per-Job Time (seconds)')
ax.set_title('Fig 5. Pod Tuning: mem-limit Effect — 260_part_aa, 1 Node')
ax.set_ylim(0, 70)
ax.annotate('19% faster', xy=(1, 44), xytext=(0.3, 60),
            fontsize=10, color='green', fontweight='bold',
            arrowprops=dict(arrowstyle='->', color='green', lw=1.5))
save(fig, 'fig5-pod-tuning')


# ── Fig 6: Batch-000 Tail Latency ──
fig, ax = plt.subplots(figsize=(7, 4))
tests = ['SSD 1N', 'SSD 3N', 'SSD 5N', 'NVMe 1N', 'NVMe 3N']
batch000 = [1607, None, 2333, 2296, 2470]  # 3N batch000 unknown (partial data)
median_others = [679, 292, 259, 1066, 244]

x = np.arange(len(tests))
w = 0.3

bars1 = ax.bar(x - w/2, median_others, w, label='Median (other batches)', color=C_SSD, alpha=0.7, edgecolor='white')
b000_plot = [v if v is not None else 0 for v in batch000]
b000_colors = [C_SSD if 'SSD' in t else C_NVME for t in tests]
bars2 = ax.bar(x + w/2, b000_plot, w, label='batch-000 (16S rRNA)', color='#F44336', alpha=0.8, edgecolor='white')

for bar, val in zip(bars2, batch000):
    if val is not None:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 30,
                f'{val}s', ha='center', va='bottom', fontsize=8, color='#C62828')

# Ratio annotations
for i, (b0, med) in enumerate(zip(batch000, median_others)):
    if b0 is not None:
        ratio = b0 / med
        ax.text(i, max(b0, med) + 120, f'{ratio:.1f}×',
                ha='center', fontsize=9, fontweight='bold', color='#C62828')

ax.set_ylabel('Job Duration (seconds)')
ax.set_title('Fig 6. Tail Latency: batch-000 (16S rRNA) vs Other Batches')
ax.set_xticks(x)
ax.set_xticklabels(tests)
ax.set_ylim(0, 2800)
ax.legend(loc='upper left')
save(fig, 'fig6-tail-latency')


print(f'\nAll charts saved to {OUT_DIR}/')
