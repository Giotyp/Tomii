#!/usr/bin/env python3
"""
wavefront-comp.py
=================
Extended wavefront comparison: adds TBB flow_graph block-DAG to the
figure in fig-wavefront-comparison.tex, to show that TBB *can* express
block-DAG via flow_graph but with API limitations (no core pinning,
worker count controlled via global_control rather than task_arena).

Data sources
------------
- Tomii, Taskflow, TBB parallel_for, Timely: hardcoded from the
  paper-validated coordinates in fig-wavefront-comparison.tex.
- TBB flow_graph: read live from
  ../../benchmarks/results/csvs/tbb_flow_wavefront_n{N}_w{W}.csv
  (system == 'tbb_flow', i.e. global_control run, not the earlier
   tbb_flow_pinned run which used task_arena and didn't scale).

Output
------
  paper/figs/wavefront-comp.png
"""

import os, csv, glob
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ── paper-validated coordinates (fig-wavefront-comparison.tex) ──────────────

WORKERS = [1, 2, 4, 8, 16, 32]
NS      = [64, 128, 256, 512]

TOMII = {
    64:  [0.0210, 0.0300, 0.0330, 0.0320, 0.0380, 0.0460],
    128: [0.0630, 0.0570, 0.0600, 0.0650, 0.0710, 0.0860],
    256: [0.2350, 0.1520, 0.1150, 0.1360, 0.1420, 0.1810],
    512: [0.8130, 0.5310, 0.3300, 0.2540, 0.2950, 0.3740],
}
TASKFLOW = {
    64:  [0.0220, 0.0264, 0.0302, 0.0290, 0.0301, 0.0304],
    128: [0.0612, 0.0785, 0.0658, 0.0649, 0.0803, 0.0759],
    256: [0.2162, 0.2507, 0.1747, 0.1696, 0.1674, 0.1728],
    512: [0.8158, 0.7018, 0.3763, 0.3142, 0.3977, 0.3819],
}
TBB_PFOR = {
    64:  [0.0547, 0.1377, 0.2730, 0.3490, 0.5414, 0.8872],
    128: [0.1228, 0.4087, 0.5967, 0.6566, 1.1643, 1.8454],
    256: [0.3039, 0.9993, 1.2107, 1.5807, 2.5293, 3.9333],
    512: [0.9027, 2.7516, 2.7885, 3.3404, 5.0424, 7.4402],
}
TIMELY = {
    64:  [0.0230, 0.3403, 0.4243, 0.7520, 4.9667, 10.4457],
    128: [0.0580, 0.7300, 0.8253, 1.4077, 9.5937, 20.8583],
    256: [0.1587, 1.6103, 1.8613, 2.8793, 18.6217, 42.5967],
    512: [0.8767, 3.6723, 4.1787, 5.9003, 33.3600, 86.6413],
}

# ── load TBB flow_graph results from CSVs ───────────────────────────────────

def load_flow_graph():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_dir    = os.path.normpath(os.path.join(
        script_dir, '..', '..', 'benchmarks', 'results', 'csvs'))
    data = {}
    for n in NS:
        row_vals = []
        for w in WORKERS:
            path = os.path.join(csv_dir, f'tbb_flow_wavefront_n{n}_w{w}.csv')
            val  = None
            if os.path.exists(path):
                with open(path) as f:
                    for row in csv.DictReader(f):
                        # Prefer the global_control run (system == 'tbb_flow'),
                        # not the earlier task_arena run ('tbb_flow_pinned').
                        if row['system'] == 'tbb_flow':
                            val = float(row['s_per_iter']) * 1000  # → ms
            row_vals.append(val)
        data[n] = row_vals
    return data

# ── series definitions ───────────────────────────────────────────────────────
# (label, data_dict_or_None, color, linestyle, marker, linewidth, zorder)
# flow_graph gets a dashed line — visually signals the methodology caveat.

def build_series(flow_graph):
    return [
        ('Tomii block-DAG, tile=32',
         TOMII,      '#000000', '-',  'o', 1.4, 6),
        ('Taskflow block-DAG, tile=32',
         TASKFLOW,   '#444444', '-',  's', 1.4, 5),
        ('TBB flow_graph block-DAG, tile=32†',
         flow_graph, '#666666', '--', 'D', 1.4, 4),
        ('TBB parallel_for, anti-diag (pinned)',
         TBB_PFOR,   '#999999', '-',  '^', 1.4, 3),
        ('Timely, anti-diag (pinned)',
         TIMELY,     '#bbbbbb', '-',  'v', 1.4, 2),
    ]

# ── figure ───────────────────────────────────────────────────────────────────

def make_figure(flow_graph):
    plt.rcParams.update({
        'font.family':      'serif',
        'font.size':         9,
        'axes.titlesize':    9,
        'axes.labelsize':    8,
        'legend.fontsize':   12,
        'xtick.labelsize':   7.5,
        'ytick.labelsize':   7.5,
        'axes.linewidth':    0.7,
        'grid.linewidth':    0.4,
        'grid.alpha':        0.35,
    })

    series = build_series(flow_graph)
    fig, axes = plt.subplots(2, 2, figsize=(7.5, 5.4),
                             gridspec_kw={'hspace': 0.38, 'wspace': 0.28})
    ax_list = [axes[0, 0], axes[0, 1], axes[1, 0], axes[1, 1]]

    for panel_idx, (ax, n) in enumerate(zip(ax_list, NS)):
        for label, data_dict, color, ls, marker, lw, zo in series:
            yvals = data_dict.get(n, [None] * len(WORKERS))
            pairs = [(w, y) for w, y in zip(WORKERS, yvals) if y is not None]
            if not pairs:
                continue
            wx, wy = zip(*pairs)
            kw = dict(color=color, linestyle=ls, marker=marker,
                      linewidth=lw, markersize=4, zorder=zo,
                      label=label if panel_idx == 0 else '_nolegend_')
            ax.plot(wx, wy, **kw)

        ax.set_yscale('log')
        ax.set_xscale('log', base=2)
        ax.set_xticks(WORKERS)
        ax.set_xticklabels([str(w) for w in WORKERS], fontsize=7.5)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(
            lambda x, _: (f'{x:.0f}' if x >= 1 else
                          f'{x:.1f}' if x >= 0.1 else
                          f'{x:.2f}')
        ))
        ax.yaxis.set_minor_formatter(mticker.NullFormatter())
        ax.set_title(f'$N={n}$', fontsize=9, pad=4)
        ax.grid(True, which='major')
        ax.grid(True, which='minor', linewidth=0.2, alpha=0.2)

        # Axis labels only on outer edges
        if panel_idx in (2, 3):
            ax.set_xlabel('Workers', fontsize=8)
        if panel_idx in (0, 2):
            ax.set_ylabel('ms / sweep', fontsize=8)

    # ── shared legend below the subplots ────────────────────────────────────
    handles, labels = ax_list[0].get_legend_handles_labels()
    fig.legend(handles, labels,
               loc='lower center', ncol=2,
               bbox_to_anchor=(0.5, -0.13),
               frameon=True, framealpha=1.0,
               edgecolor='#cccccc',
               fontsize=7.5,
               handlelength=2.4,
               columnspacing=1.2)

    # ── footnote explaining flow_graph methodology caveat ───────────────────
    fig.text(0.5, -0.20,
             '† TBB flow_graph: worker count via global_control (limits TBB global pool); '
             'no core pinning.\n'
             '  Incompatible with task_arena— results not directly comparable '
             'to pinned arena-based measurements.',
             ha='center', va='top', fontsize=6.5, color='#555555',
             style='italic')

    return fig


def main():
    flow_graph = load_flow_graph()
    missing = [(n, w) for n in NS for w, v in zip(WORKERS, flow_graph[n]) if v is None]
    if missing:
        print(f'Warning: missing flow_graph data for {missing}')

    fig = make_figure(flow_graph)

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'wavefront-comp.png')
    fig.savefig(out_path, dpi=200, bbox_inches='tight')
    print(f'Saved: {out_path}')
    plt.close(fig)


if __name__ == '__main__':
    main()
