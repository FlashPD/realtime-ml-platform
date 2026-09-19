"""Render full-holdout and passenger-cohort MAE from an immutable training report."""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "tripml-matplotlib"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from tripml.training import TrainingRunReport


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; preserve earlier evidence")
    report = TrainingRunReport.model_validate_json(args.report.read_bytes())
    cohorts = report.passenger_count_cohorts
    if len(cohorts) != 2 or any(cohort.rows == 0 for cohort in cohorts):
        parser.error("the report must contain nonempty known and unknown passenger cohorts")
    names = ["Median baseline", "Static LightGBM", "Offline rolling features"]
    colors = ["#8b98a5", "#1660a7", "#008577"]
    metrics = [
        report.baseline_metrics,
        report.static_candidate_metrics,
        report.streaming_candidate_metrics,
    ]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), gridspec_kw={"width_ratios": [1, 1.2]})
    fig.patch.set_facecolor("white")
    positions = np.arange(3)
    bars = axes[0].barh(
        positions, [metric.mae_seconds for metric in metrics], color=colors, height=0.6
    )
    axes[0].set_yticks(positions, names)
    axes[0].invert_yaxis()
    axes[0].bar_label(bars, fmt="%.2f s", padding=6, fontsize=10)
    axes[0].set_xlim(0, max(metric.mae_seconds for metric in metrics) * 1.3)
    axes[0].set_title(f"All {report.holdout_rows:,} holdout trips", loc="left", pad=14)
    x = np.arange(len(cohorts))
    for offset, (field, name, color) in enumerate(
        zip(
            ("baseline_mae_seconds", "static_mae_seconds", "streaming_mae_seconds"),
            names,
            colors,
            strict=True,
        )
    ):
        values = [getattr(cohort, field) for cohort in cohorts]
        bars = axes[1].bar(x + (offset - 1) * 0.26, values, width=0.24, label=name, color=color)
        axes[1].bar_label(bars, fmt="%.1f", padding=4, fontsize=9)
    axes[1].set_xticks(
        x,
        [
            f"{'Known' if cohort.passenger_count_known else 'Unknown'} count\n{cohort.rows:,} trips"
            for cohort in cohorts
        ],
    )
    axes[1].set_ylim(0, axes[1].get_ylim()[1] * 1.12)
    axes[1].set_title("Passenger-count cohorts", loc="left", pad=14)
    axes[0].set_xlabel("Mean absolute error (seconds); lower is better")
    axes[1].set_ylabel("Mean absolute error (seconds)")
    for axis in axes:
        axis.spines[["top", "right"]].set_visible(False)
    fig.suptitle(
        f"NYC taxi duration: {report.holdout_month} holdout", x=0.03, ha="left", fontsize=16
    )
    fig.text(
        0.03,
        0.02,
        f"Bundle {report.run_id} | Training: {', '.join(report.train_months)}\n"
        "Observed completed-trip distance; rolling features evaluated offline. No live ETA claim.",
        fontsize=9,
        color="#475569",
    )
    fig.tight_layout(rect=(0, 0.11, 1, 0.94))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=160, metadata={"Description": f"Training bundle {report.run_id}"})
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
