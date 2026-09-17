"""Render the exported HPA observations as a standalone portfolio figure."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    output = parser.parse_args().evidence
    rows = [json.loads(line) for line in (output / "observations.jsonl").read_text().splitlines()]
    if not rows:
        raise ValueError("observations must contain at least one sample")

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    minutes = [row["elapsed_seconds"] / 60 for row in rows]
    figure, (replicas, cpu) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    replicas.step(
        minutes,
        [row["desired_replicas"] for row in rows],
        where="post",
        label="HPA desired",
        color="#2563eb",
        linestyle="--",
    )
    replicas.step(
        minutes,
        [row["ready_replicas"] for row in rows],
        where="post",
        label="Deployment ready",
        color="#0f766e",
    )
    replicas.set_ylabel("Replicas")
    replicas.yaxis.set_major_locator(MaxNLocator(integer=True))
    cpu.plot(
        minutes, [row["cpu_utilization"] for row in rows], label="Observed HPA CPU", color="#7c3aed"
    )
    hpa = next(
        item for item in rows[-1]["resources"]["items"] if item["kind"] == "HorizontalPodAutoscaler"
    )
    target = hpa["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"]
    cpu.axhline(target, label=f"CPU target ({target}%)", color="#b45309", linestyle="--")
    cpu.set_ylabel("CPU (% of request)")
    cpu.set_xlabel("Minutes since observations began")
    load = [row["elapsed_seconds"] / 60 for row in rows if row["phase"] == "load"]
    cooldown = [row["elapsed_seconds"] / 60 for row in rows if row["phase"] == "cooldown"]
    for axis in (replicas, cpu):
        if load:
            axis.axvspan(
                min(load),
                min(cooldown) if cooldown else max(load),
                color="#dbeafe",
                alpha=0.6,
                label="Observed load phase",
            )
        axis.grid(alpha=0.2)
        axis.set_ylim(bottom=0)
        axis.legend(loc="upper right", fontsize=8)
    figure.suptitle("Synthetic serving load and CPU autoscaling on kind")
    figure.tight_layout()
    figure.savefig(output / "autoscaling.png", dpi=180)
    plt.close(figure)


if __name__ == "__main__":
    main()
