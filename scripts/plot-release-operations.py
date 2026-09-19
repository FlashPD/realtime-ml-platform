"""Render exported release load samples and Prometheus telemetry as a standalone figure."""

from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "tripml-matplotlib"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from tripml.benchmark import percentiles


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve the earlier figure")
    healthy = args.evidence / "healthy/evidence"
    failure = args.evidence / "broker-failure/evidence"
    summary = json.loads((healthy / "healthy/summary.json").read_text())
    phases = {
        name: json.loads((failure / name / "summary.json").read_text())
        for name in ("baseline", "broker-down", "recovered")
    }
    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    bins: dict[int, list[float]] = defaultdict(list)
    for line in (healthy / "healthy/samples.jsonl").read_text().splitlines():
        sample = json.loads(line)
        if sample["outcome"] == "success":
            bins[int(sample["scheduled_offset_seconds"])].append(sample["latency_ms"])
    for field, color in (("p95", "#2563eb"), ("p99", "#93c5fd")):
        axes[0, 0].plot(
            sorted(bins),
            [percentiles(bins[key])[field] for key in sorted(bins)],
            label=field.upper() + " per 1-second bin",
            color=color,
        )
    axes[0, 0].axhline(50, color="#dc2626", linestyle="--", label="P95 objective: <50 ms")
    axes[0, 0].set(
        title="100 requests/s: client latency",
        xlabel="Scheduled load time (seconds)",
        ylabel="Latency including dispatch lag (ms)",
        ylim=(0, None),
    )
    axes[0, 0].legend(fontsize=8)
    rows = [
        json.loads(line)
        for line in (args.evidence / "healthy/observations.jsonl").read_text().splitlines()
    ]
    origin = datetime.fromisoformat(rows[0]["at"]).timestamp()
    seconds, cpu, memory = [], [], []
    for row in rows:
        if row["phase"] != "healthy":
            continue
        for pod in (row["cpu_memory"] or {}).get("items", []):
            for container in pod["containers"]:
                if container["name"] == "serving":
                    usage = container["usage"]
                    if not usage["cpu"].endswith("n") or not usage["memory"].endswith("Ki"):
                        raise ValueError("unexpected resource quantity units")
                    seconds.append(datetime.fromisoformat(row["at"]).timestamp() - origin)
                    cpu.append(float(usage["cpu"][:-1]) / 1e6)
                    memory.append(float(usage["memory"][:-2]) / 1024)
    axes[0, 1].plot(seconds, cpu, color="#2563eb", label="CPU")
    axes[0, 1].set(
        title="Serving resource observations",
        xlabel="Seconds since observation start",
        ylabel="CPU (millicores)",
        ylim=(0, None),
    )
    right = axes[0, 1].twinx()
    right.plot(seconds, memory, color="#059669", label="Memory")
    right.set(ylabel="Memory (MiB)", ylim=(0, max(memory) * 1.3))
    axes[0, 1].tick_params(axis="y", labelcolor="#2563eb")
    right.tick_params(axis="y", labelcolor="#059669")
    monitoring = json.loads((failure / "final-dashboard-queries.json").read_text())
    traffic = next(
        row for row in monitoring["queries"] if row["panel"] == "Prediction responses by status"
    )
    start = datetime.fromisoformat(phases["baseline"]["started_at"]).timestamp()
    for series in traffic["response"]["data"]["result"]:
        status = series["metric"]["status"]
        if status not in {"200", "503"}:
            continue
        values = [
            (float(time), float(value))
            for time, value in series["values"]
            if math.isfinite(float(value))
        ]
        axes[1, 0].plot(
            [(time - start) / 60 for time, _ in values],
            [value for _, value in values],
            label="HTTP " + status,
            color="#059669" if status == "200" else "#dc2626",
        )
    down = phases["broker-down"]
    axes[1, 0].axvspan(
        (datetime.fromisoformat(down["started_at"]).timestamp() - start) / 60,
        (datetime.fromisoformat(down["finished_at"]).timestamp() - start) / 60,
        color="#fee2e2",
        label="Broker-down measurement",
    )
    axes[1, 0].set(
        title="Isolated failure: Prometheus response rates (1-minute window)",
        xlabel="Minutes since baseline began",
        ylabel="Responses/second",
        ylim=(0, None),
    )
    axes[1, 0].legend(fontsize=8)
    labels = ["Baseline\nHTTP 200", "Broker down\nHTTP 503", "Recovered\nHTTP 200"]
    latencies = [phase["all_attempted_latency_ms"]["p95"] for phase in phases.values()]
    bars = axes[1, 1].bar(labels, latencies, color=["#2563eb", "#dc2626", "#059669"], width=0.55)
    axes[1, 1].set_yscale("log")
    axes[1, 1].set(
        title="Isolated failure: bounded responses", ylabel="Client P95 latency (ms, log scale)"
    )
    axes[1, 1].set_ylim(1, max(latencies) * 3)
    axes[1, 1].bar_label(bars, fmt="%.2f ms", padding=6, fontsize=9)
    for axis in axes.flat:
        axis.spines[["top", "right"]].set_visible(False)
        axis.grid(axis="y", alpha=0.18)
    fig.suptitle("Approved static model: April load and broker failure on kind", fontsize=16)
    fig.text(
        0.04,
        0.02,
        f"10,000 requests at 100/s | Client P95 {summary['successful_latency_ms']['p95']:.2f} ms | "
        f"{summary['scheduled_requests'] - summary['successful_requests']} errors / "
        f"{summary['outcomes'].get('load_generator_overflow', 0)} dropped arrivals\n"
        "Failure profile: 20/s baseline/recovery, 2/s outage. Exported telemetry; "
        "single-node local evidence, not a capacity claim.",
        fontsize=9,
        color="#475569",
    )
    fig.tight_layout(rect=(0, 0.09, 1, 0.95))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=160)
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
