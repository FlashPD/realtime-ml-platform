"""Host-side observation and evidence export for an isolated, in-cluster CPU HPA test."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic, sleep
from typing import Any

from prometheus_client.parser import text_string_to_metric_families

from tripml.contracts import OnlineZoneWindowFeatures

ROOT = Path(__file__).resolve().parents[2]


def current_cpu_utilization(hpa: dict[str, Any]) -> int | None:
    for metric in (hpa.get("status") or {}).get("currentMetrics") or []:
        resource = metric.get("resource") or {}
        if resource.get("name") == "cpu":
            return (resource.get("current") or {}).get("averageUtilization")
    return None


def scaling_checks(
    observations: list[dict[str, Any]], pod_requests: dict[str, float]
) -> dict[str, bool]:
    baseline = [row for row in observations if row["phase"] == "baseline"]
    load = [row for row in observations if row["phase"] == "load"]
    cooldown = [row for row in observations if row["phase"] == "cooldown"]
    return {
        "one_ready_replica_before_load": bool(baseline)
        and baseline[-1]["ready_replicas"] == 1
        and baseline[-1]["desired_replicas"] == 1,
        "cpu_metrics_available_under_load": any(row["cpu_utilization"] is not None for row in load),
        "hpa_requested_scale_up_under_load": any(row["desired_replicas"] >= 2 for row in load),
        "additional_replicas_ready_under_load": any(row["ready_replicas"] >= 2 for row in load),
        "multiple_pods_served_predictions": sum(count > 0 for count in pod_requests.values()) >= 2,
        "returned_to_one_replica_after_load": bool(cooldown)
        and cooldown[-1]["desired_replicas"] == 1
        and cooldown[-1]["ready_replicas"] == 1,
    }


def run_kind_load(
    kube: Callable[..., subprocess.CompletedProcess[str]],
    apply: Callable[[object], None],
    *,
    image: str,
    namespace: str,
    output: Path,
    snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    # The caller owns namespace/topic cleanup and exports diagnostics on failure.
    observations: list[dict[str, Any]] = []
    started = monotonic()

    def record(phase: str) -> dict[str, Any]:
        resources = json.loads(
            kube(
                "get",
                "hpa,deployment,pods",
                "-l",
                "app.kubernetes.io/component=serving",
                "-o",
                "json",
            ).stdout
        )
        (output / "latest-resources.json").write_text(json.dumps(resources, indent=2))
        hpa = next(item for item in resources["items"] if item["kind"] == "HorizontalPodAutoscaler")
        deployment = next(item for item in resources["items"] if item["kind"] == "Deployment")
        status = hpa.get("status", {})
        cpu = current_cpu_utilization(hpa)
        metrics = kube(
            "get", "--raw", f"/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods", check=False
        )
        row = {
            "phase": phase,
            "observed_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": monotonic() - started,
            "desired_replicas": status.get("desiredReplicas", 0),
            "ready_replicas": deployment.get("status", {}).get("readyReplicas", 0),
            "cpu_utilization": cpu,
            "resources": resources,
            "pod_metrics": json.loads(metrics.stdout) if metrics.returncode == 0 else None,
        }
        observations.append(row)
        with (output / "observations.jsonl").open("a", encoding="utf-8") as sink:
            sink.write(json.dumps(row) + "\n")
        return row

    deadline = monotonic() + 180
    while True:
        row = record("baseline")
        if (
            row["desired_replicas"] == row["ready_replicas"] == 1
            and row["cpu_utilization"] is not None
        ):
            break
        if monotonic() > deadline:
            raise TimeoutError("HPA did not reach a one-replica baseline with CPU metrics")
        sleep(5)
    inputs = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "load-inputs"},
        "data": {
            "driver.py": (ROOT / "tests/e2e/in_cluster_load.py").read_text(),
            "requests.jsonl": (ROOT / "examples/benchmark/requests.jsonl").read_text(),
            "snapshots.json": json.dumps(
                [snapshot.model_dump(mode="json") for snapshot in snapshots]
            ),
        },
    }
    (output / "load-inputs.json").write_text(json.dumps(inputs, indent=2))
    apply(inputs)
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "load-client"},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 10001,
                "runAsGroup": 10001,
                "fsGroup": 10001,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "load",
                    "image": image,
                    "imagePullPolicy": "Never",
                    "command": ["python", "/load/driver.py"],
                    "env": [
                        {
                            "name": "TRIPML_LOAD_URL",
                            "value": "http://serving-check-tripml-serving:8000",
                        },
                        {
                            "name": "REDIS_PASSWORD",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": "fixture-credentials",
                                    "key": "redis-password",
                                },
                            },
                        },
                        {
                            "name": "TRIPML_SERVING__REDIS_URL",
                            "value": "redis://:$(REDIS_PASSWORD)@serving-check-tripml-redis:6379/0",
                        },
                    ],
                    "resources": {
                        "requests": {"cpu": "250m", "memory": "128Mi"},
                        "limits": {"cpu": "1", "memory": "512Mi"},
                    },
                    "securityContext": {
                        "readOnlyRootFilesystem": True,
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "volumeMounts": [
                        {"name": "inputs", "mountPath": "/load", "readOnly": True},
                        {"name": "tmp", "mountPath": "/tmp"},
                    ],
                }
            ],
            "volumes": [
                {"name": "inputs", "configMap": {"name": "load-inputs"}},
                {"name": "tmp", "emptyDir": {"sizeLimit": "256Mi"}},
            ],
        },
    }
    (output / "load-pod.json").write_text(json.dumps(pod, indent=2))
    apply(pod)
    kube("wait", "pod/load-client", "--for=condition=Ready", "--timeout=90s")
    deadline = monotonic() + 300
    while True:
        row = record("load")
        completed = kube(
            "exec", "load-client", "--", "cat", "/tmp/evidence/result.json", check=False
        )
        if completed.returncode == 0:
            break
        if monotonic() > deadline:
            raise TimeoutError("in-cluster benchmark did not finish before its deadline")
        sleep(5)
    kube("cp", "load-client:/tmp/evidence", str(output / "client"))
    result = json.loads(completed.stdout)
    pod_requests: dict[str, float] = {}
    for item in row["resources"]["items"]:
        if item["kind"] != "Pod" or item.get("status", {}).get("phase") != "Running":
            continue
        name = item["metadata"]["name"]
        response = kube(
            "get", "--raw", f"/api/v1/namespaces/{namespace}/pods/{name}:8000/proxy/metrics"
        )
        (output / f"{name}-metrics.txt").write_text(response.stdout)
        pod_requests[name] = sum(
            sample.value
            for family in text_string_to_metric_families(response.stdout)
            for sample in family.samples
            if sample.name == "tripml_prediction_requests_total"
            and sample.labels.get("status") == "200"
        )
    # Keep the chart's actual 300-second scale-down stabilization window.
    deadline = monotonic() + 480
    while True:
        row = record("cooldown")
        if row["desired_replicas"] == row["ready_replicas"] == 1:
            break
        if monotonic() > deadline:
            break
        sleep(5)
    (output / "events.json").write_text(kube("get", "events", "-o", "json").stdout)
    plot_script = output / "plot-serving-load.py"
    plot_script.write_text((ROOT / "scripts/plot-serving-load.py").read_text())
    with tempfile.TemporaryDirectory(prefix="tripml-plot-") as cache:
        subprocess.run(
            [sys.executable, str(plot_script), str(output)],
            check=True,
            timeout=30,
            env=os.environ | {"MPLCONFIGDIR": cache, "XDG_CACHE_HOME": cache},
        )
    registry = json.loads(kube("get", "pods", "-l", "app=mlflow-fixture", "-o", "json").stdout)
    registry_pod = registry["items"][0]["metadata"]["name"]
    kube("cp", f"{registry_pod}:/tmp/artifacts", str(output / "registry-artifacts"))
    checks = scaling_checks(observations, pod_requests)
    checks["in_cluster_prediction_objectives"] = result["passed"]
    summary = {
        "passed": all(checks.values()),
        "checks": checks,
        "load_result": result,
        "pod_successful_predictions": pod_requests,
        "peak_ready_replicas": max(row["ready_replicas"] for row in observations),
        "artifacts_sha256": {
            str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(output.rglob("*"))
            if path.is_file()
        },
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    (output / "README.md").write_text(
        "# In-cluster CPU HPA evidence\n\n"
        f"Result: **{'PASS' if summary['passed'] else 'FAIL'}**. "
        f"Peak ready replicas: **{summary['peak_ready_replicas']}**.\n\n"
        "![Observed CPU and replica counts](autoscaling.png)\n\n"
        "[Objective checks](summary.json), [raw HPA/CPU/replica observations](observations.jsonl), "
        "[prediction benchmark](client/benchmark/README.md), [Kubernetes events](events.json).\n\n"
        "Synthetic model and seeded snapshots; 18,000 scheduled requests at 100/s, "
        "with broker acknowledgments. The client runs in-cluster against the Service with "
        "HTTP keep-alive disabled so new connections can reach newly ready replicas. "
        "The default CPU target, 1-3 replicas, and 300-second scale-down stabilization "
        "are retained. "
        "This is single-node kind evidence, not high availability, real-data model quality, "
        "feature parity, request-rate scaling, or production capacity.\n",
    )
    assert summary["passed"], f"Load/HPA checks failed; inspect {output / 'README.md'}"
