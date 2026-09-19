"""Opt-in serving deployment test using a temporary namespace on an existing kind platform."""

from __future__ import annotations

import json
import os
import subprocess
import traceback
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from kind_load import run_kind_load

from tripml.contracts import OnlineZoneWindowFeatures

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.e2e


def test_serving_deployment_from_registry_to_acknowledged_http(
    tmp_path: Path, online_snapshots: tuple[OnlineZoneWindowFeatures, ...]
) -> None:
    context = os.getenv("TRIPML_TEST_KIND_CONTEXT")
    image = os.getenv("TRIPML_TEST_SERVING_IMAGE")
    if not context or not image:
        pytest.skip("TRIPML_TEST_KIND_CONTEXT and TRIPML_TEST_SERVING_IMAGE are not configured")
    if not context.startswith("kind-"):
        pytest.fail("the serving smoke test requires an explicitly selected kind context")
    infrastructure = os.getenv("TRIPML_TEST_INFRA_NAMESPACE", "tripml")
    release = os.getenv("TRIPML_TEST_INFRA_RELEASE", "tripml")
    namespace = f"tripml-serving-check-{uuid4().hex[:8]}"
    topic = namespace
    base = f"{release}-tripml"
    helm = str(ROOT.parent / ".tools/bin/helm")
    repository = ROOT.parent
    load_enabled = os.getenv("TRIPML_TEST_KIND_LOAD") == "1"
    output: Path | None = None

    def run(
        *args: str, body: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            args, input=body, capture_output=True, text=True, check=check, timeout=360
        )

    def kube(
        *args: str, body: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return run("kubectl", "--context", context, "-n", namespace, *args, body=body, check=check)

    def apply(document: object) -> None:
        kube("apply", "-f", "-", body=yaml.safe_dump(document))

    if load_enabled:
        run(
            "kubectl",
            "--context",
            context,
            "wait",
            "apiservice/v1beta1.metrics.k8s.io",
            "--for=condition=Available",
            "--timeout=30s",
        )
        output = Path(os.environ["TRIPML_KIND_LOAD_OUTPUT"]).resolve()
        output.mkdir(parents=True, exist_ok=False)
        (output / "cluster-version.json").write_text(
            run("kubectl", "--context", context, "version", "-o", "json").stdout
        )
        (output / "nodes.json").write_text(
            run("kubectl", "--context", context, "get", "nodes", "-o", "json").stdout
        )
    run("kubectl", "--context", context, "create", "namespace", namespace)
    try:
        if load_enabled:
            apply(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": "fixture-credentials"},
                    "stringData": {"redis-password": uuid4().hex},
                }
            )
        else:
            credentials = json.loads(
                run(
                    "kubectl",
                    "--context",
                    context,
                    "-n",
                    infrastructure,
                    "get",
                    "secret",
                    "tripml-infra-credentials",
                    "-o",
                    "json",
                ).stdout
            )
            apply(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": "fixture-credentials"},
                    "data": {"redis-password": credentials["data"]["redis-password"]},
                }
            )
        # Broker metadata advertises its short service name, resolved in the test namespace.
        apply(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": f"{base}-redpanda"},
                "spec": {
                    "type": "ExternalName",
                    "externalName": f"{base}-redpanda.{infrastructure}.svc.cluster.local",
                },
            }
        )
        apply(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "mlflow-fixture"},
                "spec": {"selector": {"app": "mlflow-fixture"}, "ports": [{"port": 5000}]},
            }
        )
        apply(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "mlflow-fixture"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "mlflow-fixture"}},
                    "template": {
                        "metadata": {"labels": {"app": "mlflow-fixture"}},
                        "spec": {
                            "automountServiceAccountToken": False,
                            "securityContext": {
                                "runAsUser": 10001,
                                "runAsGroup": 10001,
                                "fsGroup": 10001,
                            },
                            "containers": [
                                {
                                    "name": "mlflow",
                                    "image": image,
                                    "imagePullPolicy": "Never",
                                    "command": [
                                        "mlflow",
                                        "server",
                                        "--host",
                                        "0.0.0.0",
                                        "--port",
                                        "5000",
                                        "--workers",
                                        "1",
                                        "--backend-store-uri",
                                        "sqlite:////tmp/mlflow.db",
                                        "--artifacts-destination",
                                        "/tmp/artifacts",
                                        "--allowed-hosts",
                                        "mlflow-fixture,mlflow-fixture:5000",
                                    ],
                                    "readinessProbe": {
                                        "httpGet": {"path": "/health", "port": 5000},
                                        "periodSeconds": 2,
                                    },
                                    "resources": {
                                        "requests": {"cpu": "100m", "memory": "256Mi"},
                                        "limits": {"cpu": "1", "memory": "2Gi"},
                                    },
                                    "securityContext": {
                                        "readOnlyRootFilesystem": True,
                                        "allowPrivilegeEscalation": False,
                                    },
                                    "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
                                }
                            ],
                            "volumes": [{"name": "tmp", "emptyDir": {}}],
                        },
                    },
                },
            }
        )
        kube("rollout", "status", "deployment/mlflow-fixture", "--timeout=180s")
        kube(
            "create",
            "configmap",
            "registry-seed",
            f"--from-file=seed.py={ROOT / 'e2e/seed_serving_registry.py'}",
        )
        apply(
            {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": "registry-seed"},
                "spec": {
                    "backoffLimit": 0,
                    "activeDeadlineSeconds": 180,
                    "template": {
                        "spec": {
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "seed",
                                    "image": image,
                                    "imagePullPolicy": "Never",
                                    "command": ["python", "/seed/seed.py"],
                                    "env": [
                                        {
                                            "name": "TRIPML_TRACKING__TRACKING_URI",
                                            "value": "http://mlflow-fixture:5000",
                                        }
                                    ],
                                    "resources": {
                                        "requests": {"cpu": "100m", "memory": "256Mi"},
                                        "limits": {"cpu": "1", "memory": "1Gi"},
                                    },
                                    "volumeMounts": [{"name": "seed", "mountPath": "/seed"}],
                                }
                            ],
                            "volumes": [{"name": "seed", "configMap": {"name": "registry-seed"}}],
                        }
                    },
                },
            }
        )
        seeded = kube(
            "wait", "job/registry-seed", "--for=condition=complete", "--timeout=180s", check=False
        )
        assert seeded.returncode == 0, kube("logs", "job/registry-seed").stdout
        repository_name, tag = image.rsplit(":", 1)
        values = {
            key: {"enabled": False}
            for key in ("postgresql", "redis", "minio", "redpanda", "airflow", "mlflow")
        }
        values.update(
            {
                "credentialsSecret": "fixture-credentials",
                "global": {"imagePullPolicy": "Never"},
                "serving": {
                    "enabled": True,
                    "image": {"repository": repository_name, "tag": tag},
                    "trackingUri": "http://mlflow-fixture:5000",
                    "redisHost": f"{base}-redis.{infrastructure}.svc.cluster.local",
                    "bootstrapServers": f"{base}-redpanda.{infrastructure}.svc.cluster.local:9092",
                    "predictionTopic": topic,
                },
            }
        )
        if load_enabled:
            values["redis"] = {"enabled": True}
            values["serving"]["redisHost"] = "serving-check-tripml-redis"
            values["serving"]["autoscaling"] = {"enabled": True}
        values_file = tmp_path / "values.yaml"
        values_file.write_text(yaml.safe_dump(values))
        if output is not None:
            (output / "values.yaml").write_text(values_file.read_text())
        deployed = run(
            helm,
            "upgrade",
            "--install",
            "serving-check",
            str(repository / "deploy/helm/tripml"),
            "--kube-context",
            context,
            "-n",
            namespace,
            "-f",
            str(values_file),
            "--wait",
            "--timeout",
            "3m",
            check=False,
        )
        if deployed.returncode:
            pytest.fail(
                deployed.stderr
                + kube(
                    "logs",
                    "deployment/serving-check-tripml-serving",
                    "--all-containers",
                    check=False,
                ).stdout
            )
        checked = run(
            helm,
            "test",
            "serving-check",
            "--kube-context",
            context,
            "-n",
            namespace,
            "--filter",
            "name=serving-check-tripml-test-serving",
            "--logs",
            "--timeout",
            "2m",
            check=False,
        )
        assert checked.returncode == 0, checked.stdout + checked.stderr
        assert "prediction_id" in checked.stdout
        deployment = json.loads(
            kube("get", "deployment", "serving-check-tripml-serving", "-o", "json").stdout
        )
        assert deployment["status"]["availableReplicas"] >= 1
        if output is not None:
            run_kind_load(
                kube,
                apply,
                image=image,
                namespace=namespace,
                output=output,
                snapshots=online_snapshots,
            )
    except Exception:
        if output is not None:
            (output / "failure.txt").write_text(traceback.format_exc())
        print(kube("get", "pods", check=False).stdout)
        for workload in (
            "deployment/mlflow-fixture",
            "job/registry-seed",
            "deployment/serving-check-tripml-serving",
        ):
            print(kube("logs", workload, "--all-containers", "--tail=40", check=False).stdout)
        raise
    finally:
        try:
            if output is not None:
                for resource in ("pods", "events"):
                    (output / f"cleanup-{resource}.json").write_text(
                        kube("get", resource, "-o", "json", check=False).stdout
                    )
                for workload in ("pod/load-client", "deployment/serving-check-tripml-serving"):
                    (output / f"{workload.split('/')[-1]}.log").write_text(
                        kube("logs", workload, "--all-containers", check=False).stdout
                    )
        finally:
            try:
                # Delete only the uniquely named topic created by this test; retain platform state.
                run(
                    "kubectl",
                    "--context",
                    context,
                    "-n",
                    infrastructure,
                    "exec",
                    f"{base}-redpanda-0",
                    "--",
                    "rpk",
                    "topic",
                    "delete",
                    topic,
                    "-X",
                    "brokers=localhost:9092",
                    check=False,
                )
            finally:
                run(
                    "kubectl",
                    "--context",
                    context,
                    "delete",
                    "namespace",
                    namespace,
                    "--wait=false",
                )
