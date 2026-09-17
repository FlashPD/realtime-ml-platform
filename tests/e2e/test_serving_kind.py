"""Opt-in serving deployment test using a temporary namespace on an existing kind platform."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.e2e


def test_serving_deployment_from_registry_to_acknowledged_http(tmp_path: Path) -> None:
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

    run("kubectl", "--context", context, "create", "namespace", namespace)
    try:
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
        values_file = tmp_path / "values.yaml"
        values_file.write_text(yaml.safe_dump(values))
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
        assert deployment["status"]["availableReplicas"] == 1
    except Exception:
        print(kube("get", "pods", check=False).stdout)
        for workload in (
            "deployment/mlflow-fixture",
            "job/registry-seed",
            "deployment/serving-check-tripml-serving",
        ):
            print(kube("logs", workload, "--all-containers", check=False).stdout)
        raise
    finally:
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
        run("kubectl", "--context", context, "delete", "namespace", namespace, "--wait=false")
