from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def helm() -> str:
    binary = os.getenv("TRIPML_HELM") or str(ROOT / ".tools/bin/helm")
    if not Path(binary).is_file():
        binary = shutil.which("helm") or ""
    if not binary:
        pytest.skip("Helm is unavailable; run make tools")
    return binary


def _render(helm: str, *settings: str) -> subprocess.CompletedProcess[str]:
    command = [helm, "template", "test", str(ROOT / "deploy/helm/tripml"), "--namespace", "tripml"]
    for setting in settings:
        command.extend(["--set", setting])
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _serving_objects(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    assert result.returncode == 0, result.stderr
    return {
        item["kind"]: item
        for item in yaml.safe_load_all(result.stdout)
        if item and item["metadata"]["name"] == "test-tripml-serving"
    }


def test_serving_is_opt_in_before_a_production_model_exists(helm: str) -> None:
    assert not _serving_objects(_render(helm))


def test_serving_renders_wiring_security_and_real_health_probes(helm: str) -> None:
    objects = _serving_objects(_render(helm, "serving.enabled=true", "serving.replicas=2"))
    assert objects["Service"]["spec"]["type"] == "ClusterIP"
    deployment = objects["Deployment"]["spec"]
    assert deployment["replicas"] == 2
    assert deployment["strategy"]["rollingUpdate"]["maxUnavailable"] == 0
    pod = deployment["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["runAsUser"] == 10001
    container = pod["containers"][0]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["startupProbe"]["httpGet"]["path"] == "/readyz"
    assert container["readinessProbe"]["httpGet"]["path"] == "/readyz"
    assert container["livenessProbe"]["httpGet"]["path"] == "/healthz"
    env = {item["name"]: item for item in container["env"]}
    assert env["TRIPML_TRACKING__TRACKING_URI"]["value"] == "http://test-tripml-mlflow:5000"
    assert env["REDIS_PASSWORD"]["valueFrom"]["secretKeyRef"]["key"] == "redis-password"
    assert "$(REDIS_PASSWORD)" in env["TRIPML_SERVING__REDIS_URL"]["value"]
    assert env["TRIPML_PUBLICATION__BOOTSTRAP_SERVERS"]["value"] == "test-tripml-redpanda:9092"
    assert pod["initContainers"][0]["command"] == ["tripml", "publication", "ensure-topic"]
    assert "HorizontalPodAutoscaler" not in objects


def test_hpa_owns_replicas_and_has_resource_requests(helm: str) -> None:
    objects = _serving_objects(
        _render(helm, "serving.enabled=true", "serving.autoscaling.enabled=true")
    )
    assert "replicas" not in objects["Deployment"]["spec"]
    pod = objects["Deployment"]["spec"]["template"]["spec"]
    assert pod["containers"][0]["resources"]["requests"]["cpu"] == "100m"
    hpa = objects["HorizontalPodAutoscaler"]["spec"]
    assert hpa["scaleTargetRef"]["name"] == "test-tripml-serving"
    assert (hpa["minReplicas"], hpa["maxReplicas"]) == (1, 3)
    assert hpa["metrics"][0]["resource"]["target"]["averageUtilization"] == 70


@pytest.mark.parametrize(
    "setting",
    [
        "serving.resources.requests.cpu=0",
        "serving.autoscaling.maxReplicas=0",
        "serving.autoscaling.targetCPUUtilizationPercentage=0",
        "serving.topicPartitions=0",
        "mlflow.enabled=false",
        "redis.enabled=false",
        "redpanda.enabled=false",
    ],
)
def test_invalid_or_missing_serving_dependencies_fail_rendering(helm: str, setting: str) -> None:
    assert _render(helm, "serving.enabled=true", setting).returncode != 0


def test_inverted_autoscaling_range_fails(helm: str) -> None:
    result = _render(
        helm,
        "serving.enabled=true",
        "serving.autoscaling.enabled=true",
        "serving.autoscaling.minReplicas=4",
        "serving.autoscaling.maxReplicas=2",
    )
    assert result.returncode != 0
    assert "must not exceed" in result.stderr


def test_external_service_addresses_are_used_when_dependencies_are_disabled(helm: str) -> None:
    objects = _serving_objects(
        _render(
            helm,
            "serving.enabled=true",
            "mlflow.enabled=false",
            "redis.enabled=false",
            "redpanda.enabled=false",
            "serving.trackingUri=http://registry:5000",
            "serving.redisHost=features",
            "serving.bootstrapServers=broker:9092",
        )
    )
    container = objects["Deployment"]["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item for item in container["env"]}
    assert env["TRIPML_TRACKING__TRACKING_URI"]["value"] == "http://registry:5000"
    assert env["TRIPML_PUBLICATION__BOOTSTRAP_SERVERS"]["value"] == "broker:9092"
