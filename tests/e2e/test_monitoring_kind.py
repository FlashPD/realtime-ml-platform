"""Opt-in monitoring validation with two real API processes and a synthetic model."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from tripml.training import TrainingRunReport

pytestmark = pytest.mark.e2e


def test_monitoring_discovers_each_api_pod_and_provisions_dashboard(
    helm: str, trained_report: TrainingRunReport
) -> None:
    context = os.getenv("TRIPML_TEST_KIND_CONTEXT", "")
    image = os.getenv("TRIPML_TEST_SERVING_IMAGE", "")
    if os.getenv("TRIPML_TEST_MONITORING") != "1" or not context or not image:
        pytest.skip("Set TRIPML_TEST_MONITORING=1, KIND_CONTEXT and SERVING_IMAGE test variables")
    if not context.startswith("kind-"):
        pytest.fail("monitoring validation requires an explicitly selected kind context")
    namespace = f"tripml-monitoring-check-{uuid4().hex[:8]}"
    root = Path(__file__).resolve().parents[2]
    kube = ["kubectl", "--context", context, "-n", namespace]

    def run(*args: str, body: str | None = None) -> str:
        result = subprocess.run(
            args, input=body, capture_output=True, text=True, check=False, timeout=360
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    def apply(document: object) -> None:
        run(*kube, "apply", "-f", "-", body=yaml.safe_dump(document))

    run(*kube, "create", "namespace", namespace)
    try:
        password = uuid4().hex
        apply(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "tripml-grafana-admin"},
                "stringData": {"admin-password": password},
            }
        )
        bundle = Path(trained_report.artifact_directory)
        apply(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "model"},
                "data": {
                    name: (bundle / name).read_text()
                    for name in ("manifest.json", "static-model.txt", "streaming-model.txt")
                },
            }
        )
        run(
            helm,
            "upgrade",
            "--install",
            "check",
            str(root / "deploy/helm/tripml"),
            "--kube-context",
            context,
            "--namespace",
            namespace,
            "--set",
            "monitoring.enabled=true",
            "--set",
            "postgresql.enabled=false,redis.enabled=false,minio.enabled=false",
            "--set",
            "redpanda.enabled=false,airflow.enabled=false,mlflow.enabled=false",
            "--wait",
            "--timeout",
            "300s",
        )
        labels = {
            "app.kubernetes.io/name": "tripml",
            "app.kubernetes.io/instance": "check",
            "app.kubernetes.io/component": "serving",
        }
        apply(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "api-fixture"},
                "spec": {
                    "replicas": 2,
                    "selector": {"matchLabels": labels},
                    "template": {
                        "metadata": {"labels": labels},
                        "spec": {
                            "automountServiceAccountToken": False,
                            "containers": [
                                {
                                    "name": "serving",
                                    "image": image,
                                    "imagePullPolicy": "IfNotPresent",
                                    "command": [
                                        "tripml",
                                        "serve",
                                        "--bundle",
                                        "/model",
                                        "--host",
                                        "0.0.0.0",
                                    ],
                                    "ports": [{"name": "http", "containerPort": 8000}],
                                    "readinessProbe": {
                                        "httpGet": {"path": "/readyz", "port": "http"}
                                    },
                                    "resources": {
                                        "requests": {"cpu": "100m", "memory": "256Mi"},
                                        "limits": {"cpu": "1", "memory": "1Gi"},
                                    },
                                    "volumeMounts": [
                                        {"name": "model", "mountPath": "/model", "readOnly": True}
                                    ],
                                }
                            ],
                            "volumes": [{"name": "model", "configMap": {"name": "model"}}],
                        },
                    },
                },
            }
        )
        run(*kube, "rollout", "status", "deployment/api-fixture", "--timeout=120s")
        # Execute from the cluster so this also checks ClusterIP and DNS wiring.
        script = """
import base64, json, time, urllib.request
password = PASSWORD
def get(url, auth=False):
    headers = {}
    if auth:
        token = base64.b64encode(('admin:' + password).encode()).decode()
        headers['Authorization'] = 'Basic ' + token
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=5) as r:
        return json.load(r)
prom = 'http://check-tripml-prometheus:9090'
grafana = 'http://check-tripml-grafana:3000'
from urllib.error import HTTPError
try:
    get(grafana + '/api/dashboards/uid/tripml-serving')
except HTTPError as error:
    assert error.code == 401
else:
    raise AssertionError('Dashboard must require authentication')
for attempt in range(30):
    targets = get(prom + '/api/v1/targets')['data']['activeTargets']
    if len(targets) == 2 and all(t['health'] == 'up' for t in targets):
        break
    time.sleep(2)
else:
    raise AssertionError('Expected two individually healthy scrape targets')
assert len({t['labels']['pod'] for t in targets}) == 2
request = dict(trip_id='monitoring-check', pickup_zone_id=161, dropoff_zone_id=236,
               pickup_time='2024-02-01T12:00:00-05:00', trip_distance_miles=3.2, passenger_count=2)
for target in targets:
    url = target['scrapeUrl'].replace('/metrics', '/v1/eta')
    for _ in range(3):
        with urllib.request.urlopen(urllib.request.Request(url, data=json.dumps(request).encode(),
             headers={'Content-Type': 'application/json'}), timeout=5) as r:
            assert r.status == 200
dashboard = get(grafana + '/api/dashboards/uid/tripml-serving', True)['dashboard']
assert len(dashboard['panels']) == 10
from urllib.parse import urlencode
for attempt in range(30):
    query = 'sum(tripml_prediction_requests_total{job="tripml-serving",status="200"})'
    result = get(prom + '/api/v1/query?' + urlencode({'query': query}))['data']['result']
    if result and float(result[0]['value'][1]) == 6:
        break
    time.sleep(2)
else:
    raise AssertionError('Prometheus did not collect all six successful predictions')
for panel in dashboard['panels']:
    for target in panel['targets']:
        query = target['expr'].replace('$__rate_interval', '1m')
        assert get(prom + '/api/v1/query?' + urlencode({'query': query}))['status'] == 'success'
health = get(grafana + '/api/datasources/uid/tripml-prometheus/health', True)
assert health['status'] == 'OK', health
print('Two healthy scrape targets, six predictions, dashboard queries and datasource passed')
""".replace("PASSWORD", repr(password))
        print(run(*kube, "exec", "-i", "deployment/api-fixture", "--", "python", "-", body=script))
    finally:
        run(*kube, "delete", "namespace", namespace, "--wait=false")
