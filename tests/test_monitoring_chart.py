from __future__ import annotations

import json
from typing import Any

import pytest
import yaml

from test_serving_chart import _render


def _objects(helm: str, *settings: str) -> dict[tuple[str, str], Any]:
    result = _render(helm, *settings)
    assert result.returncode == 0, result.stderr
    return {
        (item["kind"], item["metadata"]["name"]): item
        for item in yaml.safe_load_all(result.stdout)
        if item
    }


def test_monitoring_is_opt_in(helm: str) -> None:
    objects = _objects(helm)
    assert not any(name.endswith(("-prometheus", "-grafana", "-dashboards")) for _, name in objects)


def test_discovery_is_per_pod_scoped_and_does_not_require_cluster_permissions(helm: str) -> None:
    objects = _objects(helm, "monitoring.enabled=true")
    config = yaml.safe_load(
        objects["ConfigMap", "test-tripml-prometheus"]["data"]["prometheus.yml"]
    )
    scrape = config["scrape_configs"][0]
    discovery = scrape["kubernetes_sd_configs"][0]
    assert discovery["role"] == "pod"
    assert discovery["namespaces"]["names"] == ["tripml"]
    assert discovery["selectors"][0]["label"] == (
        "app.kubernetes.io/name=tripml,app.kubernetes.io/instance=test,"
        "app.kubernetes.io/component=serving"
    )
    assert scrape["relabel_configs"][1] == {
        "source_labels": [
            "__meta_kubernetes_pod_container_name",
            "__meta_kubernetes_pod_container_port_name",
        ],
        "action": "keep",
        "regex": "serving;http",
    }
    assert objects["Role", "test-tripml-prometheus"]["rules"] == [
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch"]}
    ]
    assert not {kind for kind, _ in objects} & {"ClusterRole", "ClusterRoleBinding", "Secret"}


def test_provisioned_dashboard_queries_match_the_datasource_and_metric_contract(helm: str) -> None:
    objects = _objects(helm, "monitoring.enabled=true")
    provisioning = objects["ConfigMap", "test-tripml-grafana"]["data"]
    source = yaml.safe_load(provisioning["datasources.yaml"])["datasources"][0]
    dashboard = json.loads(objects["ConfigMap", "test-tripml-dashboards"]["data"]["serving.json"])
    assert source["url"] == "http://test-tripml-prometheus:9090"
    assert dashboard["uid"] == "tripml-serving"
    assert len(dashboard["panels"]) == 10
    for panel in dashboard["panels"]:
        assert panel["datasource"]["uid"] == source["uid"]
        assert panel["description"]
        for target in panel["targets"]:
            assert 'job="tripml-serving"' in target["expr"]
    latency = dashboard["panels"][1]["targets"][1]["expr"]
    assert "histogram_quantile(0.95, sum by (le) (rate(" in latency
    errors = dashboard["panels"][2]["targets"][0]["expr"]
    assert "or (0 * sum(" in errors  # Zero errors may have no counter label yet.
    assert "clamp_min" not in errors  # Do not turn no traffic into a healthy zero.


def test_monitoring_is_private_authenticated_and_bounded(helm: str) -> None:
    objects = _objects(helm, "monitoring.enabled=true", "monitoring.grafana.adminSecret=custom")
    for component in ("prometheus", "grafana"):
        name = f"test-tripml-{component}"
        assert objects["Service", name]["spec"]["type"] == "ClusterIP"
        template = objects["Deployment", name]["spec"]["template"]
        assert len(template["metadata"]["annotations"]["checksum/config"]) == 64
        pod = template["spec"]
        assert pod["automountServiceAccountToken"] is (component == "prometheus")
        assert pod["securityContext"]["runAsNonRoot"] is True
        container = pod["containers"][0]
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
        assert container["resources"]["limits"]["memory"]
        assert container["startupProbe"]["httpGet"]["port"] == "http"
        if component == "grafana":
            env = {entry["name"]: entry for entry in container["env"]}
            assert env["GF_AUTH_ANONYMOUS_ENABLED"]["value"] == "false"
            assert env["GF_PLUGINS_PREINSTALL_DISABLED"]["value"] == "true"
            assert env["GF_PLUGINS_PREINSTALL_AUTO_UPDATE"]["value"] == "false"
            assert env["GF_SECURITY_ADMIN_PASSWORD"]["valueFrom"]["secretKeyRef"] == {
                "name": "custom",
                "key": "admin-password",
            }
        else:
            assert "--storage.tsdb.retention.time=24h" in container["args"]
            assert "--storage.tsdb.retention.size=1GB" in container["args"]


@pytest.mark.parametrize(
    "setting",
    [
        "monitoring.grafana.adminSecret=",
        "monitoring.prometheus.persistence.size=0Gi",
        "monitoring.grafana.resources.requests.cpu=0",
    ],
)
def test_invalid_monitoring_configuration_is_rejected(helm: str, setting: str) -> None:
    assert _render(helm, "monitoring.enabled=true", setting).returncode != 0
