from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

import yaml
from mlflow.server.security_utils import is_allowed_host_header


def test_mlflow_allows_local_forwarded_ports_but_rejects_foreign_hosts(helm: str) -> None:
    chart = Path(__file__).resolve().parents[1] / "deploy/helm/tripml"
    rendered = subprocess.run(
        [helm, "template", "test", str(chart)], capture_output=True, text=True, check=True
    )
    deployment = next(
        item
        for item in yaml.safe_load_all(rendered.stdout)
        if item
        and item["kind"] == "Deployment"
        and item["metadata"]["name"] == "test-tripml-mlflow"
    )
    command = shlex.split(deployment["spec"]["template"]["spec"]["containers"][0]["args"][0])
    allowed = command[command.index("--allowed-hosts") + 1].split(",")
    for host in ("localhost:5000", "127.0.0.1:15000", "test-tripml-mlflow:5000"):
        assert is_allowed_host_header(allowed, host), host
    for host in ("evil.example:5000", "127.0.0.1.evil.example:15000", "localhost.evil.example"):
        assert not is_allowed_host_header(allowed, host), host
