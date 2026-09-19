"""Run approved-model load/monitoring evidence, or isolated broker failure/recovery on kind.

Requires the approved model already deployed in namespace tripml and its recorded image loaded.
The normal run uses that deployment. --failure creates and removes a separate namespace.
Each attempt requires a new output directory. Benchmark failures are retained, not discarded.
"""

from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "tripml-serving:16bc98e7d329a2a4065de6b1fedc5f240feaae40d319e887cac9eb65adf6c991"
TOPIC = "predictions-batch-release-v2"


class Operations:
    def __init__(self, context: str, namespace: str, output: Path) -> None:
        self.context = context
        self.namespace = namespace
        self.output = output
        self.pod = "release-load-" + uuid4().hex[:8]
        self.created = False
        self.exported = False

    def kube(self, *args: str, body: str | None = None, check: bool = True) -> str:
        result = subprocess.run(
            ["kubectl", "--context", self.context, "-n", self.namespace, *args],
            input=body,
            capture_output=True,
            text=True,
            timeout=360,
        )
        if check and result.returncode:
            raise RuntimeError(f"kubectl {' '.join(args[:3])}: {result.stderr}")
        return result.stdout

    def apply(self, document: object) -> None:
        self.kube("create", "-f", "-", body=json.dumps(document))

    def save(self, name: str, value: object) -> None:
        (self.output / name).write_text(json.dumps(value, indent=2) + "\n")

    def helm(self, *args: str) -> None:
        result = subprocess.run(
            [
                str(ROOT / ".tools/bin/helm"),
                *args,
                "--kube-context",
                self.context,
                "--namespace",
                self.namespace,
            ],
            capture_output=True,
            text=True,
            timeout=360,
        )
        (self.output / "helm.log").write_text(result.stdout + result.stderr)
        result.check_returncode()

    def setup_failure(self) -> None:
        self.kube("create", "namespace", self.namespace)
        self.created = True
        for name in ("mlflow", "minio"):
            self.apply(
                {
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {"name": f"tripml-tripml-{name}"},
                    "spec": {
                        "type": "ExternalName",
                        "externalName": f"tripml-tripml-{name}.tripml.svc.cluster.local",
                    },
                }
            )
        self.apply(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "tripml-infra-credentials"},
                "stringData": {"redis-password": secrets.token_urlsafe(32)},
            }
        )
        self.apply(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "tripml-grafana-admin"},
                "stringData": {"admin-password": secrets.token_urlsafe(32)},
            }
        )
        self.helm(
            "install",
            "tripml",
            str(ROOT / "deploy/helm/tripml"),
            "--values",
            str(ROOT / "deploy/helm/tripml/values-batch-release.yaml"),
            "--set",
            "postgresql.enabled=false,redis.enabled=false,minio.enabled=false",
            "--set",
            "airflow.enabled=false,mlflow.enabled=false,monitoring.enabled=true",
            "--set",
            "serving.trackingUri=http://tripml-tripml-mlflow:5000",
            "--set",
            "serving.redisHost=unused-static",
            "--set",
            "redpanda.persistence.size=1Gi",
            "--wait",
            "--timeout",
            "5m",
        )

    def setup_driver(self, workload: Path) -> None:
        self.apply(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": self.pod},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                    },
                    "containers": [
                        {
                            "name": "load",
                            "image": IMAGE,
                            "imagePullPolicy": "Never",
                            "command": ["python", "-c", "import time; time.sleep(3600)"],
                            "resources": {
                                "requests": {"cpu": "250m", "memory": "256Mi"},
                                "limits": {"cpu": "1", "memory": "512Mi"},
                            },
                            "env": [
                                {
                                    "name": "GRAFANA_PASSWORD",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "tripml-grafana-admin",
                                            "key": "admin-password",
                                        }
                                    },
                                }
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
                        }
                    ],
                    "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "256Mi"}}],
                },
            }
        )
        self.kube("wait", "pod/" + self.pod, "--for=condition=Ready", "--timeout=120s")
        self.kube("exec", self.pod, "--", "mkdir", "/tmp/release-check")
        for source, name in [
            (workload / "requests.jsonl", "requests.jsonl"),
            (workload / "manifest.json", "manifest.json"),
            (ROOT / "scripts/release-operations-driver.py", "driver.py"),
        ]:
            self.kube("cp", str(source), f"{self.pod}:/tmp/release-check/{name}")
        # Wait for actual discovery before offering traffic; no benchmark warm-up is hidden here.
        script = """
import json, time
from urllib.request import urlopen
for _ in range(30):
    with urlopen('http://tripml-tripml-prometheus:9090/api/v1/targets', timeout=10) as r:
        targets = json.load(r)['data']['activeTargets']
    if targets and all(t['health'] == 'up' for t in targets):
        break
    time.sleep(2)
else:
    raise RuntimeError('No healthy serving scrape target')
"""
        self.kube("exec", self.pod, "--", "python", "-c", script)

    def resources(self, phase: str) -> dict[str, Any]:
        objects = json.loads(self.kube("get", "pod,deployment,statefulset", "-o", "json"))
        metrics = self.kube(
            "get",
            "--raw",
            f"/apis/metrics.k8s.io/v1beta1/namespaces/{self.namespace}/pods",
            check=False,
        )
        row = {
            "phase": phase,
            "at": datetime.now(UTC).isoformat(),
            "resources": objects,
            "cpu_memory": json.loads(metrics) if metrics.strip() else None,
        }
        with (self.output / "observations.jsonl").open("a") as sink:
            sink.write(json.dumps(row) + "\n")
        return row

    def driver(self, action: str, phase: str, *args: str) -> int:
        print(f"{self.namespace}: {action} {phase}", flush=True)
        command = [
            "kubectl",
            "--context",
            self.context,
            "-n",
            self.namespace,
            "exec",
            self.pod,
            "--",
            "python",
            "/tmp/release-check/driver.py",
            action,
            "--phase",
            phase,
            *args,
        ]
        with (self.output / f"{action}-{phase}.log").open("w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            deadline = monotonic() + 360
            try:
                while process.poll() is None:
                    self.resources(phase)
                    if monotonic() > deadline:
                        raise TimeoutError("measurement exceeded six minutes")
                    sleep(5)
                code = process.returncode
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=10)
        self.save(f"{action}-{phase}-exit.json", {"exit_code": code})
        return code

    def process_identity(self) -> dict[str, Any]:
        pods = json.loads(
            self.kube("get", "pods", "-l", "app.kubernetes.io/component=serving", "-o", "json")
        )["items"]
        if len(pods) != 1:
            raise ValueError("expected exactly one serving pod")
        pod = pods[0]
        status = next(
            item for item in pod["status"]["containerStatuses"] if item["name"] == "serving"
        )
        started = self.kube(
            "exec",
            pod["metadata"]["name"],
            "--",
            "python",
            "-c",
            "from pathlib import Path; print(Path('/proc/1/stat').read_text().split()[21])",
        )
        return {
            "uid": pod["metadata"]["uid"],
            "container_id": status["containerID"],
            "restarts": status["restartCount"],
            "process_start_ticks": started.strip(),
            "image_id": status["imageID"],
        }

    def recovery_probe(self) -> dict[str, Any]:
        script = """
import json, time, uuid
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
payload = json.loads(Path('/tmp/release-check/requests.jsonl').read_text().splitlines()[0])
payload['trip_id'] = 'recovery-probe-' + uuid.uuid4().hex
started = time.monotonic()
try:
    with urlopen(Request('http://tripml-tripml-serving:8000/v1/eta',
                        data=json.dumps(payload).encode(),
                        headers={'Content-Type':'application/json'}), timeout=2) as r:
        result = {'status': r.status, 'publication': r.headers.get('X-TripML-Publication'),
                  'prediction': json.load(r)}
except HTTPError as e:
    result = {'status': e.code, 'publication': e.headers.get('X-TripML-Publication'),
              'body': json.loads(e.read())}
except (URLError, TimeoutError) as e:
    result = {'error': type(e).__name__}
result['elapsed_seconds'] = time.monotonic()-started
print(json.dumps(result))
"""
        return json.loads(self.kube("exec", self.pod, "--", "python", "-c", script))

    def failure(self) -> dict[str, Any]:
        identity = self.process_identity()
        baseline = self.driver("benchmark", "baseline", "--requests", "2000", "--rate", "20")
        self.kube("scale", "statefulset/tripml-tripml-redpanda", "--replicas=0")
        self.kube("wait", "--for=delete", "pod/tripml-tripml-redpanda-0", "--timeout=120s")
        self.resources("broker-stopped")
        outage_probe = self.recovery_probe()
        self.save("outage-probe.json", outage_probe)
        outage = self.driver(
            "benchmark", "broker-down", "--requests", "120", "--rate", "2", "--warmup", "0"
        )
        restore_started = monotonic()
        self.kube("scale", "statefulset/tripml-tripml-redpanda", "--replicas=1")
        probes = []
        recovered = False
        while monotonic() - restore_started < 60:
            probe = self.recovery_probe()
            probe["seconds_since_restore"] = monotonic() - restore_started
            probes.append(probe)
            if probe.get("status") == 200 and probe.get("publication") == "acknowledged":
                recovered = probe["seconds_since_restore"] <= 60
                break
            sleep(1)
        self.save("recovery-probes.json", probes)
        recovery = self.driver("benchmark", "recovered", "--requests", "2000", "--rate", "20")
        after = self.process_identity()
        result = {
            "baseline_passed": baseline == 0,
            "outage_passed": outage == 0,
            "recovered_load_passed": recovery == 0,
            "recovered_within_60s": recovered,
            "same_api_process": identity == after,
            "outage_response_semantics": outage_probe.get("status") == 503
            and outage_probe.get("publication") == "unconfirmed"
            and outage_probe.get("body", {}).get("detail", {}).get("delivery_unknown") is True,
            "before": identity,
            "after": after,
        }
        self.save("failure-checks.json", result)
        return result

    def export(self) -> None:
        self.kube("cp", f"{self.pod}:/tmp/release-check", str(self.output / "evidence"))
        self.exported = True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--context", default="kind-tripml")
    parser.add_argument("--failure", action="store_true")
    args = parser.parse_args()
    if not args.context.startswith("kind-"):
        raise ValueError("an explicit kind context is required")
    args.output.mkdir(parents=True, exist_ok=False)
    namespace = "tripml-broker-check-" + uuid4().hex[:8] if args.failure else "tripml"
    run = Operations(args.context, namespace, args.output)
    run.save(
        "execution.json",
        {
            "namespace": namespace,
            "context": args.context,
            "image": IMAGE,
            "failure": args.failure,
            "started_at": datetime.now(UTC).isoformat(),
        },
    )
    try:
        if args.failure:
            run.setup_failure()
        run.setup_driver(args.workload)
        before = run.process_identity()
        run.save("serving-before.json", before)
        if args.failure:
            outcome = run.failure()
            passed = all(value for key, value in outcome.items() if key not in {"before", "after"})
        else:
            passed = run.driver("benchmark", "healthy") == 0
        # Capture two more scrape opportunities after the last request.
        sleep(31)
        monitoring = run.driver("monitor", "final")
        delivery = run.driver("readback", "final", "--topic", TOPIC)
        run.resources("finished")
        after = run.process_identity()
        run.save("serving-after.json", after)
        run.export()
        result = {
            "passed": passed and monitoring == 0 and delivery == 0 and before == after,
            "load_or_failure_checks": passed,
            "monitoring": monitoring == 0,
            "broker_readback": delivery == 0,
            "same_api_process": before == after,
        }
        run.save("result.json", result)
        print(json.dumps(result), flush=True)
        return 0 if result["passed"] else 1
    except Exception as error:
        run.save("failure.json", {"type": type(error).__name__, "message": str(error)})
        try:
            run.export()
        except Exception:
            print(
                "Evidence export failed; inspect the retained driver if present.", file=sys.stderr
            )
        raise
    finally:
        if args.failure and run.created:
            run.kube("scale", "statefulset/tripml-tripml-redpanda", "--replicas=1", check=False)
            if run.exported:
                run.kube("delete", "namespace", namespace, "--wait=false")
                run.save("cleanup.json", {"namespace_deletion_requested": namespace})
        elif run.exported:
            run.kube("delete", "pod", run.pod, "--wait=false")
            run.save("cleanup.json", {"driver_deletion_requested": run.pod})


if __name__ == "__main__":
    raise SystemExit(main())
