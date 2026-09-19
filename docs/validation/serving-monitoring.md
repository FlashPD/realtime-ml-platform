# Local serving monitoring validation — 2026-09-17

The isolated kind smoke test passed from a fresh namespace in 45.14 seconds with the final chart.
Prometheus v3.14.0 and Grafana 13.2.2 ran alongside two prediction API pods using the existing local
`tripml-serving:hpa-validation` image and a synthetic model trained by the test fixture.

| Check | Result |
|---|---|
| Per-pod discovery | Two distinct healthy targets with pod labels |
| API collection | All six successful predictions present in Prometheus |
| Dashboard provisioning | `tripml-serving` loaded with ten panels |
| PromQL execution | Every provisioned query accepted by Prometheus |
| Grafana datasource | Health check returned `OK` |
| Dashboard authentication | Anonymous API read returned HTTP 401; admin read succeeded |
| Local quality gate | 255 tests passed, six optional service tests skipped, 95.10% coverage; Ruff, mypy and Helm checks passed |

The monitoring kind test was executed separately from the default local suite. Its console result:

```text
Two healthy scrape targets, six predictions, dashboard queries and datasource passed
1 passed in 45.14s
```

The initial deployment exposed Grafana's default background plugin installation and update behavior:
catalog requests timed out and attempts to modify bundled plugins failed on the read-only filesystem.
The final chart disables both installation and automatic updates. A fresh-namespace rerun verified
startup and the bundled Prometheus datasource with those settings applied from Helm.

This is a functional monitoring smoke test, not a load test or a dashboard screenshot review. Query
acceptance does not independently prove every aggregation under faults or low traffic. The test does
not measure real-data model accuracy, release latency, HPA readiness, or broker delivery. Redis and
publication are disabled in these fixture API processes; their inactive panels may have no data.

Reproduce using the [monitoring runbook](../runbooks/serving-monitoring.md#reproduce-the-isolated-smoke-test)
and [test](../../tests/e2e/test_monitoring_kind.py). The test creates its own Secret and namespace and
requests namespace/PVC cleanup in `finally`; the existing platform release is not upgraded.
