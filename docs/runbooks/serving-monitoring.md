# Serving monitoring on local kind

The opt-in chart profile installs Prometheus and Grafana with the **TripML / TripML Serving**
dashboard. Prometheus discovers individual serving pods in its release and namespace and scrapes
them every 15 seconds. Grafana includes traffic/status, server latency, errors, static fallback, feature lookups,
publication outcomes, scrape health, per-pod traffic, broker wait and accepted feature age panels.

## Enable

Use the existing local cluster and installed `tripml` release. Serving may be enabled later;
until serving pods exist, the dashboard correctly has no data. The additional default resource
requests are 150m CPU and 384 MiB memory, plus a 2 GiB Prometheus volume.

Create a separate admin Secret once. The generated password travels over stdin and is not written
to Helm values or a checked-in file:

```bash
python3 - <<'PY' | kubectl --context kind-tripml -n tripml create -f -
import json, secrets
print(json.dumps({"apiVersion": "v1", "kind": "Secret",
                  "metadata": {"name": "tripml-grafana-admin"},
                  "stringData": {"admin-password": secrets.token_urlsafe(32)}}))
PY

.tools/bin/helm upgrade tripml deploy/helm/tripml \
  --kube-context kind-tripml --namespace tripml --reset-then-reuse-values \
  --set monitoring.enabled=true --wait --timeout 5m
```

The `--reset-then-reuse-values` flag merges new chart defaults with the installed release settings.
Use `monitoring.grafana.adminSecret` for an existing Secret with an `admin-password` key.

Forward Grafana in one terminal and retrieve the password in another:

```bash
kubectl --context kind-tripml -n tripml port-forward service/tripml-tripml-grafana 3000:3000
kubectl --context kind-tripml -n tripml get secret tripml-grafana-admin \
  -o jsonpath='{.data.admin-password}' | base64 --decode
```

Open `http://localhost:3000/d/tripml-serving` and log in as `admin`. Services use ClusterIP;
there is no ingress or anonymous Grafana access. This is a private local profile, not a shared
multi-tenant monitoring deployment. Prometheus has read access only to pods in its namespace.

## Validate and interpret

```bash
kubectl --context kind-tripml -n tripml port-forward service/tripml-tripml-prometheus 9090:9090
```

At `http://localhost:9090/targets`, expect one healthy target per running serving pod. Run the
[benchmark](../../README.md#measuring-serving-under-load) for at least a minute so rate panels have multiple
scrapes; Grafana rates use a minimum interval of four scrapes. Save the exact time window with
any dashboard capture.

- **No data:** Check serving pods, `/metrics`, target discovery and scrape errors. No traffic makes
  ratios/quantiles undefined. Missing targets are not displayed as proof of zero errors.
- **100% fallback:** Expected in the batch release when valid online features are absent. Check
  feature lookup outcomes before calling it an incident. The deployed model is then the static sibling.
- **Latency:** Server histogram quantiles are estimates, aggregated across pods. They exclude
  client dispatch and network time; headline release latency comes from the client benchmark.
- **Scrape count:** Successful scrapes are not desired/ready replica counts. Use `kubectl get hpa,pods`
  and the existing load-test observations for autoscaling evidence.
- **Publication failures:** Check broker connectivity and API logs. HTTP success with configured
  publication requires acknowledgment; this dashboard does not prove downstream consumption.
- **Feature age:** Relative to pickup event time. It is not elapsed time since a wall-clock write.

Prometheus retains at most 24 hours or approximately 1 GB of TSDB blocks, whichever limit is reached
first; WAL and head data need additional space. Export release evidence separately. Grafana's local
database is ephemeral: pod replacement resets UI preferences and users to the configured admin.
Datasources and dashboards are restored from version-controlled files. Edit the dashboard JSON in
the chart and upgrade Helm to make permanent changes. Config checksums trigger pod replacement.
Runtime plugin installation and updates are disabled; the pinned image supplies the Prometheus plugin.

## Reproduce the isolated smoke test

Build and load a serving image into the existing kind cluster, then run:

```bash
docker build -f docker/serving/Dockerfile -t tripml-serving:monitoring-check .
.tools/bin/kind load docker-image tripml-serving:monitoring-check --name tripml
TRIPML_TEST_KIND_CONTEXT=kind-tripml \
TRIPML_TEST_SERVING_IMAGE=tripml-serving:monitoring-check \
TRIPML_TEST_MONITORING=1 .venv/bin/python -m pytest tests/e2e/test_monitoring_kind.py --no-cov -s
```

The test creates an isolated namespace, provisions monitoring, serves a synthetic trained model
in two API pods, and checks per-pod discovery, six collected HTTP successes, all dashboard queries,
and Grafana's datasource health. It requests namespace deletion in `finally`, including its test PVC.
It does not mutate the installed platform release or establish real-data performance.
See the [recorded validation](../validation/serving-monitoring.md) for the initial observed result
and its limits.

## Disable

```bash
.tools/bin/helm upgrade tripml deploy/helm/tripml \
  --kube-context kind-tripml --namespace tripml --reuse-values \
  --set monitoring.enabled=false --wait --timeout 5m
```

Disabling removes the monitoring workloads and their chart-managed PVC, deleting local metric
history with the default storage class. Export needed evidence first. The separately created admin
Secret remains. Do not delete the cluster to remove monitoring.
