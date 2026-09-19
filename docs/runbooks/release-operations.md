# Measure the approved release under load and broker failure

Prerequisites: the [approved model deployment](deploy-approved-model.md), its content-identified
image loaded into `kind-tripml`, the original April workload, and
[serving monitoring](serving-monitoring.md) enabled in namespace `tripml`.
Run from the repository root with the development environment installed.

The harness is deliberately scoped to the approved `98ef1dd9ef4447f7-static` model, registry
version 1, and the recorded serving image. It does not train or promote models. To use a different
release, first review the identity constants and create a new measurement plan and evidence root.

## Declare and run healthy load

Retain a plan before measuring. The initial release profile is 10,000 sampled April requests,
100 arrivals/second, concurrency 32, keep-alive enabled, 20 excluded warm-up requests, and a
two-second request deadline. Its unchanged gates require client P95 below 50 ms, errors at most
1%, and no dropped arrivals. No client request is retried.

```bash
.venv/bin/python scripts/run-release-operations.py \
  --workload artifacts/workloads/april-passenger-v1_1 \
  --output artifacts/releases/my-operations-run/healthy
```

Each output path must be new. The harness creates a separate load-generator pod in `tripml`,
copies the checksummed workload, waits for healthy Prometheus discovery, and sends traffic to
the deployed Kubernetes Service. The load pod requests 250m CPU / 256 MiB and is limited to
one CPU / 512 MiB. Its requests include broker acknowledgment latency.

The harness exports client samples, summary gates, readiness and raw API metrics, Kubernetes
resources/CPU/memory observations, the actual Grafana dashboard definition and datasource health,
and range-query responses for every dashboard expression. It independently consumes broker records
and compares each successful response's prediction hash and envelope. Warm-up delivery is included
in readback; warm-ups remain excluded from client latency and throughput metrics.

## Run isolated broker failure and recovery

```bash
.venv/bin/python scripts/run-release-operations.py \
  --workload artifacts/workloads/april-passenger-v1_1 \
  --output artifacts/releases/my-operations-run/broker-failure \
  --failure
```

This creates a unique `tripml-broker-check-*` namespace with one serving replica, its own persistent
Redpanda broker, Prometheus and Grafana. It uses the same serving image, approved registry model,
resource limits and publication settings. Cluster MLflow and MinIO are shared read-only by the
serving process through namespace-local service aliases. The original serving deployment and
broker remain running. Redis is bypassed by the selected static-primary model.

The predeclared phases are:

| Phase | Traffic | Expected behavior |
|---|---|---|
| Baseline | 2,000 requests at 20/s; 20 warm-ups | Approved static predictions, acknowledgment, ordinary benchmark gates |
| Broker stopped | 120 requests at 2/s; no warm-ups | All HTTP 503 with publication `unconfirmed`; no client timeouts or dropped arrivals |
| Recovery | Probe for acknowledgment for up to 60 seconds after restoring the broker | The existing serving process reconnects; probe timings retained |
| Recovered load | 2,000 requests at 20/s; 20 warm-ups | Ordinary benchmark gates and complete acknowledged-event readback |

The broker is scaled down until its pod is gone, then restored with its persistent data. The
API pod UID, container identity, restart count and process start time must remain unchanged.
A separate outage probe checks the API's `delivery_unknown` response: a failed HTTP request must
not be interpreted as proof that an event can never reach the broker. Readback reports any such
ambiguous deliveries separately, along with mismatches and duplicates. This is not an exactly-once
delivery claim.

The outage phase's ordinary benchmark summary is expected to fail its success/error gates.
Its separate phase result checks the intended outage behavior; do not relabel the raw benchmark
as a passing healthy-load run.

## Evidence and cleanup

`result.json` is the overall verdict. Inspect the individual phase results even when the command
exits nonzero. `evidence/` contains the in-cluster raw artifacts; host logs and observation snapshots
remain alongside it. Query results can include empty series or NaN where a denominator is zero;
that is not evidence of zero errors. Successful Prometheus scrapes are checked explicitly.

After measurement, the harness allows two more scrape intervals before exporting monitoring.
It exports evidence before deleting its load pod or isolated namespace, including failed attempts.
If export fails, the driver/resources are retained for diagnosis and the output states that.
The failure harness attempts to restore its broker in `finally` before cleanup. Verify namespace
deletion after a run, and preserve any unsuccessful cleanup result. Main-platform monitoring and
serving remain installed.

Run the healthy workload and isolated failure suite sequentially on the shared laptop. Do not run
training, builds or repository tests during timed measurements. Record any other observed resource
contention and retain every attempt. A single-node local measurement does not establish production
capacity, high availability, streaming-feature parity, or live ETA accuracy.
