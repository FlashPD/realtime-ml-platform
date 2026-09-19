# Platform development and operations guide

For measured release results and a short reviewer path, start with the [README](../README.md).
Run commands from the repository root.

## Quick start

Requires Python 3.12.

On macOS, LightGBM also requires the OpenMP runtime:

```bash
brew install libomp
```

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
make check
```

Validate the platform configuration and inspect its deterministic fingerprint:

```bash
tripml config validate
```

Export the versioned contracts that will be registered with the Redpanda schema registry:

```bash
tripml contracts export --output build/contracts
```

Environment variables override YAML values using `TRIPML_` and double underscores for nested
fields. For example:

```bash
TRIPML_SERVING__P95_LATENCY_OBJECTIVE_MS=75 tripml config validate
```

## Batch ingestion

Download and process one official TLC yellow-taxi partition:

```bash
make ingest MONTH=2024-01
```

For deterministic development, validate an existing Parquet fixture without network access:

```bash
tripml ingest --month 2024-01 --source path/to/fixture.parquet
```

The ingestion path writes the source atomically to `data/bronze/` with a SHA-256 manifest, scans
it in bounded Arrow record batches, and publishes normalized silver Parquet only after the
partition gate passes. Invalid rows include per-rule flags under `data/quarantine/`; a rejected
partition preserves any previously accepted silver file. The JSON quality report records row
counts, rule-level violations, source checksum, output paths, and contract version.

Set `TRIPML_LINEAGE__DATABASE_URL` to persist each run and its normalized quality checks in
PostgreSQL. The migration is applied under an advisory lock, and run updates enforce explicit
`running` to `accepted`, `quarantined`, or `failed` transitions. The DSN is a secret setting: it is
excluded from configuration output and the configuration fingerprint.

```bash
TRIPML_LINEAGE__DATABASE_URL='postgresql://tripml:...@localhost:5432/tripml' \
  make ingest MONTH=2024-01
```

The optional real-database integration test uses an isolated test database:

```bash
TRIPML_TEST_DATABASE_URL='postgresql://tripml:...@localhost:5432/tripml_test' \
  pytest -m integration --no-cov
```

See the [data card](data-card.md) for source limitations and validation rules, and
[ADR-0002](adr/0002-transactional-bounded-memory-ingestion.md) for the implementation trade-offs.

## Point-in-time gold features

Build the training feature table after ingesting a partition:

```bash
make features MONTH=2024-01
```

The dbt-duckdb model computes 15- and 60-minute pickup-zone statistics and a 60-minute
dropoff-zone count from trips that completed strictly before each pickup. It reads the preceding
silver month when available so lookbacks remain correct at month boundaries. A completion at the
exact pickup timestamp is excluded, preventing target leakage from simultaneous events.
The `gold-features-v1` schema requires 900/3600-second windows; changing those durations requires a
new feature definition rather than reusing columns labelled `15m` and `60m`.

The tested table is atomically published to
`data/gold/yellow/month=YYYY-MM/training_features.parquet`. Its adjacent manifest records input and
output checksums, row counts, window configuration, configuration fingerprint, feature model
version, and build time. See
[ADR-0005](adr/0005-point-in-time-gold-features.md) for the event-ledger window design.
Monthly builds default to two DuckDB workers, a 2 GB buffer-manager limit and 8 GB of spill space
under the disposable build directory. Configure these through the `features` settings. Previous-month
inputs are pruned by completion time to retain the required boundary context. The DuckDB limit is
not a hard process-memory ceiling; see [ADR-0016](adr/0016-real-data-pilot-and-bounded-feature-builds.md).

## Model training and promotion gate

After building every configured training and holdout month, train the baseline and both model
candidates:

```bash
make train
```

The run compares a hierarchical median baseline, a static-feature LightGBM model, and a LightGBM
model augmented with rolling zone features. All models use the same held-out month. By default, the
command logs each path as a separate MLflow run, reads the current production alias for the
incumbent comparison, and registers the streaming candidate only when it satisfies the configured
MAE improvement, distance-bucket calibration, and single-row inference-latency gates. A rejection
leaves the registry and production alias unchanged. Use `tripml train --no-track` when only the
local, reproducible model bundle is needed.

The real-data pilot uses a separate January-training / February-holdout configuration while the
original strict March and April partitions remain quarantined. The new
[batch-release profile](../examples/training/batch-release.yaml) opts into versioned unknown-passenger
support for the completed January–March / April evaluation. Follow the
[release runbook](runbooks/nullable-passenger-release.md) to reproduce that split; the
[pilot runbook](runbooks/real-data-pilot.md) preserves the earlier experiment. Training evidence now includes
distance-bucket diagnostics for every model and static-model eligibility independently of the
selected candidate's promotion decision. Set `tracking.candidate_role: static` with a separate
registered model name to promote the batch model explicitly. Evidence version 3 introduced that
choice in the manifest, model card and bundle identifier. See the
[static pilot configuration](../examples/training/static-pilot.yaml) and
[promotion runbook](runbooks/static-model-promotion.md). Incumbent comparisons require the
same role and exact holdout bytes; stale reports cannot replace an uncompared incumbent.

Evidence version 4 also records training/holdout missing passenger counts and separate known/unknown
cohort errors. Contract-1.1 gold preserves null counts for LightGBM's native missing-value routing.
Unknown-count API requests require `schema_version: "1.1"` and `passenger_count: null`; older models
reject them with HTTP 422. New-model predictions use schema 1.1. See
[ADR-0018](adr/0018-unknown-passenger-counts.md) for the data and consumer migration boundary.

Each content-addressed run under `artifacts/training/<run-id>/` contains the native LightGBM model
files, serialized baseline, evaluation report, input checksums, artifact checksums, promotion
decision, and generated model card. Repeating an identical run reuses that immutable bundle. See
[ADR-0006](adr/0006-reproducible-training-and-promotion.md) for the evaluation and artifact
boundary and [ADR-0007](adr/0007-guarded-mlflow-registry.md) for tracking and registry safety.

Without configuration overrides, MLflow uses a repository-local SQLite backend and filesystem
artifact store under `artifacts/mlflow/`. The kind profile instead runs MLflow with PostgreSQL
metadata and server-proxied artifacts in MinIO.

## Prediction API

Install the serving runtime and start the API after a successful tracked training run:

```bash
python -m pip install -e '.[serving]'
tripml serve                         # localhost:8000; resolves the production alias at startup
```

For development without MLflow, explicitly select a bundle produced by `tripml train --no-track`:

```bash
tripml serve --bundle artifacts/training/<run-id>
```

Local-bundle mode accepts unpromoted models and reports a null registry version. The default registry
mode requires a finished, promoted run of the selected role. A static release loads only its
approved static model and reports `mode=static_primary`; Redis configuration cannot switch it to
streaming. A streaming release also loads its matching static sibling. Startup verifies the loaded
models' checksums, feature order, and warm-up predictions before the process becomes ready. Missing,
corrupt, or inconsistent artifacts abort startup. The alias is resolved once; restart the process to
adopt a different version. Registry availability is not a request-time dependency.

```bash
curl --fail http://127.0.0.1:8000/v1/eta \
  -H 'Content-Type: application/json' \
  -d '{"trip_id":"demo-001","pickup_zone_id":161,"dropoff_zone_id":236,
       "pickup_time":"2024-04-15T12:00:00-04:00","trip_distance_miles":3.2,
       "passenger_count":2}'
curl --fail http://127.0.0.1:8000/readyz
curl --fail http://127.0.0.1:8000/metrics
```

Without Redis configuration, the API uses the static-feature LightGBM model: `feature_fallback` is
`true`, `feature_timestamps` is empty, and `model_version` is `<training-run-id>-static`. This also
applies to an intentional static release: the version-1 field and fallback counter indicate static
feature usage, so interpret them alongside readiness mode. The returned
`features_used` records the exact model inputs. Pickup time must include an offset; calendar features
convert to New York time and match gold's Sunday-zero hour-of-week convention. Distances must be
finite and nonnegative; invalid inputs return HTTP 422. Inference failures return HTTP 503.

`/healthz` reports process liveness, `/readyz` reports the loaded model and fallback mode, `/docs`
provides the interactive API contract, and `/metrics` exposes response counters, fallback counts, and
a latency histogram with a 50 ms bucket. Metrics are per process; the CLI runs one Uvicorn worker.
Representative cluster latency objectives have **not** yet been established. The benchmark below
provides the measurement path; a synthetic local run alone does not validate those objectives.

For a streaming release, enable online feature reads by pointing the API to a Redis instance
populated with the snapshot contract described in [ADR-0009](adr/0009-online-feature-serving.md):

```bash
TRIPML_SERVING__REDIS_URL='redis://localhost:6379/0' tripml serve
```

Redis credentials can be supplied in that secret URL, which is excluded from configuration output.
The client performs one `MGET` for pickup 15-minute, pickup 60-minute, and dropoff 60-minute snapshots.
All must have matching cutoffs, correct identities and feature versions, valid source timestamps,
and cutoffs no later than pickup and no more than 600 seconds behind it. Freshness uses **pickup event
time**, so historical replay does not become stale solely because it runs today. The default connect
and socket timeouts are each 10 ms with retries disabled; these are socket limits, not a measured
end-to-end request deadline.

Accepted snapshots select `<training-run-id>-streaming`, return `feature_fallback=false`, and record
each rolling feature's exclusive window-end timestamp. Missing, stale, future, inconsistent, corrupt,
or unavailable snapshots select the static model. Empty pickup windows also fall back because their
gold means are null; an empty dropoff window supplies a valid zero count. `/readyz` reports configured
mode and both model versions; Redis failure does not remove a service capable of static inference
from readiness. The `tripml_feature_lookup_total{outcome=...}` counter and
`tripml_feature_age_seconds` histogram make selection reasons and accepted snapshot lag observable.

The Redis protocol test runs automatically in CI. Run it locally against a **dedicated disposable
database**; it writes and removes the documented feature keys:

```bash
TRIPML_TEST_REDIS_URL='redis://127.0.0.1:6379/15' \
  pytest tests/integration/test_online_redis.py --no-cov
```

In streaming mode, the static sibling has held-out evaluation metrics but is not independently
promotion-gated. An explicit static release is gated on its own metrics. The
stream producer and operator actions remain pending. Lagged
Redis snapshots have not yet passed offline/online parity tests. See
[ADR-0008](adr/0008-initial-prediction-api.md) for the original serving boundary and ADR-0009
for the online read contract.

### Prediction publication

Set `TRIPML_PUBLICATION__BOOTSTRAP_SERVERS` to enable acknowledged delivery to the `predictions`
topic. The topic must already exist; the producer disables automatic creation. For a broker reachable
from the API at `localhost:19092`, provision it with `rpk` and start serving:

```bash
rpk topic create predictions --partitions 3 --replicas 1 -X brokers=localhost:19092
TRIPML_PUBLICATION__BOOTSTRAP_SERVERS=localhost:19092 tripml serve
```

The local client uses plaintext Kafka, matching the private local Redpanda profile. Use a broker's
advertised address reachable from the API process; a Kubernetes port-forward alone does not rewrite
Kafka metadata. The serving chart uses internal service addresses; external broker authentication
is separate follow-up work.

Every successful request in this mode has received a broker delivery acknowledgment. The message is
the same `Prediction` JSON returned over HTTP, keyed by UTF-8 `trip_id`, with schema-version and
prediction-ID headers. Both streaming and static-fallback predictions are published. HTTP 200 carries
`X-TripML-Publication: acknowledged`. Without broker configuration, the explicit model-only mode
carries `X-TripML-Publication: disabled`; `/readyz` also exposes the configured publication mode.

Queue rejection, producer failure, or an acknowledgment timeout returns HTTP 503 with a prediction
ID and delivery outcome, rather than returning an unlogged prediction as success. A timeout or
delivery error can still mean the event reached the broker; `delivery_unknown` makes that ambiguity
explicit. Retrying HTTP creates a new prediction ID. Kafka producer idempotence covers transport
retries, **not** HTTP retry deduplication or exactly-once evaluation. Consumers should deduplicate
by prediction ID and apply an explicit policy when one trip has multiple predictions.

The default producer delivery timeout is 1 second and the request acknowledgment wait is 1.5 seconds.
These are failure bounds, not measured serving latency. Queue capacity is 10,000 messages and 10 MiB.
Metrics include `tripml_prediction_publication_total{outcome=...}` and
`tripml_prediction_publication_duration_seconds`. Shutdown stops admission and attempts a bounded
flush; unconfirmed messages are logged. There is no durable application outbox or PostgreSQL
prediction log yet. The local broker has one replica, so its acknowledgment provides no redundancy
against loss of that broker's storage.

The broker integration test creates and removes a unique test topic and compares a consumed event
with its HTTP response. CI starts an isolated Redpanda instance. To run it against a disposable broker:

```bash
TRIPML_TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
  pytest tests/integration/test_prediction_publication.py --no-cov
```

See [ADR-0010](adr/0010-acknowledged-prediction-publication.md) for the delivery boundary.

### Deploying the API on kind

Serving is disabled during the initial infrastructure install because a production model does not
exist yet. Bring the infrastructure up to date with `make cluster`, then run the training workflow
against **the cluster's MLflow server** (for example through the Airflow training DAG) until a model
passes the promotion gate. A model registered in the default host-local SQLite store is not visible
to cluster serving.

```bash
make serving-deploy
make serving-ui       # localhost:8000; runs until interrupted
```

The deployment command builds and loads an image tagged with its Docker image ID, preserves prior
Helm overrides, enables serving, waits for readiness, and runs a prediction smoke test. Helm rolls
back a failed rollout. A missing or invalid production model prevents the new pod from becoming
ready. An init container creates the configured `predictions` topic or verifies its partition and
replica counts; it does not modify an incompatible existing topic. Serving uses cluster MLflow,
Redis credentials from the existing Secret, and acknowledged publication to cluster Redpanda.
The smoke test publishes a real event with trip ID `helm-serving-smoke`, which evaluation consumers
must exclude from business metrics.

The API runs as UID 10001 with a read-only root filesystem, a writable temporary volume, no service
account token, and dropped Linux capabilities. Startup/readiness probes call `/readyz`; liveness
calls `/healthz`. The Service is internal-only. Model changes require a rolling restart after the
registry alias changes. There is no unauthenticated HTTP reload endpoint.

CPU autoscaling is optional and requires an available Metrics Server. For **local kind only**, this
pinned upstream chart can provide it; the kubelet TLS exception is specific to the local cluster:

```bash
make metrics-server
TRIPML_SERVING_AUTOSCALING=true make serving-deploy
```

The HPA defaults to 1–3 replicas at 70% of requested CPU, with a five-minute scale-down stabilization
window. With HPA enabled, the chart omits Deployment `replicas` so upgrades do not reset its target.
The deployment command checks metrics availability before enabling it. The isolated load test below
measures CPU scaling and synthetic serving latency. Request-rate autoscaling remains future work.
The approved static release has separate [representative traffic evidence](validation/approved-model-operations.md).
Opt-in Prometheus and Grafana use per-pod discovery; see the
[monitoring runbook](runbooks/serving-monitoring.md). A ServiceMonitor is not required by this profile.

An optional kind test seeds a synthetic model into a temporary MLflow deployment, installs the
serving chart in the same temporary namespace, and verifies an acknowledged HTTP prediction. It
reads the existing local platform's Redis credential, reuses its Redis and Redpanda services, and
removes its namespace and unique broker topic afterward. It never moves the platform registry alias:

```bash
docker build -f docker/serving/Dockerfile -t tripml-serving:check .
.tools/bin/kind load docker-image tripml-serving:check --name tripml
TRIPML_TEST_KIND_CONTEXT=kind-tripml TRIPML_TEST_SERVING_IMAGE=tripml-serving:check \
  pytest tests/e2e/test_serving_kind.py --no-cov
```

See [ADR-0011](adr/0011-kubernetes-serving-deployment.md) for deployment and validation boundaries.

### In-cluster load and CPU autoscaling

The [recorded synthetic validation](validation/kind-serving-load.md) passed 18,000 requests
at 100 requests/second with P95 latency of 8.59 ms, zero errors or fallback, and observed scaling
from one to three ready replicas and back to one. The report includes P99 latency, a scaling chart,
configuration, failed-attempt history, and the limits of the measurement.

With the local kind platform running, use a new evidence directory and a freshly built image:

```bash
make metrics-server
docker build -f docker/serving/Dockerfile -t tripml-serving:load-check .
.tools/bin/kind load docker-image tripml-serving:load-check --name tripml
make serving-load-test PYTHON=.venv/bin/python \
  IMAGE=tripml-serving:load-check OUTPUT=artifacts/benchmarks/kind-load-001
```

The test extends the existing registry-to-serving deployment check. Its load profile uses a new
Redis instance and credentials in the temporary namespace. It reuses the local platform's broker
through a unique topic; platform Redis keys and the production registry alias are untouched. A load
pod seeds synthetic online snapshots and sends **18,000 requests at 100 requests/second** to the
serving Service, with acknowledged publication. The default objectives are P95 below 50 ms, at most
1% errors, and fallback below 1%. The load client permits 256 in-flight requests to cover the
100 requests/second × 2-second timeout window; every dropped arrival still fails the run.

This profile disables HTTP keep-alive, opening a new connection per request so traffic can reach
newly ready replicas. Connection overhead is included in latency; the general benchmark retains
keep-alive unless `--no-keepalive` is supplied. Both client timing and its resource limits are exported.

The HPA uses the chart defaults: a 100m CPU request, a one-core limit, a 70% CPU target, 1–3 replicas,
and a **300-second scale-down stabilization window**. The harness requires a one-replica baseline
with CPU metrics, an HPA scale-up request, additional ready capacity during load, successful traffic
on multiple pods, and a return to one ready/desired replica after traffic stops. It allows up to
eight minutes for scale-down, so expect roughly 10–15 minutes including setup and cleanup.

The output includes a generated README and scaling chart, separate workload/scaling gates, raw HPA and
Metrics API samples, pod image IDs, per-pod serving metrics, model artifacts, the actual load script
and snapshots, cluster/node information, Helm values, and Kubernetes events. Failed checks preserve
diagnostics. Cleanup removes the unique topic and temporary namespace/PVC; Metrics Server remains
installed as a cluster prerequisite. `KIND_CONTEXT` defaults to `kind-tripml`, and the test rejects
contexts without the `kind-` prefix. The infrastructure namespace/release overrides from the smoke
test also apply.

These are short synthetic measurements on a shared, single-node cluster. They do not establish
real-data accuracy, feature parity, production capacity, high availability, or request-rate scaling.
See [ADR-0013](adr/0013-in-cluster-load-and-cpu-autoscaling.md) for evidence boundaries.

### Measuring serving under load

Install the lightweight load-client extra and benchmark a running API:

```bash
python -m pip install -e '.[benchmark]'
make benchmark BENCHMARK_ARGS='--output artifacts/benchmarks/first-run --expected-features static'
```

The default is 1,000 requests at 100 requests/second, at most 32 in flight, 20 sequential warm-up
requests, a 2-second total request timeout, successful-request P95 below 50 ms, and at most 1% errors.
It expects acknowledged prediction publication. For a local API with publication deliberately
disabled, add `--expected-publication disabled`. A passing run exits zero; any failed objective
exits one. Use a **new output directory** for each run; previous evidence is never overwritten.

The included three-row fixture is synthetic and uses one zone pair at a fixed historical pickup
time. It is suitable for checking the measurement path, not representing NYC traffic. Generate a
sampled real-data workload from an already accepted silver month:

```bash
make benchmark-workload MONTH=2024-01 OUTPUT=artifacts/workloads/tlc-jan \
  WORKLOAD_ARGS='--rows 10000 --seed 42'
```

The builder scans the entire partition in bounded batches, samples uniformly without replacement,
and exports `requests.jsonl`, `manifest.json`, the ingestion quality report, and a README. It records
input/output checksums and compares sample/population distributions for pickup hour of week, pickup
and dropoff zones, distance, and passengers. The same input, seed, and Python version reproduce the
fixture independently of batch size. Historical New York offsets are preserved; ambiguous or
nonexistent daylight-saving pickup times are excluded and counted. Invalid eligible rows fail the
build. The [January validation](validation/real-data-workload.md) sampled 10,000 requests from
2,757,364 accepted trips. See [ADR-0014](adr/0014-real-data-serving-workloads.md) for trade-offs.

Pass the manifest to preserve verified workload provenance in the benchmark evidence. Against a
running API configured for static serving and acknowledged publication:

```bash
tripml benchmark --requests-file artifacts/workloads/tlc-jan/requests.jsonl \
  --workload-manifest artifacts/workloads/tlc-jan/manifest.json \
  --requests 10000 --rate 100 --expected-features static \
  --output artifacts/benchmarks/tlc-jan-static
```

The manifest digest must match the request fixture before traffic is sent. This samples request
mix; the benchmark still uses constant arrivals and replaces trip IDs. Release accuracy and
representative cluster latency are measured separately in the [release evidence](batch-serving-release.md).
Historical pickups also require matching
event-time online features for a streaming-mode claim; the existing single-zone synthetic Redis
fixtures cannot establish that claim. With those features prepared, use explicit expectations:

```bash
tripml benchmark --base-url http://127.0.0.1:8000 \
  --requests-file path/to/representative-requests.jsonl \
  --requests 10000 --rate 100 --concurrency 32 \
  --expected-features streaming --expected-publication acknowledged \
  --label online-features --output artifacts/benchmarks/online-features
```

The scheduler continues offering arrivals at the requested rate when responses slow down. If its
concurrency bound is reached, it records a dropped arrival and fails the run. P95 includes client
scheduling delay, the HTTP exchange, and prediction validation; separate HTTP and dispatch-lag
percentiles help identify a saturated load generator. Percentiles use exact nearest ranks. Errors,
timeouts, malformed predictions, wrong trip IDs, unexpected feature modes, and mismatched publication
headers do not count as successful predictions. Error rate includes every scheduled arrival;
warm-up samples are saved separately and excluded from measured gates.

Each directory contains a generated `README.md`, `summary.json` with gates and SHA-256 digests,
normalized `requests.jsonl`, `config.json`, and raw measured/warm-up samples. Samples include model
versions, prediction IDs, serving modes, status, and timing, allowing results to be audited.
Requests cycle through the fixture with unique `benchmark-<run-id>-...` trip IDs; **both warm-up and
measured requests publish real events when publication is enabled**. Evaluation consumers must
exclude this prefix. Use a dedicated benchmark deployment/topic for isolated failure experiments.

For Redis degradation, benchmark the same workload against an isolated API whose Redis is
unavailable and set `--expected-features static`; inspect its feature-lookup metrics to verify the
failure cause. For recovery, restore that dependency and rerun with `--expected-features streaming`.
The client never changes services or injects faults itself. Broker failure should produce a failed
benchmark with HTTP errors because the API requires acknowledgment before success.

To automate these scenarios with **disposable dependencies**, install the development extra, start
Docker, and run:

```bash
make benchmark-resilience PYTHON=.venv/bin/python OUTPUT=artifacts/benchmarks/resilience-001
```

This opt-in test creates separate Redis and Redpanda containers on loopback-only ports, trains a
small synthetic native-model bundle, and starts one host API process. It uses no existing platform
credentials, services, model aliases, or topics. The same API process stays running through:

| Scenario | Required behavior |
|---|---|
| Healthy dependencies | Acknowledged predictions, zero measured errors, P95 below 50 ms, static fallback below 1% |
| Redis stopped | Static predictions with `unavailable` lookup metrics and acknowledged publication |
| Redis restarted and snapshots reseeded | Streaming predictions resume without an API restart |
| Broker stopped | All measured requests return HTTP 503; the API remains live and model-ready |
| Broker restarted | Streaming predictions and acknowledged publication resume without an API restart |

Each available-service phase sends 1,000 measured requests at 100 requests/second plus 20 warm-up
requests. The broker-outage phase sends 10 requests at 5 requests/second, with no warm-up. Its raw
benchmark is expected to fail; the suite passes that scenario only if every response is HTTP 503.
The suite independently consumes the topic after API shutdown and checks every acknowledged
prediction, including warm-up, against its response digest, trip key, model, fallback mode, and
contract headers. Missing events, duplicate prediction IDs, or mismatched content fail validation.
Events associated with unsuccessful HTTP requests are reported separately as ambiguous delivery.

Online phases allow static fallback below the architecture's 1% objective while requiring every
measured HTTP prediction to succeed. The suite checks that lookups are either fresh or unavailable;
invalid/stale seeded features fail validation. A transient Redis timeout can therefore produce a
valid, acknowledged fallback without making the run fail. The observed fallback rate remains in
the evidence and reaches failure at 1%; the Redis-outage phase instead requires 100% static fallback
and the `unavailable` metric cause. Redis timeout settings are unchanged by the harness.

The top-level evidence README links to scenario reports, raw samples, server metrics, logs, native
model artifacts, seeded snapshots, and consumer output. The summary includes container configuration,
image IDs through Docker inspect exports, source hashes, and artifact checksums. The test removes its
own containers and anonymous volumes in cleanup and retains evidence. A setup failure can leave
partial diagnostics without a completed summary. The **Serving resilience evidence** GitHub Actions
workflow runs the same suite on manual dispatch and uploads artifacts even if a check fails.

These are synthetic host-to-container measurements. Snapshots are manually seeded and Redis is
reseeded on recovery; the suite does not establish stream-processor correctness or recovery. It also
does not establish real-data accuracy, in-cluster latency, sustained capacity, or HPA behavior.

Record hardware, resource limits, model provenance, dependency state, and network path alongside
the results. A host port-forward run includes that forwarding path and does not prove in-cluster
capacity or HPA behavior. For those measurements, run the client in-cluster against the Service and
record replica/CPU observations during a sustained run. Model accuracy, feature parity, failure
recovery automation, and autoscaling remain separate validations. See
[ADR-0012](adr/0012-serving-load-evidence.md) for measurement trade-offs.

## Airflow orchestration

The manually triggered `tripml_ingestion` and `tripml_training` DAGs run the same
orchestrator-independent workflows as the CLI. Ingestion accepts a `month` parameter, defaulting to
`2024-01`, and an optional worker-visible `config_path`. It requires
`TRIPML_LINEAGE__DATABASE_URL`, limits ingestion to 20 minutes and the downstream gold build to 15
minutes, permits only one active run, and uses bounded retries. A partition rejected by its quality
gate fails without retrying and points operators to its durable lineage run; gold is built only
after acceptance. Training allows one active two-hour DAG run, gives the model task a 90-minute
timeout, and publishes through the guarded MLflow workflow.

Airflow discovers thin entry points in `dags/`; the implementations live in the installable
`tripml.airflow_dags` package so they can be type-checked and unit-tested. To exercise both DAGs
outside Kubernetes, install their runtime dependencies with:

```bash
python -m pip install -e '.[orchestration,tracking,training,transformation]'
```

The local cluster builds an immutable Airflow image containing the project and runs the API server,
scheduler with a two-worker LocalExecutor, and required DAG processor as separate containers. It
uses a dedicated PostgreSQL metadata database, persistent logs and ingestion data, generated
Fernet/JWT/UI credentials, resource boundaries, and component health probes. Access it after
`make cluster`:

```bash
make airflow-password # username: admin
make airflow-ui       # http://localhost:8080; runs until interrupted
make mlflow-ui        # http://localhost:5000; runs until interrupted
```

The repository tests parse the DAG and exercise its retry and quality-failure behavior without a
scheduler; the Helm smoke test verifies the deployed Airflow health endpoint. See
[ADR-0003](adr/0003-atomic-airflow-ingestion-task.md) for the task boundary and
[ADR-0004](adr/0004-local-airflow-deployment.md) for the Kubernetes topology.

## Local infrastructure

Docker and `kubectl` are the only global prerequisites. The repository downloads checksum-
verified, pinned kind and Helm binaries into the ignored `.tools/` directory.

```bash
make helm-lint       # render and validate the chart without a cluster
make cluster         # create kind and install the infrastructure
make cluster-test    # rerun data-service, broker, Airflow, and MLflow smoke tests
make cluster-status # inspect workloads, PVCs, and service endpoints
make cluster-delete # remove the cluster and its local data
```

The local chart currently provisions single-node Redpanda, PostgreSQL, Redis, MinIO, Airflow, and
MLflow with health probes, resource requests and limits, persistent state where required, and
credentials generated at cluster creation. Secrets are never written to the repository or Helm
values. MLflow metadata uses PostgreSQL while artifacts are proxied to a dedicated MinIO bucket.
MinIO is pinned to its final official community image because upstream moved to source-only
distribution; that trade-off is intentionally limited to the zero-cost local profile. Airflow's
simple auth manager is likewise limited to the private development cluster; a shared deployment
requires a production auth manager.

The packaging rationale and production boundary are recorded in
[ADR-0001](adr/0001-local-infrastructure-packaging.md).

## Engineering principles

- Event and table boundaries have explicit, versioned contracts.
- Event time—not arrival time—drives streaming windows.
- Offline features are point-in-time correct and tested against online features.
- A candidate model must pass declared quality and latency gates before promotion.
- Failure recovery and observability are product behavior, not follow-up work.
- Every portfolio claim will link to reproducible evidence from a showcase run.
