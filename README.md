# Real-Time ML Platform

A local-first, production-shaped machine learning platform built on New York City taxi trip
data. It is designed to demonstrate the data-intensive side of senior AI engineering:
point-in-time-correct features, event-time stream processing, training/serving parity, guarded
model promotion, low-latency serving, closed-loop monitoring, and reproducible operations on
Kubernetes.

> **Status:** Serving load and autoscaling milestone. The package, contracts, CI gates,
> bounded-memory bronze-to-silver ingestion, durable PostgreSQL lineage, Airflow 3 orchestration,
> leakage-safe dbt-duckdb gold features, deterministic model comparison, and explicit promotion
> gates are implemented. MLflow tracking, conditional registration, and production-alias protection
> are also complete. The prediction API selects the verified streaming model when Redis snapshots
> pass event-time checks, with explicit static fallback, health endpoints, and Prometheus metrics.
> Configured prediction publication now waits for Redpanda acknowledgment before HTTP success.
> Serving now has a non-root image, Helm deployment, health probes, topic provisioning, and an
> optional CPU HPA. A constant-arrival HTTP benchmark now exports request-level evidence and enforces
> latency, error, overload, and serving-mode checks. An isolated resilience suite exercises Redis and
> broker failures, recovery, and consumer readback. An in-cluster synthetic load test measures Service
> latency, traffic across replicas, CPU scale-up, and the default scale-down stabilization window.
> Real-data request workloads can now be sampled reproducibly from accepted silver partitions,
> with distribution comparisons and checksummed provenance preserved by the benchmark runner.
> Representative real-data load measurements, the stream producer, and the ground-truth joiner remain
> pending.

## Intended architecture

```mermaid
flowchart LR
    TLC[NYC TLC Parquet] --> B[Bronze / Silver / Gold]
    B --> TRAIN[Training + promotion gate]
    TRAIN --> REG[(MLflow registry)]
    REG --> API[Prediction API]
    B --> REPLAY[Event-time replayer]
    REPLAY --> BROKER[(Redpanda)]
    BROKER --> STREAM[Stream processor]
    STREAM --> REDIS[(Redis features)]
    REDIS --> API
    API --> PRED[Predictions]
    BROKER --> JOIN[Ground-truth joiner]
    PRED --> JOIN
    JOIN --> MONITOR[Error + drift monitoring]
    MONITOR --> TRAIN
```

The complete design, delivery phases, service objectives, and acceptance criteria are in the
[architecture plan](arch_plan/realtime-ml-platform-plan.md).

## Delivery status and roadmap

The active target is the **[batch-serving portfolio release](docs/batch-serving-release.md)**.
Its checklist separates release requirements from the later streaming roadmap. Local serving
visibility is now available through an opt-in Prometheus/Grafana profile; see the
[monitoring runbook](docs/runbooks/serving-monitoring.md). Real-data model comparison, static-model
promotion semantics, representative load evidence, and final reproduction remain release gates.

The batch path is complete through a guarded MLflow production alias. "Complete" below means
implemented, documented, and covered by the repository quality gates; it does not mean that a final
showcase run has measured the production-shaped objectives yet.

### Complete

| Workstream | Delivered evidence |
|---|---|
| Foundation and contracts | Installable package, strict configuration, versioned Pydantic event contracts, Ruff, mypy, pytest coverage gate, and CI workflow |
| Local platform foundation | kind bootstrap and Helm-managed Redpanda, PostgreSQL, Redis, MinIO, Airflow, and MLflow with persistence, probes, resource boundaries, and smoke tests |
| Bronze-to-silver ingestion | Bounded-memory validation, atomic publication, named quality checks, quarantine behavior, source manifests, and stable trip identifiers |
| Durable ingestion lineage | PostgreSQL migrations, explicit run-state transitions, normalized quality results, and an optional real-database integration test |
| Airflow batch orchestration | Quality-gated ingestion followed by gold generation, bounded retries and timeouts, concurrency policy, and DAG component tests |
| Point-in-time gold features | dbt-duckdb completion-time windows, prior-month boundary context, no-leakage tests, atomic Parquet output, and checksummed manifests |
| Reproducible training core | Hierarchical median baseline, static and streaming-feature LightGBM candidates, held-out metrics, calibration and latency gates, native model artifacts, integrity verification, and generated model cards |
| Guarded experiment tracking | Separate MLflow runs for all three model paths, checksummed lineage and evidence, idempotent publication, registration only after every gate passes, comparison with the current production model, and rollback-safe production aliases |
| Scheduled model lifecycle | Manually triggered Airflow training DAG with bounded retries, execution timeout, one active run, and the same tracking workflow used by the CLI |
| Initial prediction API | Verified MLflow or local-bundle loading, native static-model inference, New York calendar parity, explicit fallback provenance, health/readiness, Prometheus metrics, and a training-to-registry-to-HTTP integration test |
| Online-feature serving | Versioned zone-role Redis keys, validated exclusive-end snapshots, streaming inference, socket timeouts without retries, lookup-outcome metrics, static degradation, and a real-Redis integration test in CI |
| Prediction publication | JSON contract events keyed by trip ID, broker acknowledgment before HTTP success, idempotent Kafka producer, bounded queue/waits, delivery metrics, and a real HTTP-to-Redpanda round-trip test in CI |
| Kubernetes serving | Non-root image, opt-in Helm deployment, registry/Redis/broker wiring, idempotent topic provisioning, resource limits, probes, rolling updates, optional CPU HPA, and an isolated kind smoke test |
| Serving benchmark tooling | Constant-arrival load, bounded concurrency, explicit dropped arrivals, validated predictions and serving modes, objective exit codes, checksummed raw evidence, and failure-path tests |
| Serving resilience harness | Disposable Redis/Redpanda, real HTTP and native-model inference, healthy online load, Redis/broker outages and recovery in one API process, metrics assertions, consumer readback, and manual CI evidence export |
| In-cluster load and HPA harness | Temporary registry and Redis, a Service-addressed load pod, raw CPU/HPA/readiness observations, per-pod traffic evidence, default stabilization, model artifact export, and explicit workload/scaling gates |
| Real-data request workloads | Bounded-memory sampling of accepted TLC silver, deterministic request fixtures, explicit DST exclusions, source/sample distributions, source checksums, and benchmark provenance verification |
| Local serving monitoring | Opt-in Prometheus with per-pod discovery, namespace-scoped RBAC, authenticated Grafana, provisioned serving dashboard, bounded retention, and an isolated kind smoke test |
| Engineering documentation | Data card, batch-release checklist, monitoring runbook, and fifteen ADRs covering infrastructure, ingestion, orchestration, feature correctness, reproducible promotion decisions, registry safety, serving, delivery semantics, deployment, load measurement, autoscaling evidence, real-data workloads, and monitoring |

### Remaining

Estimates are focused engineer-days for one engineer and include implementation, tests, local
integration, and documentation. They are ranges rather than deadlines.

| Priority | Workstream | Definition of done | Estimate |
|---:|---|---|---:|
| 1 | Representative load and request-rate scaling | Use the real-data workload builder with a real-data model and matching online features to measure cluster traffic and dependency degradation; add request-rate scaling with monitoring; authenticate any future operator endpoints | 1–2 days |
| 2 | Event replay and stream processor | Event-time replayer, registered broker schemas, Bytewax windows and watermarks, late-event policy, Redis writes, checkpoint recovery, and service metrics | 6–8 days |
| 3 | Offline/online feature parity | Replay a fixture day, compare stream outputs with gold, report mismatch rate and maximum difference, and fail on skew | 1–2 days |
| 4 | Closed-loop evaluation | Prediction/completion joiner, durable error records, live MAE and coverage, Evidently drift report, and guarded retraining trigger | 4–6 days |
| 5 | Observability and integration hardening | Prometheus, Grafana, alerts, CI values profile, kind end-to-end workflow, dependency/image scanning, and automated cluster load validation in CI | 4–6 days |
| 6 | Showcase and failure scenarios | Resumable harness, eight planned fault scenarios, objective assertions, raw exports, generated evidence README, and safe teardown | 6–8 days |
| 7 | Portfolio release polish | Runbooks, measured headline results, architecture and model evidence links, final limitations review, clean-laptop reproduction, and tagged release | 2–3 days |
|  | **Full remaining scope** | **Everything in the original architecture and acceptance plan** | **24–35 days** |

### Calendar view

| Target | Included outcome | Expected time |
|---|---|---:|
| Batch-serving portfolio release | Serving API on kind, basic dashboards, and a real-data model comparison | 7–11 engineer-days, roughly 1.5–2.5 full-time weeks |
| Differentiated streaming release | Batch-serving release plus replay, event-time features, recovery, and offline/online parity | 15–21 engineer-days, roughly 3–4.5 full-time weeks |
| Full planned platform | Closed-loop monitoring, all failure scenarios, complete evidence export, and release polish | 24–35 engineer-days, roughly 5–7 full-time weeks |

At approximately 15 hours per week, the full planned platform is roughly 3.5–5 months. The main
schedule risks are real-data performance tuning, Bytewax recovery behavior, Kubernetes resource
pressure on a 16 GB laptop, and integration debugging across the broker, registry, Redis, and
observability stack. Optional EKS/Terraform work remains outside these estimates and outside the
release scope.

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

See the [data card](docs/data-card.md) for source limitations and validation rules, and
[ADR-0002](docs/adr/0002-transactional-bounded-memory-ingestion.md) for the implementation trade-offs.

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
[ADR-0005](docs/adr/0005-point-in-time-gold-features.md) for the event-ledger window design.

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

Each content-addressed run under `artifacts/training/<run-id>/` contains the native LightGBM model
files, serialized baseline, evaluation report, input checksums, artifact checksums, promotion
decision, and generated model card. Repeating an identical run reuses that immutable bundle. See
[ADR-0006](docs/adr/0006-reproducible-training-and-promotion.md) for the evaluation and artifact
boundary and [ADR-0007](docs/adr/0007-guarded-mlflow-registry.md) for tracking and registry safety.

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
mode requires a finished, promoted streaming run and its matching static sibling. Startup verifies
both models' checksums, feature order, and warm-up predictions before the process becomes ready. Missing,
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
`true`, `feature_timestamps` is empty, and `model_version` is `<training-run-id>-static`. The returned
`features_used` records the exact model inputs. Pickup time must include an offset; calendar features
convert to New York time and match gold's Sunday-zero hour-of-week convention. Distances must be
finite and nonnegative; invalid inputs return HTTP 422. Inference failures return HTTP 503.

`/healthz` reports process liveness, `/readyz` reports the loaded model and fallback mode, `/docs`
provides the interactive API contract, and `/metrics` exposes response counters, fallback counts, and
a latency histogram with a 50 ms bucket. Metrics are per process; the CLI runs one Uvicorn worker.
Representative cluster latency objectives have **not** yet been established. The benchmark below
provides the measurement path; a synthetic local run alone does not validate those objectives.

Enable online feature reads by pointing the API to a Redis instance populated with the snapshot
contract described in [ADR-0009](docs/adr/0009-online-feature-serving.md):

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

The static sibling has held-out evaluation metrics but is not independently promotion-gated. The
stream producer and operator actions remain pending. Lagged
Redis snapshots have not yet passed offline/online parity tests. See
[ADR-0008](docs/adr/0008-initial-prediction-api.md) for the original serving boundary and ADR-0009
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

See [ADR-0010](docs/adr/0010-acknowledged-prediction-publication.md) for the delivery boundary.

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
measures CPU scaling and synthetic serving latency. Request-rate autoscaling and representative
traffic objectives remain pending. Prometheus scrape
annotations are included; a Prometheus installation and ServiceMonitor remain observability work.

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

See [ADR-0011](docs/adr/0011-kubernetes-serving-deployment.md) for deployment and validation boundaries.

### In-cluster load and CPU autoscaling

The [recorded synthetic validation](docs/validation/kind-serving-load.md) passed 18,000 requests
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
See [ADR-0013](docs/adr/0013-in-cluster-load-and-cpu-autoscaling.md) for evidence boundaries.

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
build. The [January validation](docs/validation/real-data-workload.md) sampled 10,000 requests from
2,757,364 accepted trips. See [ADR-0014](docs/adr/0014-real-data-serving-workloads.md) for trade-offs.

Pass the manifest to preserve verified workload provenance in the benchmark evidence. Against a
running API configured for static serving and acknowledged publication:

```bash
tripml benchmark --requests-file artifacts/workloads/tlc-jan/requests.jsonl \
  --workload-manifest artifacts/workloads/tlc-jan/manifest.json \
  --requests 10000 --rate 100 --expected-features static \
  --output artifacts/benchmarks/tlc-jan-static
```

The manifest digest must match the request fixture before traffic is sent. This samples request
mix; the benchmark still uses constant arrivals and replaces trip IDs. Real-data model accuracy
and representative cluster latency remain unmeasured. Historical pickups also require matching
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
[ADR-0012](docs/adr/0012-serving-load-evidence.md) for measurement trade-offs.

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
[ADR-0003](docs/adr/0003-atomic-airflow-ingestion-task.md) for the task boundary and
[ADR-0004](docs/adr/0004-local-airflow-deployment.md) for the Kubernetes topology.

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
[ADR-0001](docs/adr/0001-local-infrastructure-packaging.md).

## Engineering principles

- Event and table boundaries have explicit, versioned contracts.
- Event time—not arrival time—drives streaming windows.
- Offline features are point-in-time correct and tested against online features.
- A candidate model must pass declared quality and latency gates before promotion.
- Failure recovery and observability are product behavior, not follow-up work.
- Every portfolio claim will link to reproducible evidence from a showcase run.
