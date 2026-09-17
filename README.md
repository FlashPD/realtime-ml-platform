# Real-Time ML Platform

A local-first, production-shaped machine learning platform built on New York City taxi trip
data. It is designed to demonstrate the data-intensive side of senior AI engineering:
point-in-time-correct features, event-time stream processing, training/serving parity, guarded
model promotion, low-latency serving, closed-loop monitoring, and reproducible operations on
Kubernetes.

> **Status:** Initial prediction API milestone. The package, contracts, CI gates,
> bounded-memory bronze-to-silver ingestion, durable PostgreSQL lineage, Airflow 3 orchestration,
> leakage-safe dbt-duckdb gold features, deterministic model comparison, and explicit promotion
> gates are implemented. MLflow tracking, conditional registration, and production-alias protection
> are also complete. A local prediction API now serves the verified static model from the production
> bundle with explicit fallback metadata, health endpoints, and Prometheus metrics. Online features,
> prediction publication, and the serving deployment are next.

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
| Engineering documentation | Data card and eight ADRs covering infrastructure, ingestion, orchestration, feature correctness, reproducible promotion decisions, registry safety, and the serving boundary |

### Remaining

Estimates are focused engineer-days for one engineer and include implementation, tests, local
integration, and documentation. They are ranges rather than deadlines.

| Priority | Workstream | Definition of done | Estimate |
|---:|---|---|---:|
| 1 | Complete prediction service | Add Redis lookup and streaming inference to the initial API, prediction publication, authentication for any operator actions, Helm deployment, and HPA | 3–5 days |
| 2 | Event replay and stream processor | Event-time replayer, registered broker schemas, Bytewax windows and watermarks, late-event policy, Redis writes, checkpoint recovery, and service metrics | 6–8 days |
| 3 | Offline/online feature parity | Replay a fixture day, compare stream outputs with gold, report mismatch rate and maximum difference, and fail on skew | 1–2 days |
| 4 | Closed-loop evaluation | Prediction/completion joiner, durable error records, live MAE and coverage, Evidently drift report, and guarded retraining trigger | 4–6 days |
| 5 | Observability and integration hardening | Prometheus, Grafana, alerts, CI values profile, kind end-to-end workflow, dependency/image scanning, and serving load test | 4–6 days |
| 6 | Showcase and failure scenarios | Resumable harness, eight planned fault scenarios, objective assertions, raw exports, generated evidence README, and safe teardown | 6–8 days |
| 7 | Portfolio release polish | Runbooks, measured headline results, architecture and model evidence links, final limitations review, clean-laptop reproduction, and tagged release | 2–3 days |
|  | **Full remaining scope** | **Everything in the original architecture and acceptance plan** | **26–38 days** |

### Calendar view

| Target | Included outcome | Expected time |
|---|---|---:|
| Batch-serving portfolio release | Serving API on kind, basic dashboards, and a real-data model comparison | 7–11 engineer-days, roughly 1.5–2.5 full-time weeks |
| Differentiated streaming release | Batch-serving release plus replay, event-time features, recovery, and offline/online parity | 15–21 engineer-days, roughly 3–4.5 full-time weeks |
| Full planned platform | Closed-loop monitoring, all failure scenarios, complete evidence export, and release polish | 26–38 engineer-days, roughly 5–8 full-time weeks |

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
artifact checksums, feature order, and a warm-up prediction before the process becomes ready. Missing,
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

This initial API always uses the static-feature LightGBM model: `feature_fallback` is `true`,
`feature_timestamps` is empty, and `model_version` is `<training-run-id>-static`. The returned
`features_used` records the exact model inputs. Pickup time must include an offset; calendar features
convert to New York time and match gold's Sunday-zero hour-of-week convention. Distances must be
finite and nonnegative; invalid inputs return HTTP 422. Inference failures return HTTP 503.

`/healthz` reports process liveness, `/readyz` reports the loaded model and fallback mode, `/docs`
provides the interactive API contract, and `/metrics` exposes response counters, fallback counts, and
a latency histogram with a 50 ms bucket. Metrics are per process; the CLI runs one Uvicorn worker.
Latency objectives have **not** yet been measured under serving load.

The static sibling has held-out evaluation metrics but is not independently promotion-gated. Redis
lookup, streaming-model inference, broker publication, operator actions, and a Kubernetes serving
deployment are not implemented in this slice. Predictions currently exist only in the HTTP response;
they are not durably logged. See [ADR-0008](docs/adr/0008-initial-prediction-api.md) for these boundaries.

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
