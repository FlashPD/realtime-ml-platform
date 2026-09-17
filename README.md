# Real-Time ML Platform

A local-first, production-shaped machine learning platform built on New York City taxi trip
data. It is designed to demonstrate the data-intensive side of senior AI engineering:
point-in-time-correct features, event-time stream processing, training/serving parity, guarded
model promotion, low-latency serving, closed-loop monitoring, and reproducible operations on
Kubernetes.

> **Status:** Reproducible training milestone. The package, contracts, CI gates,
> bounded-memory bronze-to-silver ingestion, durable PostgreSQL lineage, Airflow 3 orchestration,
> leakage-safe dbt-duckdb gold features, deterministic model comparison, and explicit promotion
> gates are implemented. MLflow tracking and model registration are next.

## Intended architecture

```mermaid
flowchart LR
    TLC[NYC TLC Parquet] --> B[Bronze / Silver / Gold]
    B --> TRAIN[Training + promotion gate]
    TRAIN --> API[Prediction API]
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
model augmented with rolling zone features. All models use the same held-out month. The streaming
candidate is promoted only when it satisfies the configured MAE improvement, distance-bucket
calibration, and single-row inference-latency gates; an existing production-model comparison is
also enforced when production metrics are supplied.

Each content-addressed run under `artifacts/training/<run-id>/` contains the native LightGBM model
files, serialized baseline, evaluation report, input checksums, artifact checksums, promotion
decision, and generated model card. Repeating an identical run reuses that immutable bundle. See
[ADR-0006](docs/adr/0006-reproducible-training-and-promotion.md) for the evaluation and artifact
boundary.

## Airflow orchestration

The manually triggered `tripml_ingestion` DAG runs the same orchestrator-independent workflows as
the CLI. It accepts a `month` parameter, defaulting to `2024-01`, and an optional worker-visible
`config_path`. The DAG requires `TRIPML_LINEAGE__DATABASE_URL`, limits ingestion to 20 minutes and
the downstream gold build to 15 minutes, permits only one active run, and uses bounded retries. A
partition rejected by its quality gate fails without retrying and points operators to its durable
lineage run; gold is built only after acceptance.

Airflow discovers the thin entry point at `dags/tripml_ingestion.py`; the implementation lives in
the installable `tripml.airflow_dags` package so it can be type-checked and unit-tested. For local
development outside Kubernetes, install the orchestration dependency with:

```bash
python -m pip install -e '.[orchestration]'
```

The local cluster builds an immutable Airflow image containing the project and runs the API server,
scheduler with a two-worker LocalExecutor, and required DAG processor as separate containers. It
uses a dedicated PostgreSQL metadata database, persistent logs and ingestion data, generated
Fernet/JWT/UI credentials, resource boundaries, and component health probes. Access it after
`make cluster`:

```bash
make airflow-password # username: admin
make airflow-ui       # http://localhost:8080; runs until interrupted
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
make cluster-test    # rerun data-service, broker, and Airflow smoke tests
make cluster-status # inspect workloads, PVCs, and service endpoints
make cluster-delete # remove the cluster and its local data
```

The local chart currently provisions single-node Redpanda, PostgreSQL, Redis, MinIO, and Airflow
with health probes, resource requests and limits, persistent volumes, and credentials generated at
cluster creation. Secrets are never written to the repository or Helm values. MinIO is pinned to
its final official community image because upstream moved to source-only distribution; that
trade-off is intentionally limited to the zero-cost local profile. Airflow's simple auth manager is
likewise limited to the private development cluster; a shared deployment requires a production
auth manager.

The packaging rationale and production boundary are recorded in
[ADR-0001](docs/adr/0001-local-infrastructure-packaging.md).

## Engineering principles

- Event and table boundaries have explicit, versioned contracts.
- Event time—not arrival time—drives streaming windows.
- Offline features are point-in-time correct and tested against online features.
- A candidate model must pass declared quality and latency gates before promotion.
- Failure recovery and observability are product behavior, not follow-up work.
- Every portfolio claim will link to reproducible evidence from a showcase run.
