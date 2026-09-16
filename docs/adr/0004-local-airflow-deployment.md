# ADR-0004: Single-pod Airflow topology for the local profile

- Status: accepted
- Date: 2026-09-15

## Context

Airflow 3 requires an API server, scheduler, and standalone DAG processor. The platform needs all
three components, a non-SQLite metadata database, durable task logs and ingestion outputs, and an
image that contains exactly the DAG code and dependencies under test. It must also fit alongside
PostgreSQL, Redis, MinIO, and Redpanda on a single-node kind cluster with a 10–12 GB memory budget.

The official Airflow Helm chart is the appropriate default for a shared or multi-node deployment,
but its distributed topology and supporting components add operational and memory overhead that do
not improve this private laptop showcase. Installing dependencies or fetching DAG code when a pod
starts would also weaken reproducibility.

## Decision

- Build an immutable project image from the pinned Airflow 3.3.1 Python 3.12 image. Install the
  `tripml` package and copy the DAG entry point during the image build.
- Run the API server, scheduler with LocalExecutor, and DAG processor as separate containers in one
  Kubernetes pod. Limit LocalExecutor parallelism to two tasks for predictable local memory use.
- Use a dedicated database in the existing PostgreSQL server for Airflow metadata. Apply Airflow
  migrations in an init container before any long-running component starts.
- Persist Airflow logs and ingestion data in separate claims. Use the image-embedded local DAG
  bundle; a shared deployment would use a versioned Git DAG bundle.
- Generate the database URLs, Fernet key, JWT signing secret, and local admin password during
  cluster bootstrap. Helm references the pre-created Secret and never renders credential values.
- Run as a non-root user with dropped Linux capabilities and a read-only root filesystem. Mount
  only configuration, temporary, data, and log paths as writable.
- Probe the API server, scheduler, and DAG-processor heartbeat independently. Extend the platform
  Helm smoke test to cover Airflow's public health endpoint.
- Use Airflow's simple auth manager only in the private local profile. A shared or production
  profile must use a production auth manager and the official chart or an equivalent maintained
  deployment.

## Consequences

`make cluster` now builds and loads the same image the scheduler executes, creates durable secrets
without committing them, migrates metadata idempotently, waits for all Airflow components, and
tests their health. Reviewers can inspect the DAG and trigger ingestion through a port-forwarded UI
without installing Airflow on the host.

The pod is intentionally a single failure and scaling unit. Logs and data use node-local storage,
authentication is development-grade, and LocalExecutor tasks share scheduler resources. These are
explicit local-profile constraints, not claims about a production or multi-tenant deployment.
