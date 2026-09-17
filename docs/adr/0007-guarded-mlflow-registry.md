# ADR-0007: Guard MLflow registration with the promotion decision

- Status: accepted
- Date: 2026-09-16

## Context

The immutable training bundle already determines whether a streaming-feature candidate is safe to
promote. Adding experiment tracking must preserve that decision boundary: a rejected candidate
should remain inspectable without becoming servable, repeated publication must not create duplicate
versions, and replaying an older successful bundle must not roll production backward. The same
workflow also needs to run from a laptop and from Airflow in the local Kubernetes profile.

## Decision

- Log the baseline, static candidate, and streaming candidate as separate runs in one MLflow
  experiment, linked by the content-addressed training-bundle identifier.
- Record model metrics, feature lists, input/configuration lineage, model checksums, and the
  promotion outcome. Attach the evaluation, manifest, and model card to the streaming run.
- Read the current production alias before training and include its version and metrics in the
  promotion comparison and semantic run identifier.
- Create a registered-model version only when every promotion gate passes. Rejected candidates
  remain experiment runs and cannot change the production alias.
- Make publication idempotent by matching the bundle identifier, model role, and artifact checksum.
  Refuse inconsistent matches and never move the production alias to an older registry version.
- Keep the local default self-contained with SQLite and filesystem artifacts. In kind, use
  PostgreSQL for MLflow metadata and proxy artifacts through the MLflow server into MinIO so Airflow
  clients do not require object-store credentials.
- Run this workflow in a separate, manually triggered Airflow DAG with one active run and bounded
  retry and timeout policies. Keep a `--no-track` CLI path for offline training-core development.

## Consequences

Every candidate remains auditable while only gate-approved models become addressable through the
production alias. Registry state is safe under task retries and intentional republication, and the
serving service can later load one stable alias instead of interpreting training reports.

The local profile now operates another stateful service and custom image. SQLite is suitable for a
single local process only; concurrent or shared deployments must use the PostgreSQL-backed server.
Automatic rollback remains an explicit future operator workflow rather than an effect of rerunning
historical training output.
