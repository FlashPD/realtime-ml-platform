# ADR-0003: Atomic Airflow ingestion task

- Status: accepted
- Date: 2026-09-15

## Context

The ingestion workflow downloads or reuses a verified bronze object, validates it in bounded
batches, atomically publishes an accepted silver partition, and records the terminal result in
PostgreSQL. Airflow could expose each operation as a separate task, but that would split a single
transactional boundary across independently retryable workers. It would also pass storage details
through the orchestrator even though the CLI already needs the same business workflow.

Orchestrated runs need durable lineage, bounded retries for transient infrastructure failures, and
a clear distinction between an operational failure and a partition that deterministically fails
its data-quality gate.

## Decision

- Keep ingestion behavior in an orchestrator-independent library function used by both the CLI and
  Airflow.
- Model one partition ingestion as one Airflow task. The task invokes the complete transactional
  workflow rather than dividing its commit boundary across task instances.
- Require the PostgreSQL lineage DSN for Airflow runs while preserving lineage-free local CLI use.
- Retry ordinary exceptions twice. Each attempt creates a new lineage run, while a verified bronze
  object is safely reused.
- Convert a quarantined quality result to a non-retryable Airflow failure because retrying identical
  source data cannot change the outcome.
- Return only the compact quality report and lineage identifier through Airflow; files remain in
  storage rather than being transferred through task metadata.
- Permit one active DAG run to prevent concurrent publication of the same partition until explicit
  partition-level locking is implemented.

## Consequences

The CLI and scheduler cannot drift in their ingestion semantics, and Airflow owns operational
policy rather than data-processing logic. A run has a simple failure model: transient failures are
bounded and visible as separate lineage attempts, while data-quality rejection stops immediately
with evidence preserved in quarantine and PostgreSQL.

The single task provides less per-stage scheduling visibility than a multi-task DAG. PostgreSQL
lineage and the quality report provide stage-level diagnostic evidence instead. If future stages
gain independent commit boundaries—such as publishing gold features to versioned object storage—
they can become separate tasks without weakening the bronze-to-silver transaction.
