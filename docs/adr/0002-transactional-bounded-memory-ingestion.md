# ADR-0002: Transactional, bounded-memory local ingestion

- Status: accepted
- Date: 2026-09-15

## Context

A yellow-taxi month contains millions of rows. The local profile must process it on a 16 GB laptop,
retain evidence about source identity and rule failures, and never expose a partially written or
rejected silver partition. TLC also warns that its Parquet schema can change.

The architecture plan originally named dataframe validation as the table-contract mechanism. A
whole-partition dataframe is convenient, but it couples validation to available RAM and makes the
failure boundary less explicit. The eventual orchestration layer should call library functions;
quality behavior must not depend on Airflow task internals.

## Decision

- Download into a temporary file, verify Parquet header and footer bytes, fsync it, and atomically
  replace the bronze destination.
- Write a versioned JSON manifest containing source URL, retrieval time, size, and SHA-256. Reuse a
  bronze object only after re-verifying it against the manifest.
- Require the expected source columns before creating outputs.
- Evaluate explicit, named rule masks over bounded PyArrow record batches.
- Write normalized valid rows and annotated invalid rows to temporary Parquet files.
- Atomically publish silver only after the partition-level violation gate passes. On failure, keep
  any existing silver file unchanged and retain the full source under quarantine.
- Generate stable trip IDs from taxi kind, declared month, and source row number. The bronze hash
  supplies the revision identity.

## Consequences

Peak memory is proportional to batch size rather than partition size, and every accepted output is
traceable to an immutable source hash. Rule masks are directly unit-tested and can later be emitted
as metrics or persisted to Postgres without changing their semantics. Airflow can wrap the same
function without owning business logic.

Atomic replacement is guaranteed only within one filesystem, which is true for the local profile.
The MinIO implementation will use object-versioned keys plus a committed manifest pointer instead
of assuming rename semantics. Concurrent ingestion of the same partition will also require an
orchestrator-level lock. PyArrow does not currently ship PEP 561 typing metadata, so its imports are
isolated behind a narrow mypy override while all project-owned interfaces remain strictly typed.
