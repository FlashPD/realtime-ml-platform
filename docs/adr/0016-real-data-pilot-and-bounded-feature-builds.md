# ADR-0016: Real-data pilot and bounded feature builds

Status: Accepted

## Context

The planned release trains on January–March 2024 and holds out April. The existing 10% partition
violation gate accepts December, January and February, but rejects March (14.12%) and April
(13.88%). Null passenger counts alone affect 11.90% of March and 11.63% of April rows. No other
required source field has null values in these files.

This prevents the declared release evaluation under the current contract. The decision was made
before evaluating any real-data model metrics: retain the gate and run a January-training /
February-holdout pilot. December provides January's lookback context, not additional training rows.

## Decision

Keep source row validation and partition thresholds unchanged. Preserve rejected partitions and
their reports. Use an explicit checked-in pilot configuration with the existing 800-tree, seed-42
LightGBM settings and unchanged promotion thresholds. Run with `--no-track`; an offline eligibility
result must not imply registry publication or deployment.

Build full accepted monthly gold partitions. Bound DuckDB's buffer manager to 2 GB, spill storage
to 8 GB and its worker count to two. These are engine controls, not an operating-system RSS limit;
record actual process peak RSS separately. Set both dbt concurrency and DuckDB threads explicitly.
Use the temporary database's default spill location: setting `temp_directory` on every dbt cursor
fails once the shared database has already spilled.

Prune previous-month data by completion time to the longest relevant lookback, retaining target
pickups. Do not prune by pickup time, which would lose earlier trips that complete inside the
lookback. Keep inclusive lower bounds and exclusive pickup-time upper bounds. Regression tests
cover old completions, long trips, equal-time exclusions and the lower-bound inclusion.

Disable insertion-order preservation to reduce materialization memory. Artifact checksums identify
the actual gold bytes used; independently rebuilt gold files can differ in physical row order.
Deterministic training reuses identical input artifacts and parameters, not arbitrary reorderings.

Expand training evidence with per-distance-bucket counts, MAE, actual/predicted means and calibration
error for all three models. Evaluate the static model separately against baseline/calibration/latency
gates. Do not compare static eligibility with a streaming incumbent or change registry behavior.
Evidence version 2 participates in the run identifier so old cached reports cannot masquerade as
the expanded comparison; version-1 bundles remain readable.
Older serving images with the strict version-1 report schema cannot load the new fields. Rebuild
the serving image before any future deployment of a version-2 bundle.

## Consequences

The pilot can establish real-data feasibility and expose model weaknesses while preserving data
quality decisions. It does not satisfy the planned April release holdout, validate streaming parity,
or approve a static production alias. Supporting missing passenger counts requires an explicit
future contract decision, not a higher threshold chosen to pass these files.

Reproduction uses [the pilot configuration](../../examples/training/real-data-pilot.yaml).
The command-measurement helper retains stdout, stderr, exit status, wall time and the largest
child-process RSS high-water mark in a new directory for each attempt. Peak RSS is not aggregate
process-tree memory. Concurrent laptop work can affect timings.
