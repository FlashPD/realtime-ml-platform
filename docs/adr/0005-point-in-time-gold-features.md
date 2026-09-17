# ADR-0005: Completion-time ledgers for point-in-time gold features

- Status: accepted
- Date: 2026-09-16

## Context

The training table needs rolling zone statistics that match what a streaming processor can know at
pickup time. Joining each pickup to every trip in its lookback window is easy to read but can create
a very large intermediate relation for monthly TLC partitions. Computing windows over pickup time
would be faster but would leak outcomes from trips that had started and not yet completed.

The first hour of a monthly partition also depends on completions from the preceding month. A gold
artifact must remain reproducible after the local DuckDB database used to build it is discarded.

## Decision

- Represent each accepted trip twice in a zone event ledger: once as a completion carrying its
  observed metrics and once as a pickup probe carrying no observed outcome.
- Compute DuckDB range windows over completion time, partitioned by zone. Use `EXCLUDE GROUP` so
  every completion whose timestamp equals the pickup timestamp is excluded; only information
  strictly earlier than pickup is eligible.
- Read the target silver partition and the preceding partition when it exists, then publish only
  target-month pickup rows. This preserves lookback context at month boundaries without scanning
  the full history.
- Materialize the tested dbt model to a temporary Parquet path and atomically rename it into the
  gold partition only after all dbt tests pass.
- Write a JSON manifest with input paths, input and output SHA-256 digests, row counts, window
  configuration, configuration fingerprint, and feature-model version.
- Keep gold as explicit Parquet rather than introducing a feature-store abstraction. The same
  feature definitions will be implemented by the streaming processor and compared by a parity
  test.

## Consequences

Window execution is bounded by the target and preceding partitions instead of by a pickup-to-trip
range-join explosion. The synthetic known-answer test covers an unfinished trip, a completion at
the exact pickup timestamp, and a prior-month completion. Feature timestamps in the output make the
no-leakage property auditable by downstream training and serving code.

The dbt build requires the `transformation` package extra. Null means and timestamps are retained
when no history exists, while counts are zero; the future serving layer must apply an explicit
fallback policy rather than confusing missing history with a measured zero.
