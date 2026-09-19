# ADR-0014: Reproducible real-data serving workloads

- Status: Accepted
- Date: 2026-09-17

## Context

The serving and autoscaling suites use synthetic requests, models, and Redis snapshots. Those
tests establish deployment and failure behavior, but their request mix cannot support claims about
representative TLC traffic. Taking the first rows of a monthly Parquet file also risks concentrating
the workload in one time period or source ordering.

## Decision

Add `tripml benchmark-workload` and `make benchmark-workload`. Scan the requested silver month in
bounded Arrow batches and sample uniformly without replacement using a seeded reservoir. Memory
scales with the Arrow batch plus the requested sample, capped at 100,000 requests to match the HTTP
benchmark. Shuffle the final reservoir so even full-population fixtures do not retain source order.
Reproduction requires the same source bytes, seed, and Python version; batch size does not affect
selection. This is a marginal request-mix sample, not a reconstruction of real arrival patterns.

Require a matching accepted ingestion quality report and matching silver row count. Validate every
eligible row against the serving request contract; fail on invalid values or schema versions.
Fingerprint silver before and after scanning and recheck the quality report to detect changing
inputs. The ingestion report has bronze provenance but no historical silver digest: these checks
bind the workload to the observed silver bytes, not to an independently verified ingestion-time
silver hash. No duration or fare labels enter the generated request payloads.

Interpret TLC naive timestamps in America/New_York. Exclude and count ambiguous autumn fold times
and nonexistent spring gap times because the source cannot identify a unique instant. Preserve
historical timestamps and UTC offsets for all remaining pickups. Do not rebase them to today or
seed arbitrary online features to make a freshness check pass.

Export canonical ETARequest JSONL, the original quality report, a manifest, and a usage/limitations
README to a new directory. Validate inputs before creating output; never reuse an existing
directory. Write the manifest last so an interrupted write lacks a completion record. Compare the
sample and eligible population using counts and total variation distance for pickup hour of week,
pickup zone, dropoff zone, distance bucket, and passenger count. These descriptive marginals expose
sampling limitations; they do not guarantee joint-distribution or tail coverage and impose no
arbitrary pass threshold.

The HTTP benchmark accepts `--workload-manifest`, checks its request digest before any traffic, and
copies the manifest into its checksummed evidence. Existing hand-authored fixtures remain supported.
Benchmark requests still receive unique trip IDs and run at the configured constant arrival rate.

## Validation and limits

Tests exercise ingestion-to-workload-to-benchmark compatibility, seed and batch reproducibility,
unique source trip IDs, DST exclusions, population/profile accounting, corrupted and changing
inputs, overwrite prevention, CLI behavior, and provenance mismatch rejection before HTTP traffic.
The real January 2024 partition is also sampled locally; the validation note records its input and
output digests and distribution differences.

This step supplies real request inputs. Real-data training and held-out accuracy evidence, matching
online feature snapshots, in-cluster performance measurements, and request-rate autoscaling remain
separate work. Historical request timestamps alone do not establish streaming correctness or fresh
Redis features. A bounded sample can miss rare routes; review distribution differences and use
additional explicit stress workloads when studying tails.
