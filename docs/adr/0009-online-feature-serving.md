# ADR-0009: Event-time Redis reads and explicit model selection

- Status: Accepted
- Date: 2026-09-16
- Extends: [ADR-0008](0008-initial-prediction-api.md)

## Context

The prediction API can serve a verified static model. The next boundary is consuming online features
and selecting the promoted streaming model without turning Redis outages or invalid feature data
into request failures. The stream producer is not yet implemented, so its write contract must be
explicit and independently testable.

## Decision

Load, verify, check feature order, and warm both native models at startup. Select streaming inference
only when all required Redis snapshots pass validation; otherwise use the static sibling. Report
the actual model version and features in each prediction. Native inference errors still return 503;
they are not silently reclassified as feature-store failures.

Use one `MGET` against three string keys in the local single-node Redis deployment:

| Key | Aggregate population |
|---|---|
| `tripml:features:v1:pickup:zone:<pickup-id>:15m` | Completed trips grouped by pickup zone, previous 900 seconds |
| `tripml:features:v1:pickup:zone:<pickup-id>:60m` | Completed trips grouped by pickup zone, previous 3600 seconds |
| `tripml:features:v1:dropoff:zone:<dropoff-id>:60m` | Completed trips grouped by dropoff zone, previous 3600 seconds |

Each value is JSON conforming to the exported `online-zone-window-features-v1` contract. It extends
the existing window aggregate with `zone_role`, `feature_model_version=gold-features-v1`, and
`source_max_event_time`. All timestamps carry offsets. The population consists of completions in
`[window_start, window_end)`; the greatest contributing completion must be inside that interval.
The producer must never include a completion at the exclusive cutoff. Empty windows have zero
counts and means and a null source timestamp. All numeric aggregates must be finite and nonnegative.

The reader checks identity against each requested key, exact window duration, feature/schema version,
source-time bounds, and one common cutoff across all three snapshots. The short pickup count cannot
exceed the long pickup count. It rejects cutoffs after the pickup or older than the configured
staleness limit (600 seconds by default, inclusive at the boundary). Age is relative to pickup event
time, not process wall time; `computed_at` records processing time and must be at least the cutoff.
This supports accelerated historical replay. Expiry is enforced separately by Redis in wall time.

The upcoming producer must write JSON with the configured TTL, publish coherent cutoffs, and prevent
older replay/checkpoint emissions from overwriting newer snapshots. These write/recovery semantics
are not implemented by this read-only client. MGET observes a single Redis state; the common-cutoff
check rejects intermediate producer states. The scheme stores only the latest snapshot. Requests
older than that snapshot fall back rather than reading future features; historical lookup would
require a different storage scheme. Redis Cluster would also require a key/co-location redesign.

Map the snapshots into the existing four streaming columns, preserving the native training order.
Empty pickup windows select the static model because zero means would differ from gold's null means.
Empty dropoff windows legitimately yield a zero count. Accepted predictions record the **exclusive
cutoff** for each rolling feature in `feature_timestamps`; these are validity boundaries, not the
latest contributing event timestamps. The latter remain in each validated snapshot.

Configure Redis through `TRIPML_SERVING__REDIS_URL`, a secret excluded from config output and its
fingerprint. Reject URL query options that could override the configured socket policy. Use a bounded
pool (32 connections), 10 ms connect and socket timeouts, and zero retries. These are socket limits;
DNS, connection/authentication work, scheduling, and inference mean they are not a hard total request
deadline. The client connects lazily, so Redis failure does not prevent model readiness. Close the
owned pool at shutdown. Omitting the URL preserves explicit static-only operation.

Export bounded lookup outcomes (`fresh`, `disabled`, `unavailable`, `missing`, `invalid`, `future`,
`stale`, `inconsistent`, `empty`), successful fallback counts, and accepted event-time snapshot age.
Readiness reports configured mode and both model versions, not whether the most recent lookup was
fresh. Do not include Redis credentials or raw malformed values in metrics or responses.

## Validation and limits

Unit/API tests cover native streaming prediction equivalence, source-window boundaries, replay time,
staleness boundaries, malformed/version-mismatched data, cross-role mistakes, mixed cutoffs, empty
windows, Redis failures, lifecycle cleanup, and observable static degradation. An opt-in real Redis
test checks MGET, JSON validation, TTL attachment, expiration, and cleanup; CI supplies a disposable
Redis service automatically. The local integration run uses an isolated loopback-only container.

The offline builder now rejects nonstandard durations for the `gold-features-v1` column names.
Previously built or externally supplied gold files remain trusted inputs; version strings alone do
not prove how those files were computed. Rebuild artifacts if their window configuration is unknown.

Snapshots ending before pickup may include older completions and omit newer ones relative to gold's
exact pickup-aligned lookback. Bounded lag does **not** establish offline/online parity or measured
quality. The producer/parity milestone must measure and resolve that difference. Static fallback
quality also remains evaluated but not independently promotion-gated. No live traffic, broker
publication, serving load objective, or Kubernetes serving deployment is claimed here.

## References

- [Redis Python production usage and retries](https://redis.io/docs/latest/develop/clients/redis-py/produsage/)
- [Redis Python connection pools](https://redis.io/docs/latest/develop/clients/redis-py/connect/)
