# ADR-0012: Constant-arrival serving load and auditable evidence

- Status: Accepted
- Date: 2026-09-16

## Context

The API verifies model artifacts, degrades to static inference when online features are unusable,
and can wait for broker acknowledgment. Unit tests and a Kubernetes smoke prediction establish
functional behavior. The 50 ms serving objective still needs a reproducible measurement path before
it can become a portfolio claim. A client that waits for each response before scheduling another
request silently reduces offered load when the service slows down.

## Decision

Provide `tripml benchmark` as an optional HTTPX client, independent of the serving runtime. Require a
validated ETARequest JSONL fixture and a new evidence directory. Send a finite, fixed-rate sequence
of arrivals using a monotonic clock, pooled HTTP connections, no retries, no redirects, and no
environment proxy configuration. Limit in-flight tasks and connections. Record arrivals that exceed
that limit as load-generator overflow; they count toward errors and independently fail the run.
An asyncio total deadline complements HTTPX's per-operation timeouts. The request count is bounded
at 100,000 and fixture rows at 100,000; sample storage is O(request count), not an indefinite soak test.

Measure latency from scheduled arrival through response validation, so delayed client dispatch does
not disappear from the percentile. Export HTTP-attempt latency and dispatch lag separately. Use exact
nearest-rank P50/P95/P99, reporting null for an empty population. Gate successful-prediction P95
strictly below the configured objective and errors at or below their budget; require at least one
successful prediction and full arrival accounting. Report attempted-request latencies as well so
timeouts remain visible. Dropped arrivals have no HTTP latency and remain explicit failed records.

HTTP 200 alone is insufficient. Validate the Prediction contract, matching trip ID, declared feature
mode, and publication header. Require an explicit publication expectation, defaulting to acknowledged.
The feature expectation can be static, streaming, or any; use streaming for online-serving claims.
The acknowledgment header reflects the API's delivery contract, not an independent consumer check.
Feature mode checks do not establish feature correctness or offline/online parity.

Run sequential warm-up before measurement, retain its outcomes separately, and do not include it in
measurement gates. A failed warm-up remains visible but does not invalidate a subsequent successful
measurement. Generate unique trip IDs with a benchmark prefix for every request including warm-up.
This avoids accidental trip collisions and gives downstream evaluation an exclusion rule. Configured
publication still sends real events. Consumers must exclude benchmark traffic; there is no joiner yet.

Export the configuration, normalized fixture, each request outcome, model and prediction identifiers,
client runtime information, and a summary with checksums of the raw inputs/outputs. Generate a short
evidence README. Never overwrite existing output. An interrupted/unexpectedly failed run may leave
partial inputs without a summary; only a completed summary is a result. HTTP failures are completed
results, not missing evidence. Deliberately omit raw response bodies and exception strings from
samples; the target origin cannot contain credentials or query parameters.

## Validation and limits

Tests cover fixed-rate overload accounting and the concurrency cap, empty latency populations,
exact percentiles, objective boundaries, total deadlines, connection failures, HTTP errors and
redirects, malformed predictions, identity and serving-mode mismatches, fixture/config validation,
warm-up exclusion, evidence integrity, overwrite refusal, and CLI exit codes. These tests do not
assert machine-dependent latency performance.

An isolated target is required for manual failure experiments. The benchmark itself never modifies
Redis, broker, registry, or Kubernetes resources. Compare explicit streaming and static expectations
with server metrics to establish the degradation cause. A local synthetic native-model run can
validate the real HTTP measurement path, but it does not prove representative traffic behavior,
real-data quality, feature parity, clustered latency, or autoscaling. Target CPU/memory limits,
hardware, image/model provenance, dependency state, and the network path must accompany published
capacity claims. Git/image provenance is not inferred from the client's checkout, which may differ
from the server. CPU HPA exercise, request-rate scaling, and monitoring remain follow-up milestones.
