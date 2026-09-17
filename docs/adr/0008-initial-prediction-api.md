# ADR-0008: Verified prediction API with an explicit static fallback

- Status: Accepted
- Date: 2026-09-16

## Context

The batch pipeline already publishes a guarded streaming-feature model to an MLflow production
alias. There is no online feature producer or Redis read contract yet. The next deliverable should
make trained artifacts callable and establish observable serving behavior without claiming live
features or fabricating rolling statistics.

## Decision

Provide a FastAPI service with `POST /v1/eta`, `/healthz`, `/readyz`, and `/metrics`, launched using
`tripml serve`. Use FastAPI's lifespan boundary to load and warm the model before accepting requests.
Run native inference in a synchronous endpoint, which FastAPI dispatches to its thread pool, with
LightGBM restricted to one inference thread per request. Keep one Uvicorn process per instance so the
process-local Prometheus registry accurately describes that instance.

The default startup path resolves the production alias once, checks its finished streaming run and
promotion manifest, verifies the promoted model checksum, and locates exactly one static sibling
in the same experiment and training bundle. Verify the static checksum against both its run tag and
the promoted manifest, check native feature order, and warm the actual prediction path. Read and
hash the static model bytes before loading those same bytes into LightGBM. No pickle deserialization
is involved. Artifact checksums detect corruption and inconsistent metadata; they are not signatures
or a defense against a malicious registry administrator.

An explicit `--bundle` path supports local development, including rejected training candidates.
Resolve fixed filenames within that directory rather than trusting paths recorded on the training
machine. This makes copied bundles usable. Local mode does not claim a registry version or promotion.

Until the feature store is implemented, serve the static model on every request. Record its actual
`<bundle-run-id>-static` identifier, all input features, `feature_fallback=true`, and no online feature
timestamps. This is a deliberate refinement of the original plan's batch-default fallback: a model
trained without rolling features avoids inventing statistics to feed the streaming model. The static
model has evaluation evidence but has not passed an independent promotion gate. A later integrated
serving milestone must evaluate degraded-mode quality and either gate this fallback or replace it
with a measured alternative.

Calendar features convert offset-aware requests to `America/New_York` and use Sunday as day zero,
matching the existing gold SQL. Both repeated autumn hours map to the same calendar feature; this
does not resolve the separate ambiguity in historical TLC event ordering. Input validation rejects
unknown fields, invalid zones/counts, naive timestamps, and nonfinite distances. Nonfinite or
nonpositive model outputs fail with HTTP 503 rather than being silently clamped.

The loaded model is immutable for the process lifetime. A restart selects a new alias; there is no
HTTP reload or other operator mutation endpoint in this slice. Failed startup cannot advertise
readiness. Once loaded, inference does not depend on the registry remaining available. Liveness
tracks the process, readiness tracks the loaded snapshot, and counters and a latency histogram
measure prediction requests, including validation and inference failures. Labels exclude trip IDs,
user input, and model versions to keep cardinality bounded.

## Validation and consequences

Tests train native LightGBM artifacts, publish through a real temporary SQLite-backed MLflow
registry, load the promoted bundle, and request a prediction over the ASGI HTTP interface. They
check native prediction equivalence, relocated bundles, calendar/DST boundaries, concurrent model
reads, validation failures, corrupt checksums, mismatched feature order, registry inconsistencies,
health transitions, and metric values. These are integration correctness checks, not a network load
test or evidence that the 50 ms service objective has been achieved.

Redis reads, streaming inference, prediction publication, durable prediction logging, Kubernetes
packaging/HPA, and serving load measurements remain follow-up milestones. The API defaults to
loopback access. Future operator endpoints must include authentication before being exposed.

## References

- [FastAPI lifespan events](https://fastapi.tiangolo.com/advanced/events/)
- [FastAPI synchronous endpoint execution](https://fastapi.tiangolo.com/async/)
- [Prometheus Python histograms](https://prometheus.github.io/client_python/instrumenting/histogram/)
