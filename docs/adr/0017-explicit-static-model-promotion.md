# ADR-0017: Explicit static-model promotion

Status: Accepted

## Context

The January / February pilot established static-model eligibility, while registry publication
always selected the streaming candidate. Serving then used the static sibling when Redis was
absent. That fallback is not an independently approved batch deployment, and enabling Redis could
change the deployed model without changing the alias.

## Decision

Select `tracking.candidate_role: static` or `streaming` before training. Keep `streaming` as the
backward-compatible default. Train and report all three model paths, but apply the baseline,
calibration, inference-latency and incumbent gates to the selected candidate. Evidence version 3
records `promotion_role`; the role participates in the bundle identifier. Static eligibility
remains a separate diagnostic without an incumbent comparison. Existing version-1/2 manifests
default to streaming and remain readable; their static eligibility cannot authorize publication.

Use distinct registered model names for static and streaming models. Publication refuses a
registry containing versions of the other role. Each candidate run carries its native model and
the bundle evidence. Only the selected passing run can become a registered version. Verify local
artifacts and agreement between the supplied report and manifest before logging.

An incumbent comparison requires the same model role, holdout month and exact gold checksum.
Stored scores from a different evaluation population cannot authorize promotion. Changing the
holdout requires a future workflow that reevaluates the incumbent on the new population; this
implementation fails closed. Legacy runs without a holdout checksum cannot authorize a new
incumbent comparison, though existing promoted bundles remain serveable.

Check that the production alias still matches the evaluated incumbent before registering a new
version and again before moving the alias. Repeated publication reuses an existing version and
cannot move the alias backward. This is a stale-evidence guard, not an atomic compare-and-swap:
MLflow alias updates are not transactional with these reads. Keep a single registry writer
(the training DAG already limits active runs); concurrent independent CLI publishers are outside
the supported promotion workflow.

Serving derives its role from the verified registry evidence, not from Redis availability or the
training configuration on the API host. A static release loads and warms only `static-model.txt`,
reports `mode=static_primary`, exposes no streaming version, and skips Redis construction and
lookups. A streaming release retains its verified static sibling and existing fallback behavior.
Changing aliases requires a process restart; inference remains independent of registry uptime.

Preserve the version-1 prediction contract: `feature_fallback=true` and the existing fallback
counter mean that static features were used. In `static_primary` mode that is intentional, not
degradation. Benchmark with `--expected-features static`; interpret counters alongside readiness.
A future event-contract revision can separate feature mode from fallback reason.

## Consequences

The static pilot can be registered and served with attributable approval evidence without a live
stream producer. Existing streaming installations retain their behavior. Model cards identify the
candidate actually considered for promotion. The new runbook uses an isolated SQLite registry and
an explicitly named pilot model; it does not complete the April release evaluation or deploy to
Kubernetes. Rebuild serving images before using version-3 reports with their added role field.

Tests cover native training, registry-to-HTTP static inference, acknowledged publisher invocation,
Redis bypass, candidate rejection, role isolation, stale evidence, holdout mismatch, artifact
tampering, idempotency, and protection against alias rollback.

See the [static pilot runbook](../runbooks/static-model-promotion.md).
