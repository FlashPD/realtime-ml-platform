# Portfolio completion checklist

Repository audit: 2026-09-18, starting at `541f22a`. The first publishable milestone is the
[batch-serving release](batch-serving-release.md). The broader streaming architecture is a
separate milestone. Completion means a reviewer can inspect the evidence and reproduce the
demonstrated behavior.

Update, 2026-09-19: the approved model is now deployed and ready on kind. Its three real April
cohort predictions matched native inference and consumed broker events. Subsequent
[load, monitoring and isolated broker recovery](validation/approved-model-operations.md) also
passed. The starting-point observations below describe the earlier audit; clean-checkout
reproduction, accessible artifacts and portfolio release packaging remain pending.

Update, packaging pass: the [clean-checkout receipt](validation/clean-checkout.md),
[portable artifact workflow](runbooks/reproduce-release.md), concise README and
[five-minute walkthrough](portfolio-walkthrough.md) are now prepared. The maintainer explicitly
owns all commits and releases; [publication commands](runbooks/publish-release.md) are provided.
Earlier “pending” statements below describe the starting audit, not the packaged candidate.

## Verified starting point

- The recorded January–March training / April evaluation uses 9,316,058 training trips and
  3,419,441 holdout trips. Static MAE is 187.18 seconds, 25.78% below the declared baseline.
  The approved bundle is `98ef1dd9ef4447f7`; see the
  [evaluation and limitations](validation/batch-release-model.md).
- Ingestion, temporal features, model gates, registry publication, serving, broker acknowledgment,
  online feature reads, Helm packaging, and monitoring have implementations and tests.
- The audit reran the default suite: **300 passed, 6 skipped, 94.89% coverage**. Ruff, formatting,
  strict mypy, shell syntax, and all three Helm lint/render profiles passed. The skipped tests
  require explicit service or Kubernetes configuration; this run is not end-to-end evidence.
- The inspected `kind-tripml` / `tripml` namespace has running Airflow, PostgreSQL, Redis,
  MinIO, and Redpanda. MLflow, serving, Prometheus, and Grafana workloads were absent at audit time.
  This is a local observation, not a statement about every possible deployment.
- The original CI workflow supplied Redis and Redpanda but omitted
  `TRIPML_TEST_DATABASE_URL`, so the PostgreSQL lineage integration test was always skipped.
  This change adds a disposable PostgreSQL service and its test connection to CI.
  The existing integration test passed separately against a fresh `postgres:16.15-bookworm`
  container (**1 passed**); the container and its data were removed. The changed GitHub Actions
  workflow has not yet run remotely.

## Finish the first portfolio release, in this order

| Order | Remaining work | Evidence required to close it |
|---|---|---|
| 1 | Completed: approved model available to Kubernetes | Cluster MLflow version 1, original bundle/input verification, and idempotent guarded publication recorded |
| 2 | Completed: deployment and monitoring | Approved identities, static readiness, cohort/native parity, healthy scrapes, Grafana datasource/dashboard query exports and telemetry figure recorded |
| 3 | Completed: representative cluster traffic | 10,000 April successes at 100/s, P95 7.99 ms, no errors/drops, 10,020 benchmark/warm-up broker events matched; raw samples and resource observations retained |
| 4 | Completed: isolated broker failure and recovery | 120 explicit 503 responses, acknowledgment restored in 6.25 s, all 4,040 baseline/recovery benchmark events matched, same API process, and verified cleanup |
| 5 | Completed locally: reproducible package | Fresh environment checks and relocated-model smoke, checksummed model/evidence archive, dependency/image inventory, rebuild instructions and cleanup boundary recorded |
| 6 | Presentation complete; publication pending | Short README, five-minute walkthrough, evidence links, limitations, portfolio description and release notes delivered; maintainer commits, tags and uploads assets |

The release workload already exists. Start with the recorded 10,000-request April sample and
declare the load before running it. The existing objectives are client P95 <50 ms, errors ≤1%,
and no dropped arrivals. Preserve failures and report the observed result; do not adjust the
threshold after seeing a run. The local 100 requests/s preflight is a starting configuration,
not a guarantee that acknowledged Kubernetes serving achieves the same result.

Static-primary serving intentionally bypasses Redis. Its `feature_fallback=true` records static
feature usage, not a Redis incident. Redis degradation is meaningful for the later streaming
release; the first release must demonstrate broker behavior for its actual serving configuration.

## Delivered: real-model cluster load validation

The existing [kind serving test](../tests/e2e/test_serving_kind.py) seeds synthetic training data
and a temporary registry. The [load helper](../tests/e2e/kind_load.py) uses the example workload
and fixture online snapshots. Neither can establish release-model capacity simply by rerunning it.
The [approved-model deployment path](runbooks/deploy-approved-model.md) now verifies real-model
serving and broker readback. The [operations harness](../scripts/run-release-operations.py)
accepts the held-out workload and reuses the benchmark implementation. Synthetic fixture tests
remain independently runnable. Clean-checkout reproduction and packaging are recorded in the linked packaging update.

- [x] Verify the original report, native artifact, gold hashes, and held-out workload before any
  registry write. Reuse the checks in [the release validator](../scripts/validate-release-model.py).
- [x] Separate immutable training configuration from deployment destination settings. The validator
  verifies the original fingerprint before applying `--tracking-uri` and records the destination
  separately. Corrupt-evidence tests cover the destination path; existing incumbent gates remain.
- [x] Publish the existing approved report through `publish_training_report`; do not retrain simply
  to relocate artifacts. Record destination registry identity and idempotent retry behavior.
- [x] Deploy the verified serving image, approved static model name, and dedicated prediction topic.
- [x] Enable monitoring for the release workload. Scope temporary resources and cleanup explicitly.
- [x] Check native/API prediction agreement for the three passenger-count cohorts and consume
  acknowledged events before starting load.
- [x] Drive load from inside the cluster; export raw benchmark output, scrape results, timestamps,
  and sanitized deployment identities. Keep HTTP and client latency definitions distinct.
- [x] Export diagnostics on failure and restore any injected failure before cleanup. Broker
  fault injection must use an isolated dependency so the existing platform is unaffected.

This milestone is complete only when the evidence uses `98ef1dd9ef4447f7-static`, the April
workload, and acknowledged publication together under declared load. Synthetic test results remain
separately labeled.

## Then add the streaming differentiator

These are planned capabilities still missing from the implementation, rather than
documentation tasks:

1. **Event replay and live feature production:** event-time replay, broker schemas, window and
   watermark policy, late-event accounting, Redis writes, and checkpoint/restart recovery.
2. **Offline/online parity:** compare a replayed day against gold, quantify mismatch rate and
   maximum difference, and fail on skew. Offline rolling features improve April MAE by 5.11%
   relative to static; that is currently an offline result, not a live-streaming result.
3. **Closed-loop evaluation:** join predictions to completions, persist errors, report join
   coverage and live MAE, detect drift, and trigger guarded retraining with an auditable decision.
4. **Operational automation:** request-rate autoscaling, actionable alerts, scheduled/on-demand
   kind CI, dependency/image scanning, and a resumable showcase harness with the planned faults.

A complete batch release can be presented while these are built. Describe the implemented scope
explicitly whenever using the project's “real-time” name. Cloud deployment is optional in the
current plan; it is not a substitute for correctness and measured failure behavior.

## Reviewer and interview checks

- Explain the product assumption: TLC distance is observed completed-trip distance. The current
  experiment does not establish pre-trip ETA accuracy. A pre-trip claim needs available-at-request
  inputs, a matched experiment, and a fresh final holdout.
- Explain model comparison fairly: the median baseline lacks distance and passenger count. A
  distance-aware baseline or feature ablation would strengthen modeling evidence, but any tuning
  now needs a new validation design because April has already been observed.
- Explain trade-offs with evidence: acknowledged publication adds latency and broker dependence;
  static serving has different feature semantics; single-node kind does not establish high
  availability or production capacity.
- Make the evidence accessible: raw runs and native models currently live in ignored `artifacts/`
  directories. Checked-in summaries help, but release instructions must not depend on files that
  only exist on the author's laptop.
- Keep the entry point brief. The audit-time 710-line README contains useful operating details;
  move detailed procedures to runbooks when preparing the final reviewer walkthrough.

The previous README calendar estimates included work that has since shipped. Remaining batch
estimates should focus on reproduction and packaging, with streaming estimated separately;
the old estimates are not a current delivery commitment.
