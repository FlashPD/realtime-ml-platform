# Batch-serving portfolio release

The first release demonstrates a reproducible path from public taxi data to evaluated models,
guarded registry publication, Kubernetes prediction serving, and operational evidence. Its value
for a senior AI engineering portfolio is the combination of data correctness, model decisions,
failure behavior, and reproducible measurements.

This is the active release scope. The [full architecture](../arch_plan/realtime-ml-platform-plan.md)
continues to describe later streaming and closed-loop milestones. No release tag is ready yet.

## Acceptance checklist

| Deliverable | Current evidence | Release acceptance |
|---|---|---|
| Data and leakage controls | Ingestion, lineage, gold feature tests; [full release gold and April workload](validation/unknown-passenger-policy.md), with versioned missingness policy and preserved strict evidence | Retain these source checksums, row counts and contract versions in the final model/release evidence |
| Real-data model comparison | [Full January–March / April evaluation](validation/batch-release-model.md): static MAE 187.18 s, 25.78% below baseline; all fixed gates passed; cohorts and original pilot retained | Completed for bundle `98ef1dd9ef4447f7`; future tuning requires a fresh final holdout |
| Model selection for batch serving | Full release static model registered as version 1 in isolated `tripml-trip-duration-static-v2`; native/API parity, role isolation and idempotent retry verified ([ADR-0017](adr/0017-explicit-static-model-promotion.md)) | Completed: exact artifact published to cluster MLflow and served on kind |
| Kubernetes API | [Approved model deployment](validation/approved-model-deployment.md): verified running image, readiness, real April cohort/native parity, and exact broker event readback | Completed for bundle `98ef1dd9ef4447f7`; preserve identity in subsequent load and failure evidence |
| Serving visibility | [Release monitoring exports](validation/approved-model-operations.md#monitoring-evidence): healthy scrapes, authenticated Grafana, 13 dashboard queries, raw metrics and telemetry figure | Completed; static fallback and undefined feature-age interpretation documented |
| Representative load | [April load on kind](validation/approved-model-operations.md): 10,000 successes at 100/s, P95 7.99 ms, no errors/drops; acknowledgment enabled and 10,020 benchmark/warm-up events matched | Completed for the declared single-node workload; raw samples, provenance, client percentiles, resource observations and identities retained |
| Failure behavior | [Isolated broker failure/recovery](validation/approved-model-operations.md#isolated-broker-failure-and-recovery): 120 explicit 503s, acknowledgment recovered in 6.25 s, unchanged API process, complete benchmark-event readback | Completed at the separately declared lower-rate fault profile; delivery uncertainty and cleanup documented |
| Reproduction and packaging | CLI, Helm, CI and ADRs | Run documented commands from a clean checkout, pin image digests, link measured claims, review limitations, then tag the release |

The defaults specify a 50 ms client P95 objective and the benchmark defaults allow at most 1%
errors; dropped arrivals fail independently. Declare offered load, duration, concurrency and
publication mode before measuring. Preserve failed attempts. Do not weaken model gates or relabel
synthetic evidence to obtain a passing headline.

## Delivery sequence

1. Provision serving monitoring and write its operating runbook (delivered).
2. Build real-data gold partitions and publish a measured comparison (delivered: full evaluation,
   cohort diagnostics, fixed gates and approved static registration). Reproduce using the
   [preparation and validation runbook](runbooks/nullable-passenger-release.md).
3. Deploy the final release model (delivered: [approved model evidence](validation/approved-model-deployment.md)).
   Reproduce using the [deployment runbook](runbooks/deploy-approved-model.md). Keep static and
   streaming results separately attributable.
4. Run held-out traffic and isolated broker failure/recovery (delivered:
   [operational evidence](validation/approved-model-operations.md)). Reproduce using the
   [operations runbook](runbooks/release-operations.md).
5. Finish the concise portfolio README, walkthrough, reproducibility check and tagged release.

The existing January request sample is a tooling validation; January is a configured training
month, so that sample cannot stand in for held-out model evaluation. Historical rolling features
computed offline may be compared for accuracy, but do not prove a live feature producer exists.

## Portfolio presentation

The README should lead with the problem, the implemented release architecture, three or four
measured results, and a short reproduction path. Link each number to its input/model identity,
configuration, raw evidence and limitations. Use the current intended-architecture diagram only
when planned components are explicitly identified.

A walkthrough should show one accepted data partition, the model comparison and promotion
decision, an API prediction with model provenance, live traffic in Grafana, and a documented
failure/recovery result. Explain trade-offs such as acknowledged publication increasing latency,
local single-node infrastructure, and static fallback changing the model used for inference.

Replay, Bytewax windows/recovery, offline/online parity, live ground-truth evaluation, drift-triggered
retraining, request-rate autoscaling, and cloud deployment belong to later releases. Avoid claiming
production capacity, high availability, live ETA accuracy, or streaming correctness from this
release. TLC trip distance is an observed trip field, not an independently validated pre-trip route
estimate; the model's practical serving assumptions remain part of the data/model limitations.
