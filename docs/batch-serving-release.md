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
| Data and leakage controls | Ingestion, lineage, gold feature tests; January real-data workload | Record source checksums, accepted/rejected counts, and all training/holdout months |
| Real-data model comparison | Training/evaluation pipeline implemented; no published real-data comparison | Train January–March 2024, hold out April; publish baseline, static and offline rolling-feature metrics, bucket calibration, model card and gate decisions |
| Model selection for batch serving | Registry promotes streaming candidate; API serves verified static sibling when features are absent | Explicitly evaluate and approve the static model that handles batch-release requests; resolve registry semantics before declaring it production-ready |
| Kubernetes API | Synthetic registry-to-HTTP and broker acknowledgment tests | Deploy the selected real-data artifact; record image identity, bundle checksum, registry provenance and startup/readiness evidence |
| Serving visibility | Opt-in Prometheus and provisioned Grafana dashboard | Capture dashboard and scrape evidence during the release workload; document missing-data and fallback interpretation |
| Representative load | Real-data request builder and synthetic kind/HPA measurements | Use held-out April requests and the real-data model; export raw samples, workload provenance, throughput, P50/P95/P99, errors, dropped arrivals and serving mode |
| Failure behavior | Redis/broker resilience harness | Demonstrate static behavior and broker failure/recovery for the release configuration; retain failure evidence and limitations |
| Reproduction and packaging | CLI, Helm, CI and ADRs | Run documented commands from a clean checkout, pin image digests, link measured claims, review limitations, then tag the release |

The defaults specify a 50 ms client P95 objective and the benchmark defaults allow at most 1%
errors; dropped arrivals fail independently. Declare offered load, duration, concurrency and
publication mode before measuring. Preserve failed attempts. Do not weaken model gates or relabel
synthetic evidence to obtain a passing headline.

## Delivery sequence

1. Provision serving monitoring and write its operating runbook (this increment).
2. Build the real-data gold partitions and publish the model comparison. Inspect memory use for
   monthly feature construction and training before running all months on the laptop.
3. Resolve static-model promotion and deployment based on that comparison. Record the decision
   in an ADR; keep static and streaming results separately attributable.
4. Run held-out traffic through kind with dashboards, acknowledgment enabled, and exported
   benchmark evidence. Record the resource profile and dependency behavior.
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
