# ADR-0013: In-cluster load and CPU autoscaling evidence

- Status: Accepted
- Date: 2026-09-17

## Context

Local host-to-container tests exercise online inference, broker acknowledgments, and dependency
recovery. They do not exercise Kubernetes Service routing or the deployed HPA. A rendered HPA and a
successful deployment smoke test cannot establish scaling behavior or serving latency under load.

## Decision

Extend the existing opt-in kind deployment test with `make serving-load-test IMAGE=... OUTPUT=...`.
Require an explicit kind context, a loaded serving image, a new output directory, and a functioning
Metrics API before creating test workloads. Provide `make metrics-server` for the pinned upstream
3.13.0 chart in local kind; the kubelet TLS exception is limited to that development context. Metrics
Server is a retained cluster prerequisite, separate from temporary test resources.

Use the actual serving chart with its default 100m CPU request, one-core limit, 70% CPU target,
1–3 replicas, and 300-second scale-down stabilization. The test owns a temporary namespace, MLflow
registry, synthetic model, credential, Redis StatefulSet/PVC, and load pod. Reuse the existing local
Redpanda broker through a unique topic, deleting only that topic afterward. No platform model alias
or Redis key is modified. The original smoke-only profile retains its previous shared-Redis behavior.

Run the existing fixed-arrival benchmark inside the cluster against the serving Service: 18,000
requests at 100 requests/second, 20 warm-up requests, acknowledged publication, P95 below 50 ms,
at most 1% errors, and static fallback below 1%. Allow 256 in-flight requests, covering the 100/s
arrival rate times the two-second request timeout with headroom. The first observed run used the
general client's default cap of 32 and dropped 78 arrivals during initial overload; retain that
failed evidence. Raising client concurrency does not relax the offered rate, latency/error gates,
or the rule that any client overflow fails the run. Seed feature snapshots into the dedicated Redis
with a 30-minute TTL. Historical event time remains fixed; TTL expiry should not confound the test.

Disable HTTP keep-alive explicitly for this profile. Service routing selects backends for new
connections; a small persistent client connection pool can keep sending traffic to the original
pod after scale-up. Charge connection setup to measured latency and record the choice in benchmark
configuration. Ordinary benchmark runs retain pooled keep-alive behavior. This profile is not a
universal production traffic model.

Collect timestamped HPA state, Deployment readiness, pod status, and raw Metrics API samples roughly
every five seconds. Require one ready replica with usable CPU metrics before load, an HPA request for
at least two replicas during load, at least two ready replicas during load, and actual successful
prediction counters on multiple pods. After traffic stops, allow up to eight minutes to observe the
return to one ready/desired replica. Do not shorten stabilization to produce a faster demo.

Export the client's raw evidence, per-pod request metrics, cluster/node version information, Helm
values, pod image IDs, input scripts/snapshots, registry model artifacts, events, and observations.
Generate a report with separate workload and scaling gates and checksums of its evidence inputs.
Keep diagnostic exports on failure. Cleanup of the unique broker topic and namespace runs even if
diagnostic export fails; never delete the existing cluster or its platform services.

## Validation and limits

Tests reject missing CPU observations, desired replicas without ready capacity, pods that never
served traffic, missing baseline, and absent scale-down evidence. The real kind run validates the
deployment path and reports the observed objectives, including failures. The load pod has a bounded
CPU budget; client dispatch lag and overflow remain visible rather than silently lowering the rate.

A single-node kind cluster shares CPU and memory with the existing platform and a synthetic MLflow
registry. The workload is synthetic and short, so results do not establish real-data model quality,
feature parity, sustained production capacity, or high availability. Publication is verified through
the API acknowledgment contract; independent broker readback is covered by the resilience suite.
Request-rate scaling and monitoring dashboards remain separate work. Scale events are observed with
polling resolution, not claimed as precise controller decision timestamps.

## References

- [Kubernetes HPA algorithm and stabilization](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
- [Metrics Server requirements and compatibility](https://kubernetes-sigs.github.io/metrics-server/)
