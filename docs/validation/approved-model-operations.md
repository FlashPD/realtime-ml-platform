# Approved model: load, monitoring and broker recovery — 2026-09-19

The deployed static model `98ef1dd9ef4447f7-static` passed the predeclared April workload on
local kind: **10,000 successful requests at 100 requests/s**, **7.99 ms client P95**, and **zero
errors or dropped arrivals**, with broker acknowledgment enabled. An isolated broker outage
produced explicit, bounded failures and recovered without restarting the serving process.

See the [machine-readable receipt](approved-model-operations.json),
[reproduction runbook](../runbooks/release-operations.md), and
[approved deployment identity](approved-model-deployment.md).

![Measured client latency, resources, and isolated broker failure](approved-model-operations.png)

The figure uses raw client samples, Kubernetes resource observations, and exported Prometheus
query results. It is an exported telemetry figure, not a Grafana screenshot.
An [SVG version](approved-model-operations.svg) is available for portfolio use.

## Representative load

| Measurement | Result |
|---|---:|
| Offered traffic / measured duration | 100 requests/s / 100 seconds |
| Successful measured requests | 10,000 / 10,000 |
| Successful throughput | 100.00 requests/s |
| Client P50 / P95 / P99 | 5.12 / 7.99 / 11.43 ms |
| HTTP-only P95 | 6.20 ms |
| Maximum observed client latency | 87.28 ms |
| Errors / dropped arrivals | 0 / 0 |
| Acknowledged events independently matched | 10,020 / 10,020, including 20 warm-ups |

A separate in-cluster load pod called the Kubernetes Service using the approved image and
checksummed 10,000-request April sample, including **1,178 unknown passenger counts**. Keep-alive
was enabled, concurrency bounded at 32, and the total request deadline was two seconds. No
requests were retried. Warm-ups are excluded from latency and throughput measurements.

The unchanged gates required client P95 **below 50 ms**, error rate at most **1%**, and no dropped
arrivals. Client latency includes dispatch lag through response validation; HTTP-only latency
excludes dispatch lag. All measured responses identified the approved static model and broker
acknowledgment. The P95 gate does not promise every response below 50 ms: raw samples and the
figure retain the observed tail spikes.

Broker readback compared full prediction hashes, trip keys, schema headers, and prediction IDs.
All 10,020 measured/warm-up successes matched, with no mismatches or duplicate prediction IDs
among those benchmark records. Four preexisting topic records were counted separately.

The serving pod had a one-CPU / 1 GiB limit. Twenty Kubernetes metric observations recorded a
maximum of **248.49 millicores** and **203.23 MiB**. These are windowed sample maxima and may lag;
they are not instantaneous process peaks. The load pod was limited to one CPU / 512 MiB. The
single kind node reported eight CPUs and approximately 7.65 GiB memory. There was no HPA or
saturation sweep, and no training, builds, or repository tests ran during the timed workloads.

## Monitoring evidence

Prometheus and Grafana remain enabled on the main release. Both the main load run and the
isolated failure run verified one healthy per-pod scrape target, authenticated Grafana access,
datasource health, and all **13 queries** from the provisioned ten-panel dashboard. Dashboard
definitions and range-query responses were exported with their exact time windows.

The exported queries show successful traffic and, in the failure environment, HTTP 503 traffic.
Direct before/after counter snapshots reconcile with the client samples: 10,020 successful
requests, acknowledged publications, and disabled feature lookups in the main run. Static-primary
serving deliberately bypasses Redis; 100% static fallback is not an outage. Feature-age series
can be NaN because no online features are used. Missing/undefined intervals are not treated as
zero errors or as evidence of streaming behavior.

## Isolated broker failure and recovery

The failure harness created a separate namespace with the same approved serving image, model,
resource limits, and publication settings, plus its own persistent Redpanda broker and monitoring.
It shared cluster MLflow/MinIO for artifact reads; it did not stop the main broker. The temporary
broker used a 1 GiB volume, compared with the main broker's 5 GiB volume.

| Phase | Traffic | Observed behavior |
|---|---|---|
| Baseline | 2,000 requests at 20/s, plus 20 warm-ups | All successes; client P95 **8.60 ms** |
| Broker stopped | 120 requests at 2/s; no warm-ups | All **HTTP 503**, publication **unconfirmed**; no client timeouts or dropped arrivals |
| Restore broker | Probe until acknowledgment returns | First acknowledged prediction observed **6.25 seconds** after restoration began, within the 60-second objective |
| Recovered load | 2,000 requests at 20/s, plus 20 warm-ups | All successes; client P95 **7.95 ms** |

The broker pod was absent before outage traffic began. Its volume was preserved for restart.
The serving pod UID, container identity, restart count and process start time were identical
before and after the sequence, with zero serving restarts.

Outage client P95 was **1,510.48 ms**; the maximum was **1,511.54 ms**, below the two-second
client deadline. API publication counters recorded **110 acknowledgment timeouts** and **10
delivery failures**, reconciling with all 120 HTTP 503 responses. These are server publication
outcomes, not client request timeouts. The explicit outage probe reported `delivery_unknown=true`.
Two recovery probes also returned unconfirmed responses before acknowledgment resumed.

All **4,040** acknowledged baseline/recovery benchmark events, including warm-ups, matched
broker readback with no mismatches or duplicate IDs. No ambiguous delivery was observed for
the measured outage requests in the captured broker snapshot. One non-benchmark record was
counted separately; standalone probes are outside benchmark-event reconciliation. Failed HTTP
delivery can remain uncertain, so this result is not an exactly-once claim.

The raw outage benchmark correctly fails ordinary success/error gates. Its separate scenario
verdict passes because the intended explicit failure behavior occurred. Gates and results were
not rewritten to make an outage look like healthy serving.

## Cleanup, validation and limits

The temporary load pods and failure namespace were deleted after evidence export. The main
broker pod UID was unchanged, and main serving scrape observations remained up throughout the
isolated failure interval. Main serving, Prometheus, and Grafana remain installed.

The repository checks passed **322 tests**, with six optional service tests skipped and **94.89%**
coverage, plus Ruff, formatting, mypy, shell syntax, and Helm checks. They ran after the timed
loads. The new tests reject outage client timeouts, false acknowledgments, dropped arrivals,
wrong models, latency violations, bad warm-ups, and altered broker prediction payloads.

There was one main load run and one isolated failure suite; all phase outputs were retained.
Raw data, client samples, consumed prediction records, configuration, resource observations,
monitoring exports and cleanup receipts are under
`artifacts/releases/batch-operations-20260919/`. The checked-in JSON records their hashes and the
executed driver source. These large local artifacts remain ignored by Git.

This is a declared workload on single-node local infrastructure, not production capacity,
high availability, request-rate autoscaling, streaming parity, or live ETA accuracy. Failure
traffic is lower than the main 100/s profile. Clean-checkout reproduction, accessible release
artifacts, the short portfolio walkthrough and release tagging remain outstanding.
