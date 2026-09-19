# ADR-0015: Local serving monitoring

Status: Accepted

## Context

The batch-serving release needs visible operational behavior. The API already exports metrics,
but no chart workload collects them and there is no provisioned dashboard. The existing benchmark
measures client latency and remains the evidence source for release performance claims.

## Decision

Add opt-in Prometheus and Grafana deployments to the local chart. Use Kubernetes pod discovery,
restricted to this release's serving labels and namespace, with namespace-scoped pod-read RBAC.
Scrape the named HTTP port on each serving container so replica counters retain distinct identities.
Do not scrape a load-balanced Service as if it were a single process.

Provision the datasource and dashboard from ConfigMaps. Aggregate histogram buckets before
computing quantiles and apply rates before summing counters across pods. Use explicit descriptions
for server latency, static fallback, event-time freshness and scrape health. Preserve undefined
no-traffic ratios; absent errors can resolve to zero only against existing response traffic.

Keep services private to the cluster, disable anonymous Grafana access, and reference a separately
created admin Secret. Bound resources and retention for the laptop profile. Persist short-lived
Prometheus history, but rebuild Grafana's ephemeral state from provisioned files. A configuration
checksum restarts workloads on chart changes.
Disable Grafana's startup plugin installation and automatic updates; the pinned image includes the
Prometheus plugin. This avoids unpinned plugin changes and catalog network calls during startup.

## Consequences

The release gets reproducible operational visibility without a monitoring operator or cluster-wide
permissions. It does not yet provide Kubernetes state exporters, request-rate scaling, alert routing,
pipeline/model-quality dashboards, long-term metrics, or a shared-environment authentication boundary.
Scrape success cannot substitute for replica readiness or availability evidence.

Chart tests cover discovery scope, configuration, credentials and resource constraints. An opt-in
kind test checks two actual API processes, metric collection, dashboard provisioning and queries.
The operating runbook documents installation, interpretation, retention and teardown.

References: [Prometheus discovery configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#kubernetes_sd_config),
[Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).
