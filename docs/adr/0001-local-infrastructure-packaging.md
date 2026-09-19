# ADR-0001: Package lightweight infrastructure for the local profile

- Status: accepted
- Date: 2026-09-14

## Context

The showcase must run on a single kind node with 10–12 GB available to the container runtime.
Installing an operator and a full upstream chart for every stateful dependency adds controllers,
custom resources, and default resource reservations that do not improve the local demonstration.
The available upstream charts also have different lifecycle and secret-management conventions.
In particular, MinIO moved its official distribution to source-only and archived its community
Helm chart, making that chart a poor long-term foundation.

The platform still needs a repeatable Kubernetes deployment, persistent local data, health
checks, resource boundaries, versioned configuration, and an honest production boundary.

## Decision

The `local` profile packages single-node PostgreSQL, Redis, MinIO, and Redpanda workloads as
small templates in the TripML application chart.

- Images use immutable release tags; digest locking will be added before the first showcase
  release.
- Credentials are generated once by the cluster bootstrap and stored in a pre-existing
  Kubernetes Secret. Helm never renders secret values.
- Every workload declares requests, limits, persistent storage, and health probes.
- Helm test hooks exercise PostgreSQL connectivity, authenticated Redis access, MinIO and schema
  registry health, Redpanda cluster health, and a Kafka produce/consume round trip.
- The local chart is explicitly not a production topology: it has one replica per stateful
  service and no automated backup, failover, or in-place upgrade controller.

A production or cloud profile will use managed data services or maintained Kubernetes operators,
while first-party application workloads remain in the TripML chart.

## Consequences

The local stack is small, deterministic, and understandable in a portfolio review. It also starts
without four additional operators and keeps credential handling consistent.

TripML owns the maintenance and upgrade testing of these local manifests. This is acceptable only
for the local showcase profile; the templates must not be presented as a production-ready database
or broker deployment.

## Revisit when

- A cloud or multi-node profile is implemented.
- Stateful upgrade and backup demonstrations enter project scope.
- An upstream chart becomes both lightweight and consistent with the runtime-generated secret
  model.
