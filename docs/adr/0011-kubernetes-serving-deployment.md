# ADR-0011: Opt-in Kubernetes serving deployment

- Status: Accepted
- Date: 2026-09-16

## Context

Serving can load a verified model bundle, read Redis snapshots, and publish acknowledged predictions.
It needs a reproducible container and deployment boundary. A new cluster has no production alias,
so enabling model-dependent serving in the initial Helm install would prevent that install from
becoming ready before training can run.

## Decision

Keep `serving.enabled=false` by default. After a passing training run publishes to cluster MLflow,
`make serving-deploy` builds the image, tags it with its content-derived Docker image ID, loads it
into kind, and enables the Deployment and ClusterIP Service. Merge new chart defaults with previous
release overrides during upgrades. Roll back failed Helm upgrades; run a serving-only Helm test
after readiness. A test failure after a successful rollout remains an explicit command failure and
does not automatically roll the release back.

The image installs the serving extra and OpenMP runtime on the same Python 3.12 Debian base used by
the local MLflow image. It runs as a dedicated UID/GID 10001. Kubernetes drops all capabilities,
disables privilege escalation and service-account token mounting, applies RuntimeDefault seccomp,
and mounts the root filesystem read-only. A size-limited `/tmp` volume supports downloaded model
artifacts and runtime caches. Dependency ranges remain the package's existing ranges; content tags
identify an exact built image but are not a dependency lockfile or a signed supply-chain guarantee.

Default service addresses resolve within the Helm release. Explicit MLflow, Redis, and broker
address overrides permit an isolated test registry or externally managed dependencies. Redis's
generated hex password is injected through a Secret reference and expanded into the secret URL by
Kubernetes. No plaintext credential is rendered in the chart. Startup/readiness use `/readyz` and
liveness uses `/healthz`; broker errors remain request-level 503s under ADR-0010.

An init container calls `tripml publication ensure-topic`. This explicit provisioning operation
creates a missing topic and tolerates concurrent create attempts. It verifies partition/replica
counts for an existing topic and fails on mismatch rather than changing its layout. Broker API waits
are bounded; Kubernetes retries a failed init container. Application producers still disable
automatic topic creation. Broker ACLs for a shared deployment would separate this provisioning role
from a writer-only serving role.

Use rolling updates with `maxUnavailable=0` and `maxSurge=1`. A bad new model cannot replace the ready
old pod. This is not a cross-pod model-version pin: each pod resolves the alias at startup, and the
returned model version provides provenance during overlap. Operators must serialize alias changes
and rollouts when they need a single selected model revision. There are no operator HTTP endpoints.

Provide an optional autoscaling/v2 CPU HPA, defaulting to 1–3 replicas at 70% CPU utilization. Require
CPU requests and omit Deployment replicas when HPA is enabled. Check Metrics Server availability
in the deployment command before enabling HPA. The chart does not install cluster-wide metrics
infrastructure implicitly. A documented pinned upstream Metrics Server install supports local kind;
its kubelet TLS bypass is limited to that development environment. Request-rate scaling requires the
planned monitoring adapter and remains pending.

## Validation and limits

Helm lint and tests render both fixed-replica and HPA profiles, enforce dependency/settings validation,
and check credentials, resources, health probes, and security fields. Kubernetes server-side dry run
validates the Deployment, Service, and HPA schemas. Unit tests cover topic creation, repeat execution,
concurrent provisioning, mismatched layouts, and command configuration.

The opt-in kind test uses the real serving image, a synthetic native-model training run, and a
temporary MLflow server with proxied artifacts. It installs the serving chart in a unique namespace,
reads the local platform's existing Redis credential, and publishes through its existing Redpanda
broker into a unique topic. The test removes that topic and namespace, including the temporary
registry; it never promotes a synthetic model into the real platform registry. This checks deployment
correctness, not real-data model quality or latency. The standalone Helm test emits a real smoke
prediction with trip ID `helm-serving-smoke`; future evaluation must exclude it.

The temporary MLflow server exceeded a 1 GiB limit and was OOM-killed. With a 2 GiB limit it completed
the workflow and used approximately 1.6 GiB of cgroup memory. Set the chart's registry request to
1 GiB and limit to 2 GiB based on that observation; this is a local sizing baseline, not a production
capacity measurement. The persistent PostgreSQL/MinIO registry topology still needs its own load test.

Full cluster CI, autoscaling load measurements, real-data model evidence, monitoring, and authenticated
operator interfaces remain separate milestones. A single local node does not provide high availability.

## References

- [Kubernetes HPA behavior and prerequisites](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
- [Metrics Server requirements](https://kubernetes-sigs.github.io/metrics-server/)
