# Approved model deployed on kind — 2026-09-19

The approved static bundle `98ef1dd9ef4447f7` is deployed in `kind-tripml`, namespace `tripml`.
Helm revision **13** is deployed, and the serving pod is ready with zero restarts at capture.
The API loads version **1** of `tripml-trip-duration-static-v2` from cluster MLflow, backed by
PostgreSQL and MinIO. It does not require the author's host SQLite registry to serve predictions.

See the [machine-readable receipt](approved-model-deployment.json) and the
[deployment runbook](../runbooks/deploy-approved-model.md).

## Verified behavior

| Check | Result |
|---|---|
| Approved identity | `98ef1dd9ef4447f7-static`, registry version 1, `static_primary`, `gold-features-v2` |
| Artifact integrity | Original configuration, native bundle, four gold inputs, and checksummed April workload verified before publication |
| Serving image | Running image ID `sha256:16bc98e7d329a2a4065de6b1fedc5f240feaae40d319e887cac9eb65adf6c991`; 21 packaged Python files match approved runtime evidence |
| Registry retry | Reused version 1 without moving the alias or creating another model version |
| Helm serving smoke | Passed health, readiness, metrics, and acknowledged prediction checks |
| Real April requests | Known-positive, zero, and unknown passenger-count requests all matched the native model prediction and preserved feature values |
| Broker delivery | All three HTTP responses declared acknowledgment; consumed events matched complete prediction payloads, trip keys, schema versions, and prediction IDs |
| Legacy contract | Schema 1.0 with unknown passenger count returned HTTP 422 |

The cohort checks ran inside the serving pod against the Kubernetes **Service** address, then
consumed from `predictions-batch-release-v2`. They used unique validation trip IDs and did not
commit consumer offsets. The ordinary Helm smoke added one more acknowledged prediction.
Static-primary serving bypasses Redis; its fallback flag means static feature usage.

## Deployment fixes and retained attempts

The older local platform lacked an MLflow database connection key. It was added using the
existing PostgreSQL credentials, without rotating them. Enabling the current chart also rolled
the Airflow deployment; all three Airflow containers were ready afterward.

The first publication attempt failed before writing model runs because MLflow's allowlist rejected
`127.0.0.1:15000`. The chart now permits localhost ports and still rejects foreign hostnames;
a regression test exercises MLflow's actual host matcher.

The second attempt successfully registered version 1, but its host-side artifact check stalled
on a presigned MinIO URL reachable only inside Kubernetes. That process was stopped and its
publication receipt retained. The third attempt set `MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD=false`
to download through forwarded MLflow; registry/API validation and idempotent publication passed.
Training settings and promotion evidence were unchanged throughout.

The validator now accepts a separate destination URI after checking the original configuration.
This preserves the training fingerprint and lets the remote registry choose its artifact location.
The repository suite passed **312 tests**, with six optional service tests skipped and **94.89%**
coverage. Ruff, formatting, mypy, and Helm checks passed. The Helm test and real-model cluster
probe above were run separately against the deployed system.

## Scope and access

Run `make serving-ui` from the repository, then open `http://localhost:8000/docs` or
`http://localhost:8000/readyz`. The services remain deployed; the temporary publication port
forward was stopped. No public ingress was created.

Raw attempts, registry receipts, resource snapshots, metrics, and logs are retained under
`artifacts/deployments/batch-release-98ef1dd9ef4447f7/`; the checked-in receipt records their hashes.
These local raw files are ignored by Git. The directory spans publication on September 18 local
time and final serving deployment on September 19.

This completes deployment correctness for the approved artifact. Three cohort requests and a
Helm smoke do not establish latency under load, capacity, high availability, or failure recovery.
The April load run, monitoring capture, broker failure/recovery, and clean-checkout release
walkthrough remain pending.

Subsequent validation on September 19 completed the [load, monitoring and isolated broker
failure/recovery checks](approved-model-operations.md). Clean-checkout reproduction and release
packaging remain outstanding.
