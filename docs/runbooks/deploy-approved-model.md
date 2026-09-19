# Deploy the approved static model on local kind

This runbook deploys bundle `98ef1dd9ef4447f7` without retraining or changing its promotion gates.
It uses the existing `kind-tripml` cluster, `tripml` Helm release/namespace, PostgreSQL-backed
MLflow, and MinIO artifacts. It does not expose an internet endpoint.

## Prerequisites

Install the development dependencies and prepare the approved bundle and checksummed April
workload using the [release runbook](nullable-passenger-release.md). Native artifacts and data
are ignored by Git; a checkout alone does not contain them. A newly reproduced model must use
its own manifest and evaluation evidence, rather than inheriting this run's identity.

For a fresh local platform, `make cluster` builds/loads infrastructure images and creates the
credentials, including `mlflow-database-url`. For an older running platform, preserve the existing
Secret and add that missing key from its existing PostgreSQL credentials before enabling MLflow.
Never put credential values into Helm values, logs, or checked-in evidence.

Enable MLflow before enabling serving:

```bash
.tools/bin/helm upgrade tripml deploy/helm/tripml \
  --kube-context kind-tripml --namespace tripml --reset-then-reuse-values \
  --set mlflow.enabled=true --wait --timeout 5m --rollback-on-failure
```

## Publish the existing artifact

Forward MLflow in a separate terminal:

```bash
kubectl --context kind-tripml -n tripml port-forward service/tripml-tripml-mlflow 15000:5000
```

The chart permits localhost ports while retaining its host allowlist. Keep the original training
configuration; the explicit `--tracking-uri` changes only the destination after evidence verification.
Choose a new output path for each attempt:

```bash
MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD=false .venv/bin/python scripts/validate-release-model.py \
  --config examples/training/batch-release.yaml \
  --training-report artifacts/training/passenger-v1_1/98ef1dd9ef4447f7/manifest.json \
  --workload artifacts/workloads/april-passenger-v1_1 \
  --tracking-uri http://127.0.0.1:15000 \
  --output artifacts/deployments/my-run/registry-validation
```

The environment setting keeps host-side artifact downloads on the forwarded MLflow connection.
Auto-detected direct downloads can otherwise return MinIO URLs reachable only inside Kubernetes.
It does not change training settings or the configuration fingerprint.

The validator checks the native bundle, configuration, fixed gates, gold checksums, and workload
lineage before publishing. It records the destination separately, retains the original training
fingerprint, publishes through the existing incumbent checks, and verifies an idempotent retry.
The static model name is `tripml-trip-duration-static-v2`. This command also compares native and
in-process API predictions for known, zero, and unknown passenger counts; broker publication is
disabled only for that preliminary smoke.

If the destination contains an incompatible incumbent, stop and inspect it. Do not delete its
alias or change the evidence to force promotion. A failed attempt may have written to MLflow;
retain its receipt and use a new output directory for retry.

## Load the verified image and deploy

The release overlay names the image used in the recorded deployment by its full local image ID.
On the prepared laptop, verify and load it:

```bash
docker image inspect tripml-serving:batch-release-d6ff2c0 --format '{{.Id}}'
# Must match sha256:16bc98e7d329a2a4065de6b1fedc5f240feaae40d319e887cac9eb65adf6c991.
docker tag tripml-serving:batch-release-d6ff2c0 \
  tripml-serving:16bc98e7d329a2a4065de6b1fedc5f240feaae40d319e887cac9eb65adf6c991
.tools/bin/kind load docker-image \
  tripml-serving:16bc98e7d329a2a4065de6b1fedc5f240feaae40d319e887cac9eb65adf6c991 --name tripml

.tools/bin/helm upgrade tripml deploy/helm/tripml \
  --kube-context kind-tripml --namespace tripml --reset-then-reuse-values \
  --values deploy/helm/tripml/values-batch-release.yaml \
  --wait --timeout 5m --rollback-on-failure
```

A local tag is not an immutable registry digest. Verify the running pod's `imageID` and preserve
it in deployment evidence. On a new machine, build from the recorded runtime source using
`docker/serving/Dockerfile`, record the new image/package identities, and override
`serving.image.tag` with the new loaded tag. Do not label a rebuilt image with the old image ID.

The overlay selects one static-primary serving replica, no HPA, and the dedicated topic
`predictions-batch-release-v2`. Existing infrastructure is retained. MLflow uses persistent
PostgreSQL and MinIO storage; the host SQLite registry is not a runtime dependency.

## Verify the actual deployment

```bash
.tools/bin/helm test tripml --kube-context kind-tripml --namespace tripml \
  --filter name=tripml-tripml-test-serving --logs --timeout 3m

kubectl --context kind-tripml -n tripml exec -i deployment/tripml-tripml-serving -- \
  python -c "$(cat scripts/check-deployed-model.py)" \
  --base-url http://tripml-tripml-serving:8000 \
  --bootstrap-servers tripml-tripml-redpanda:9092 \
  --topic predictions-batch-release-v2 \
  < artifacts/deployments/my-run/registry-validation/validation.json \
  > artifacts/deployments/my-run/deployed-validation.json
```

The second command checks the real Service endpoint, static model/registry identity, native/API
agreement for all three count cohorts, acknowledgment headers, and exact broker event/key/schema
readback. It also checks that schema 1.0 rejects a null passenger count. It uses unique trip IDs,
does not commit consumer offsets, and does not delete the topic. Preserve stderr and the command's
exit status when capturing a failed attempt; a receipt is printed only when every check passes.

For interactive access, run `make serving-ui` from the repository. Open
`http://localhost:8000/docs` or fetch `http://localhost:8000/readyz`. Readiness must show
`model_version=98ef1dd9ef4447f7-static`, `mode=static_primary`, and `gold-features-v2`.
`feature_fallback=true` means intentional static feature usage in this mode; Redis is bypassed.

Stop temporary port forwards with Ctrl-C when finished. Leave the deployed services running.
To disable only the API, apply `--set serving.enabled=false` with `--reset-then-reuse-values`;
retain MLflow, its data, and the prediction topic. Do not delete the cluster as API teardown.

This smoke establishes deployment correctness. Representative load, monitoring captures, broker
failure/recovery, and clean-checkout reproduction remain separate release acceptance checks.
