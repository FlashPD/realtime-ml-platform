# Static-model promotion validation — 2026-09-18

The real-data static pilot is now registered as **version 1** of
`tripml-trip-duration-static-pilot` in an isolated local MLflow registry. The API resolved its
`production` alias, verified the native artifact, reported **`static_primary`**, and returned a
prediction using only the approved static model. No Kubernetes deployment was performed.

This closes the implementation gap between static-model eligibility and an explicitly selected,
guarded deployment. See the [machine-readable evidence](static-model-promotion.json),
[decision record](../adr/0017-explicit-static-model-promotion.md), and
[reproduction runbook](../runbooks/static-model-promotion.md).

## Real-data run

| Measurement | Result |
|---|---:|
| January training rows | 2,757,364 |
| February evaluation rows | 2,755,007 |
| Baseline MAE | 258.81 seconds |
| Selected static-model MAE | 172.85 seconds |
| MAE improvement | 33.21% (15% minimum) |
| Worst distance-bucket calibration error | 7.98% (10% maximum) |
| Local single-row inference P95 | 0.103 ms (5 ms maximum) |
| Training and publication wall time | 475.07 seconds |
| Peak child-process RSS | 1.974 GiB |

Bundle: `95a6a112e37a7013`. Training used the original pilot's accepted gold files, all rows,
800 trees and seed 42. Neither data nor model thresholds changed. Both native LightGBM model
files are **byte-identical to the original pilot**; the new bundle identity records the static
promotion role and version-3 evidence. No incumbent existed in this separate registry, so this
run establishes initial registration, not an improvement over an incumbent.

Timing is a single observation on the shared 16 GB ARM64 macOS laptop. Inference P95 excludes
HTTP, feature lookup and broker delivery. Peak RSS is the largest child-process high-water mark,
not aggregate process-tree memory. The inference timing difference from the original pilot is
not evidence of a model optimization: the model bytes are identical.

## Registry and application checks

- The registered source run is `static_candidate`, and its artifact digest matches the bundle.
- Repeating publication reuses version 1, creates no extra version, and leaves the alias unchanged.
- Startup resolves the registry alias and reports `registry_version=1`, `mode=static_primary`,
  and a null streaming-model version.
- The preserved February request returns HTTP 200 and an estimate of **698.53 seconds**, matching
  the original pilot's static prediction. Its provenance identifies `95a6a112e37a7013-static`.
- A configured Redis URL of `redis://127.0.0.1:1` does not enable lookups. Metrics record the
  lookup outcome as disabled. The response contains five static inputs and no feature timestamps.
- A negative-distance request returns HTTP 422.

These application checks use FastAPI's in-process TestClient. Prediction publication is disabled;
this run does not measure network latency, broker acknowledgment or cluster capacity. The
version-1 `feature_fallback=true` response field records intentional static feature usage here.

The quality gate passed **279 tests**, with six optional external-service tests skipped and
**95.46% coverage**, plus Ruff, mypy and Helm validation. New regression coverage includes static
selection and cache isolation, failed gates, mismatched roles, tampered artifacts/evidence,
stale incumbents, holdout mismatch, publication retries, alias rollback prevention, Redis bypass,
and registry-to-HTTP inference. A separate role-isolation regression was tightened and rerun
after the full gate, along with lint and formatting checks.

## Provenance and limits

The JSON snapshot preserves the training report, configuration, registry receipts, retry result,
request/response, resource measurements and source-code hashes. Six native model, baseline,
model-card and gold files were checked against their recorded digests. It retains the original
manifest digest while normalizing absolute repository paths for publication. Raw command logs,
the validation helper and metrics remain under
`artifacts/releases/static-promotion-20260918/`; the local registry lives under
`artifacts/mlflow/static-pilot/`. Both directories are ignored by Git.

March and April remain quarantined. This does not complete the planned January–March / April
release evaluation. February is observed pilot data, and TLC distance is a completed-trip field;
these results do not validate a pre-trip distance estimator. Live streaming parity, representative
Kubernetes load, and the final release deployment remain separate milestones.
