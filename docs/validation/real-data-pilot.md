# Real-data model pilot — 2026-09-17

The static LightGBM candidate reduced February holdout MAE by **33.21%** against the hierarchical
median baseline, from **258.81 to 172.85 seconds**. Both candidates passed the unchanged baseline,
distance-bucket calibration and local inference-latency gates. This was an offline pilot:
**no model was registered, promoted in MLflow, or deployed**.

Training used all **2,757,364** accepted January rows and evaluation used all **2,755,007** accepted
February rows. December supplied only January's lookback context. No training or evaluation rows
were subsampled. The 800-tree, 63-leaf, learning-rate-0.05, seed-42 configuration was fixed before
evaluation. Model bundle: `d05498ef10a1e365`.

## Measured comparison

| Model | MAE (s) | RMSE (s) | MAPE (%) | MAE reduction vs baseline | Worst bucket calibration error | Local inference P95 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| Hierarchical median | 258.81 | 464.71 | 32.69 | — | 20.10% | 0.070 |
| Static LightGBM | 172.85 | 308.69 | 20.04 | 33.21% | 7.98% | 0.256 |
| Offline rolling-feature LightGBM | 164.60 | 293.30 | 19.67 | 36.40% | 6.51% | 0.220 |

The gates require at least 15% MAE improvement, at most 10% bucket calibration error and at most
5 ms inference P95. There was no incumbent comparison. Static eligibility is recorded separately
from the streaming-candidate decision; neither diagnostic changes a registry alias.

The rolling-feature model improved on static MAE by another 8.24 seconds (4.77%). That is offline
evidence of potential value, not proof of a live streaming benefit. Static MAE remains 432.03 seconds
on the 205,624 holdout trips of at least ten miles, versus 116.90 seconds below two miles. The
[model card](real-data-pilot-model-card.md) reports every bucket's count, MAE and calibration; the
[machine-readable snapshot](real-data-pilot.json) also contains actual and predicted bucket means.

## Why the split changed for this pilot

| Partition | Source rows | Valid rows under the row contract | Invalid share | Partition decision |
|---|---:|---:|---:|---|
| December 2023 | 3,376,567 | 3,119,736 | 7.61% | Accepted context |
| January 2024 | 2,964,624 | 2,757,364 | 6.99% | Accepted training |
| February 2024 | 3,007,526 | 2,755,007 | 8.40% | Accepted evaluation |
| March 2024 | 3,582,628 | 3,076,672 | 14.12% | Whole partition quarantined |
| April 2024 | 3,514,289 | 3,026,562 | 13.88% | Whole partition quarantined |

March contains 426,190 null passenger counts; April contains 408,576. The other required fields
have no nulls in the audited sources. Those two months exceed the existing 10% partition violation
limit and have no published silver output. Their valid-row counts are diagnostic, not usable silver.

The pilot split was explicitly chosen after the source-quality audit and before model evaluation.
Neither the row contract nor any data/model threshold was relaxed. This does not complete the
planned January–March / April release evaluation. February is now observed pilot data; subsequent
model tuning needs a fresh final holdout.

## Resource and application checks

| Command | Wall time | Peak child-process RSS |
|---|---:|---:|
| January gold build | 64.22 s | 1.952 GiB |
| February gold build | 58.40 s | 1.891 GiB |
| Full training and comparison | 494.49 s | 1.922 GiB |

These are single-run observations on a shared 16 GB ARM64 macOS laptop. Gold used a 2 GB DuckDB
buffer-manager limit, 8 GB spill limit and two workers. LightGBM training was configured with one
thread; this does not constrain every preprocessing/evaluation library in the process. Inference
P95 uses 500 held-out single-row calls after warm-up and excludes HTTP, feature lookup and broker
delivery. No confidence intervals or repeat-run stability claims are made.

The application smoke test loaded the verified native bundle, reported ready with a null registry
version, and returned HTTP 200 using the static model for a real February request. An invalid
negative-distance request returned HTTP 422. Publication was disabled. This used FastAPI's
in-process TestClient, not a network or Kubernetes load test.

The repository quality gate passed: **260 tests, six optional service tests skipped, 95.17% coverage**,
plus Ruff, mypy and Helm validation. Regression tests cover completion-time pruning boundaries,
resource settings, bucket diagnostics and static rejection when a streaming candidate passes.

## Provenance and reproduction

Follow the [pilot runbook](../runbooks/real-data-pilot.md) with the
[explicit configuration](../../examples/training/real-data-pilot.yaml). The snapshot includes source
quality reports, source null counts, gold manifests, model metrics, gate decisions, dependencies,
resource measurements, application smoke output and source-code checksums. Fourteen source, silver,
gold and model files were checked against their recorded SHA-256 values before export. Absolute
repository paths are normalized in the snapshot; original manifest/report digests are retained.

Raw command logs and measurements remain under `artifacts/releases/batch-comparison-20260917/`;
the native bundle remains under `artifacts/training/d05498ef10a1e365/`. Both are ignored by Git.
The checked-in model card is byte-identical to the bundle's checksummed model card. Rebuilding gold
can change physical row order and file hashes; retain the captured inputs for byte-level comparisons.

An initial feature-build attempt failed because the dbt adapter tried to reset a spill directory
after it had been used. A second build completed, but macOS `/usr/bin/time -l` could not read a
sandbox-restricted sysctl. Those logs are retained; the final measurements above use the corrected
DuckDB settings and the Python resource-measurement helper.

TLC distance is observed completed-trip distance. These results do not validate a pre-trip distance
estimator, April accuracy, live feature parity, HTTP latency, production capacity or high availability.
