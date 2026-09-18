# Trip-duration model card: d05498ef10a1e365

## Intended use

Estimate yellow-taxi trip duration at pickup time for this portfolio platform. It is not suitable
for pricing, employment, enforcement, or decisions about individual passengers or drivers.

## Data

- Training months: 2024-01 (2,757,364 rows)
- Holdout month: 2024-02 (2,755,007 rows)
- Feature model: gold-features-v1

## Holdout results

| Model | MAE (s) | RMSE (s) | MAPE (%) | Inference P95 (ms) |
|---|---:|---:|---:|---:|
| Hierarchical median | 258.81 | 464.71 | 32.69 | 0.070 |
| Static LightGBM | 172.85 | 308.69 | 20.04 | 0.256 |
| Streaming-feature LightGBM | 164.60 | 293.30 | 19.67 | 0.220 |


## Distance-bucket diagnostics

Calibration is the absolute difference between predicted and actual bucket means, divided by
the actual mean. Empty buckets report N/A. It is not a confidence-interval coverage metric.

| Model | Miles (lower inclusive) | Rows | MAE (s) | Calibration error (%) |
|---|---|---:|---:|---:|
| Baseline | [0, 2.0) | 1,597,189 | 166.04 | 1.33 |
| Baseline | [2, 5.0) | 736,053 | 271.52 | 12.32 |
| Baseline | [5, 10.0) | 216,141 | 527.47 | 20.10 |
| Baseline | [10, inf) | 205,624 | 651.47 | 16.18 |
| Static | [0, 2.0) | 1,597,189 | 116.90 | 7.31 |
| Static | [2, 5.0) | 736,053 | 186.80 | 6.10 |
| Static | [5, 10.0) | 216,141 | 292.16 | 7.83 |
| Static | [10, inf) | 205,624 | 432.03 | 7.98 |
| Offline rolling | [0, 2.0) | 1,597,189 | 112.62 | 5.19 |
| Offline rolling | [2, 5.0) | 736,053 | 178.47 | 4.39 |
| Offline rolling | [5, 10.0) | 216,141 | 280.10 | 6.51 |
| Offline rolling | [10, inf) | 205,624 | 397.35 | 6.08 |


## Static-model eligibility

**PROMOTE** against baseline,
calibration and latency gates. This diagnostic does not register or promote the static model,
and does not compare it with a streaming-model incumbent.

| Gate | Observed | Threshold | Passed |
|---|---:|---:|:---:|
| mae_improvement_vs_baseline_pct | 33.213 | 15.000 | yes |
| max_distance_bucket_calibration_error_pct | 7.979 | 10.000 | yes |
| inference_p95_ms | 0.256 | 5.000 | yes |


## Streaming-candidate promotion decision

**PROMOTE** `d05498ef10a1e365-streaming`.

| Gate | Observed | Threshold | Passed |
|---|---:|---:|:---:|
| mae_improvement_vs_baseline_pct | 36.399 | 15.000 | yes |
| max_distance_bucket_calibration_error_pct | 6.507 | 10.000 | yes |
| inference_p95_ms | 0.220 | 5.000 | yes |


## Limitations

The data represents completed medallion-taxi trips and local wall-clock timestamps. Performance can
shift across seasons, policy changes, vehicle types, and unusual demand. Online feature parity and
live error monitoring are separate acceptance gates before serving claims are made.
Trip distance is the observed completed-trip distance; pre-trip use requires an external route
estimate whose accuracy is not validated here. Rolling features in this comparison are computed
offline, not supplied by a live stream processor. Timing is local single-row model inference,
not end-to-end HTTP latency.
