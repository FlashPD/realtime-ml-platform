# Trip-duration model card: 98ef1dd9ef4447f7

## Intended use

Estimate yellow-taxi trip duration at pickup time for this portfolio platform. It is not suitable
for pricing, employment, enforcement, or decisions about individual passengers or drivers.

## Data

- Training months: 2024-01, 2024-02, 2024-03 (9,316,058 rows)
- Holdout month: 2024-04 (3,419,441 rows)
- Feature model: gold-features-v2

## Holdout results

| Model | MAE (s) | RMSE (s) | MAPE (%) | Inference P95 (ms) |
|---|---:|---:|---:|---:|
| Hierarchical median | 252.20 | 447.89 | 29.74 | 0.105 |
| Static LightGBM | 187.18 | 328.04 | 19.83 | 0.279 |
| Streaming-feature LightGBM | 177.62 | 309.12 | 19.64 | 0.462 |


## Passenger-count missingness

Unknown counts remain null in data and use native missing-value routing in LightGBM.
Zero is an observed count, not an imputation. Training contains
727,015 unknown counts; holdout contains
392,879. Cohort errors are diagnostics, not new promotion gates.

| Passenger count | Holdout rows | Baseline MAE (s) | Static MAE (s) | Rolling MAE (s) |
|---|---:|---:|---:|---:|
| Known | 3,026,562 | 252.16 | 188.87 | 178.71 |
| Unknown | 392,879 | 252.48 | 174.17 | 169.19 |


## Distance-bucket diagnostics

Calibration is the absolute difference between predicted and actual bucket means, divided by
the actual mean. Empty buckets report N/A. It is not a confidence-interval coverage metric.

| Model | Miles (lower inclusive) | Rows | MAE (s) | Calibration error (%) |
|---|---|---:|---:|---:|
| Baseline | [0, 2.0) | 1,857,951 | 168.95 | 3.86 |
| Baseline | [2, 5.0) | 980,175 | 253.60 | 11.05 |
| Baseline | [5, 10.0) | 312,447 | 429.70 | 13.29 |
| Baseline | [10, inf) | 268,868 | 616.08 | 14.24 |
| Static | [0, 2.0) | 1,857,951 | 126.46 | 9.11 |
| Static | [2, 5.0) | 980,175 | 195.75 | 6.88 |
| Static | [5, 10.0) | 312,447 | 296.63 | 7.21 |
| Static | [10, inf) | 268,868 | 448.34 | 7.85 |
| Offline rolling | [0, 2.0) | 1,857,951 | 121.89 | 6.22 |
| Offline rolling | [2, 5.0) | 980,175 | 186.83 | 5.02 |
| Offline rolling | [5, 10.0) | 312,447 | 283.24 | 5.66 |
| Offline rolling | [10, inf) | 268,868 | 406.38 | 5.13 |


## Static-model eligibility

**PROMOTE** against baseline,
calibration and latency gates. This diagnostic does not register or promote the static model,
and does not compare it with a streaming-model incumbent.

| Gate | Observed | Threshold | Passed |
|---|---:|---:|:---:|
| mae_improvement_vs_baseline_pct | 25.780 | 15.000 | yes |
| max_distance_bucket_calibration_error_pct | 9.114 | 10.000 | yes |
| inference_p95_ms | 0.279 | 5.000 | yes |


## Static-candidate promotion decision

**PROMOTE** `98ef1dd9ef4447f7-static`.

| Gate | Observed | Threshold | Passed |
|---|---:|---:|:---:|
| mae_improvement_vs_baseline_pct | 25.780 | 15.000 | yes |
| max_distance_bucket_calibration_error_pct | 9.114 | 10.000 | yes |
| inference_p95_ms | 0.279 | 5.000 | yes |


## Limitations

The data represents completed medallion-taxi trips and local wall-clock timestamps. Performance can
shift across seasons, policy changes, vehicle types, and unusual demand. Online feature parity and
live error monitoring are separate acceptance gates before serving claims are made.
Trip distance is the observed completed-trip distance; pre-trip use requires an external route
estimate whose accuracy is not validated here. Rolling features in this comparison are computed
offline, not supplied by a live stream processor. Timing is local single-row model inference,
not end-to-end HTTP latency.
