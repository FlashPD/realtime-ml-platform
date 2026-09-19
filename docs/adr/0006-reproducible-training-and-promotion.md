# ADR-0006: Reproducible model comparison before registry integration

- Status: accepted
- Date: 2026-09-16

## Context

The model lifecycle must show whether rolling features improve trip-duration accuracy rather than
adding a streaming system decoratively. A candidate also must not be promoted solely because one
aggregate metric improved. Tracking infrastructure can be unavailable, but that must not change
training semantics or prevent a local reviewer from reproducing the comparison.

## Decision

- Evaluate three paths on the same held-out month: a hierarchical median baseline, LightGBM with
  static pickup-time inputs, and LightGBM with the static and rolling zone features.
- Use native LightGBM training with fixed seeds, deterministic mode, one training thread, and
  explicit categorical columns. Store boosters in LightGBM's native text format rather than a
  Python pickle.
- Define the run identifier from gold input checksums, semantic training configuration, promotion
  thresholds, and feature lists. An identical invocation reuses the immutable completed run.
- Measure MAE, RMSE, MAPE, single-row inference P95, and maximum calibration error across distance
  buckets on the holdout partition.
- Require the streaming-feature candidate to pass every configured gate: MAE improvement against
  the baseline, calibration error, inference latency, and improvement against production when a
  production model exists.
- Atomically publish the models, serialized baseline, evaluation, promotion decision, checksums,
  and generated model card as one artifact directory.
- Keep training and gating independent of MLflow. The next integration will log this completed
  bundle and update a registry alias only after the same promotion decision passes.

## Consequences

The core model comparison is testable without a tracking server, and a failed or interrupted run
cannot look complete. Static-versus-streaming metrics directly support the project's planned
headline result. Native model files reduce arbitrary-code-loading risk and can be loaded by the
future serving process without importing training code.

Single-threaded deterministic training favors reproducibility over maximum training throughput in
the local portfolio profile. Production-scale training may increase parallelism after demonstrating
that metrics remain within an agreed reproducibility tolerance. On macOS, the official LightGBM
wheel requires Homebrew's `libomp` runtime.
