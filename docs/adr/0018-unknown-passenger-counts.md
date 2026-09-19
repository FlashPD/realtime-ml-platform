# ADR-0018: Preserve unknown passenger counts with a versioned contract

Status: Accepted

## Context

The original data contract required every selected TLC field, including passenger count. It
quarantined March and April 2024 because their invalid-row shares exceeded the 10% partition gate.
The prior source audit identified null passenger counts as the dominant difference, before any
April model evaluation. Those records still contain duration labels, locations, timestamps and
distance usable for the trip-duration task.

TLC describes passenger counts as driver-reported and cautions about source accuracy on its
[trip-record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Treating an absent
predictor as an explicit unknown is a project modeling decision; the publisher does not prescribe
this admission policy or guarantee the missingness mechanism.

## Decision

Keep `ingestion.passenger_count_policy: required` as the default, producing silver contract 1.0.
Add the explicit `allow_unknown` policy, producing contract 1.1. Only a null passenger count gains
admission under the new policy. Known counts must still be integer values from zero through nine;
negative, fractional, out-of-range, NaN and infinite counts remain invalid. All other row rules
and the existing 10% partition violation threshold are unchanged. A missing column remains a
schema failure; a null in a present passenger-count column is the supported unknown value.

Preserve null in silver and gold. Report source missing-count totals and the number of otherwise
valid rows with missing counts separately from violations. Unknown-rate changes are visible even
when a partition passes. Missingness is not itself a gate in the opt-in policy: a field that is
optional has no invented completeness threshold. There is no claim that missingness is random.

Version the accepted population and its feature semantics together. Contract 1.0 inputs produce
`gold-features-v1`; contract 1.1 inputs produce `gold-features-v2`. Inspect every input's declared
contract in bounded batches and reject mixed target/lookback contracts. Rebuild the full release
split plus December context into the separate `data/passenger-v1_1` root. Preserve the original
strict data, quarantines and pilot artifacts for comparison.

LightGBM receives native NaN from Arrow nulls in its numeric feature matrix. It uses learned
missing-value routing with its default `use_missing=true` and `zero_as_missing=false` behavior
([official documentation](https://lightgbm.readthedocs.io/en/stable/Advanced-Topics.html#missing-value-handle)).
Keep observed zero counts distinct; no mean, zero or mode imputation is introduced. Feature order
stays unchanged. The median baseline does not use passenger count. Training validates known counts,
rejects null counts under feature version 1, and requires the same feature version across all months.

Evidence version 4 reports training/holdout missing counts and holdout MAE for known/unknown cohorts
for each of the baseline, static and rolling-feature candidates. These are diagnostics, not tuned
promotion thresholds. Empty cohorts report null error metrics. The aggregate model gates remain
unchanged. The evidence version participates in bundle identity; old manifests remain readable.

Unknown-count requests must explicitly supply `schema_version: "1.1"` and
`passenger_count: null`. Omission of the count is still invalid. Legacy known-count requests remain
accepted. A feature-version-1 model rejects unknown-count requests with HTTP 422 before inference
or publication. Version-2 models retain null in prediction provenance and emit prediction schema
1.1. Publication preserves that schema version in broker headers. Export separate 1.1 contract
subjects; legacy exported subjects still exclude null counts/features. Consumers of new-model
predictions and serving images must be updated for these schemas.

Online features for the changed accepted population use `tripml:features:v2:...` keys and
`feature_model_version=gold-features-v2`. The serving model selects the namespace, and snapshot
validation rejects the other population's feature version. The 900/3600-second window definitions
and event-time checks are unchanged. This versioning does not establish a live stream producer or
offline/online parity; missing version-2 snapshots still cause static fallback.

## Consequences

This resolves the data admission policy explicitly and makes the planned release split usable
without relabeling the original pilot. The newly admitted population can have different errors and
feature distributions, so the original pilot scores are not estimates for the new population.
Run the planned January–March / April evaluation and inspect both missingness cohorts before
claiming release accuracy. The source-completeness audit is not a model-quality result.

Use the [batch-release configuration](../../examples/training/batch-release.yaml) and
[runbook](../runbooks/nullable-passenger-release.md). The isolated model name and artifact roots
keep the strict pilot intact. TLC's observed trip-distance limitation still applies.
