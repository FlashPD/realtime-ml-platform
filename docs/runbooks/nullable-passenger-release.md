# Prepare the release with unknown passenger counts

Use [batch-release.yaml](../../examples/training/batch-release.yaml) for January–March 2024 training
and April evaluation. It opts into silver contract 1.1 and gold feature version 2 in a separate
data root. The original strict data, quarantines and pilot registry remain available. Read
[ADR-0018](../adr/0018-unknown-passenger-counts.md) before comparing either population's results.
The [captured validation](../validation/unknown-passenger-policy.md) records the full data
preparation, workload sample and software checks; full release model evaluation is still pending.

Install `.[dev]` and run commands from the repository root. Existing downloaded bronze can be
reused without copying or altering it:

```bash
for month in 2023-12 2024-01 2024-02 2024-03 2024-04; do
  python scripts/measure-command.py --output "artifacts/nullable-reproduction/ingest-$month" -- \
    python -m tripml ingest --month "$month" --config examples/training/batch-release.yaml \
    --source "data/bronze/yellow/month=$month/yellow_tripdata_$month.parquet" || break
done
```

If a bronze file is unavailable, omit `--source` for that month to download it into the isolated
data root. Use fresh measurement directories. Verify every quality report's accepted status,
source checksum, contract version, missing counts, and violations. An explicit opt-in does not
guarantee acceptance. Invalid known counts and failures in other fields remain quarantined.

Build the complete feature population, including consistent previous-month context:

```bash
for month in 2024-01 2024-02 2024-03 2024-04; do
  python scripts/measure-command.py --output "artifacts/nullable-reproduction/gold-$month" -- \
    python -m tripml features build --month "$month" \
    --config examples/training/batch-release.yaml || break
done
```

Each gold manifest must identify `gold-features-v2` and contract-1.1 inputs. Null and zero counts
remain distinct. Feature builds use the same two-worker, 2 GB DuckDB buffer/8 GB spill profile;
those settings are not a hard operating-system memory limit or a training-memory limit.

## Evaluate before publication

The full release training run is larger than the January-only pilot. Keep logs and measurements,
and first evaluate offline:

```bash
python scripts/measure-command.py --output artifacts/nullable-reproduction/training -- \
  python -m tripml train --config examples/training/batch-release.yaml --no-track
```

Inspect aggregate metrics, distance buckets and both known/unknown passenger-count cohorts in the
version-4 report/model card. Keep the predeclared split, parameters and gates. A passing execution
status does not mean a candidate passed its model gates. The release configuration selects static
promotion; the rolling-feature candidate remains an offline comparison.

An eligible bundle can be published through `tripml train --config
examples/training/batch-release.yaml`. With unchanged inputs, settings and no incumbent, it reuses
the verified offline bundle. Publication targets the isolated `tripml-trip-duration-static-v2`
registry, never the original pilot. Existing incumbent comparisons still require the same holdout
month and checksum. Run the API with the same configuration after successful publication:

```bash
python -m tripml serve --config examples/training/batch-release.yaml
```

For an unknown count, explicitly request the new contract:

```json
{
  "schema_version": "1.1",
  "trip_id": "unknown-passenger-demo",
  "pickup_zone_id": 161,
  "dropoff_zone_id": 236,
  "pickup_time": "2024-04-15T12:00:00-04:00",
  "trip_distance_miles": 3.2,
  "passenger_count": null
}
```

The API reports `feature_model_version=gold-features-v2`. The prediction preserves the null input
and uses schema 1.1. A version-1 model returns HTTP 422 for this request. Rebuild serving images and
update prediction consumers before deploying these artifacts. Redis version-1 snapshots cannot
supply rolling features to a version-2 model.

## Prepare held-out traffic

```bash
python -m tripml benchmark-workload --month 2024-04 \
  --config examples/training/batch-release.yaml --rows 10000 --seed 42 \
  --output artifacts/workloads/april-passenger-v1_1
```

The generated requests preserve null counts with schema 1.1. Passenger distributions include an
explicit `unknown` bucket. Preserve the manifest for benchmark provenance, use
`--expected-features static`, and declare the publication expectation. Workload preparation is
not model evaluation or a load measurement. See the [release checklist](../batch-serving-release.md).
