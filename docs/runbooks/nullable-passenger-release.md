# Prepare the release with unknown passenger counts

Use [batch-release.yaml](../../examples/training/batch-release.yaml) for January–March 2024 training
and April evaluation. It opts into silver contract 1.1 and gold feature version 2 in a separate
data root. The original strict data, quarantines and pilot registry remain available. Read
[ADR-0018](../adr/0018-unknown-passenger-counts.md) before comparing either population's results.
The [captured validation](../validation/unknown-passenger-policy.md) records the full data
preparation, workload sample and software checks. The subsequent
[full model evaluation](../validation/batch-release-model.md) passed its fixed gates and records
registry/API validation separately from the earlier preparation evidence.

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

After preparing the April workload below, the release validator can publish the existing offline
report without reloading the full training matrices. It requires the original gold files and
bundle, and the exact training configuration (including environment overrides):

```bash
python scripts/validate-release-model.py \
  --config examples/training/batch-release.yaml \
  --training-report artifacts/nullable-reproduction/training/stdout.log \
  --workload artifacts/workloads/april-passenger-v1_1 \
  --output artifacts/nullable-reproduction/registry-validation
```

This command writes to the configured registry. It verifies configuration identity, passing gates,
native bundle integrity, all gold checksums, and workload lineage before publishing. It captures
the publication receipt before checking the API, then verifies idempotent publication and native
prediction agreement for known, zero and unknown counts. An unavailable configured Redis endpoint
must be bypassed in `static_primary` mode, and schema 1.0 must reject a null passenger count.

Keep the output's `validation.json`, `publication.json`, and `metrics.txt` with the training logs
and resource report. A failed attempt records `failure.json`; publication may already have occurred
if a later API check failed. Use a new output directory for every attempt and retain failed runs.
The check uses FastAPI's in-process client with broker publication disabled. It does not establish
network latency, container deployment, broker delivery, monitoring or Kubernetes capacity.

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

## Optional local HTTP preflight

After registry validation passes, run the API on a separate loopback port:

```bash
python -m tripml serve --config examples/training/batch-release.yaml --port 8018
```

In another terminal, verify `/readyz` identifies the selected registry version and
`static_primary` mode. Declare the load before measuring. For example, this profile sends the
10,000 April requests at 100 arrivals/second with a concurrency bound of 32:

```bash
python scripts/measure-command.py --output artifacts/nullable-reproduction/http-measurement -- \
  python -m tripml benchmark \
    --base-url http://127.0.0.1:8018 \
    --requests-file artifacts/workloads/april-passenger-v1_1/requests.jsonl \
    --workload-manifest artifacts/workloads/april-passenger-v1_1/manifest.json \
    --output artifacts/nullable-reproduction/http-load \
    --requests 10000 --rate 100 --concurrency 32 --warmup 20 \
    --expected-features static --expected-publication disabled \
    --label batch-release-local-preflight
```

The existing defaults require client P95 at most 50 ms, error rate at most 1%, and no dropped
arrivals. Keep failed attempts and their request-level samples. Record readiness, the model bundle,
registry identity, hardware and API metrics alongside the benchmark report; preserve the workload
manifest and its checksum. Stop this local API when finished.

This preflight uses host loopback and disables broker publication. Release acceptance still
requires the exact approved artifact on kind, acknowledged publication, monitoring and failure
recovery. A local passing result cannot substitute for that measurement.

Render an exportable accuracy comparison directly from the training evidence:

```bash
python scripts/plot-model-comparison.py \
  --report artifacts/nullable-reproduction/training/stdout.log \
  --output artifacts/nullable-reproduction/model-comparison.svg
```
