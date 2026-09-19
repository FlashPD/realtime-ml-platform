# Register and serve the static pilot

The [static pilot configuration](../../examples/training/static-pilot.yaml) uses January training
and February evaluation, unchanged model parameters and quality gates, and a separate local MLflow
database/model name. It never targets the existing streaming registry. This is a pilot deployment;
March/April source completeness remains a separate release blocker.

The [captured validation](../validation/static-model-promotion.md) records a full-data run,
initial registration, idempotent publication retry, and registry-to-API smoke checks.

## Train and publish

Install `.[dev]` and prepare January/February gold using the
[real-data pilot runbook](real-data-pilot.md). Run from the repository root:

```bash
python scripts/measure-command.py --output artifacts/static-pilot-reproduction/training -- \
  python -m tripml train --config examples/training/static-pilot.yaml
```

Choose a fresh measurement directory for each attempt. Inspect `stdout.log`: the selected
`training.promotion_role` must be `static`, every promotion gate must pass, and
`tracking.registered_version` must be non-null. A completed training command can still report
a rejected candidate. Models, their metrics and evidence are logged even when gates reject them.

The first accepted run creates version 1 of `tripml-trip-duration-static-pilot` and assigns its
`production` alias in the isolated SQLite registry. The alias name is local to that pilot model.
`--no-track` creates an offline bundle without registration. Repeating `train` after publication
evaluates against the incumbent and normally rejects an identical model at the unchanged 1%
improvement gate; it is not the same operation as retrying publication of an existing report.

## Serve the registry artifact

```bash
python -m tripml serve --config examples/training/static-pilot.yaml
```

In another terminal:

```bash
curl --fail http://127.0.0.1:8000/readyz
curl --fail http://127.0.0.1:8000/v1/eta \
  -H 'Content-Type: application/json' \
  -d '{"trip_id":"static-pilot-demo","pickup_zone_id":161,"dropoff_zone_id":263,
       "pickup_time":"2024-02-06T22:17:18-05:00","trip_distance_miles":2.4,
       "passenger_count":1}'
```

Readiness must report `mode=static_primary`, a non-null registry version, and a null
`streaming_model_version`. The prediction identifies `<bundle-run-id>-static` and contains exactly
the five static inputs. Redis settings do not enable streaming for this model. The response's
`feature_fallback=true` and the existing fallback counter retain their version-1 meaning of static
feature usage; they do not indicate an outage in this mode. Prediction publication is disabled in
this local configuration. Broker acknowledgment remains available through the existing publication
settings and retains its HTTP-success semantics.

The request above is the February example preserved in the original pilot evidence. It is a smoke
check, not a latency or accuracy benchmark. For static load tests use `--expected-features static`
and declare the publication expectation explicitly.

## Move toward the batch release

Use the same explicit candidate role and a separate static model name when training against the
cluster-accessible MLflow service. A host SQLite registry is not reachable from Kubernetes. Point
the Helm `serving.registeredModelName` and `serving.productionAlias` values at that service's approved
static model, and build a serving image containing the version-3 report reader. The serving process
selects the role from registry evidence; no new Helm role flag is needed.

Keep one promotion writer. A stale incumbent, different role, changed holdout checksum, failed
gate or corrupt artifact must be investigated before publishing again. Do not edit an immutable
manifest, lower thresholds or reuse a streaming alias to force publication. This workflow does
not support comparing stored metrics across holdouts; reevaluating incumbents on a new holdout
remains future work. See [ADR-0017](../adr/0017-explicit-static-model-promotion.md).
