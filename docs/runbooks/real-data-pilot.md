# Reproduce the real-data model pilot

The pilot trains on all accepted January 2024 rows and evaluates all accepted February rows.
December is used only as January's completion-time lookback context. The planned release split
remains January–March / April; this pilot is a separate evaluation because March and April failed
the unchanged 10% partition-quality gate.

Run from the repository root after installing `.[dev]`. Allow disk space for source, silver, gold,
models and up to 8 GB of temporary DuckDB spill files. Gold builds use a 2 GB DuckDB memory limit
and two workers. The limit is not a hard process-RSS ceiling. Training holds feature matrices in
memory and uses one LightGBM thread; the feature-build limits do not apply to training.

## Prepare accepted data

```bash
tripml ingest --month 2023-12
tripml ingest --month 2024-01
tripml ingest --month 2024-02
```

For the source-quality audit, ingest March and April independently. A quarantined partition returns
exit code 2; preserve the generated report and source rather than overriding the gate:

```bash
tripml ingest --month 2024-03
tripml ingest --month 2024-04
```

Do not combine those expected rejections with a fail-fast loop that hides the April result.

## Build features and train

Use a new output directory for each measured command. The helper records its exact arguments;
do not include secrets. Run the gold builds sequentially on a 16 GB laptop.

```bash
python scripts/measure-command.py --output artifacts/pilot-reproduction/gold-january -- \
  python -m tripml features build --month 2024-01 --config examples/training/real-data-pilot.yaml

python scripts/measure-command.py --output artifacts/pilot-reproduction/gold-february -- \
  python -m tripml features build --month 2024-02 --config examples/training/real-data-pilot.yaml

python scripts/measure-command.py --output artifacts/pilot-reproduction/training -- \
  python -m tripml train --config examples/training/real-data-pilot.yaml --no-track
```

The generated `resource.json` records command exit status, wall time and the largest child-process
peak RSS. `stdout.log` and `stderr.log` preserve the command output. Check the exit status and the
training report: successful execution does not mean that promotion gates passed.

Gold manifests record source checksums and resource-configuration fingerprints. Training writes a
content-addressed bundle under `artifacts/training/<run-id>/` with native model files, baseline,
manifest, evaluation JSON and model card. Evidence version 2 added all three models' bucket
diagnostics and a separate static eligibility decision; current version 3 also records the selected
promotion role. `--no-track` leaves the registry untouched.
Rebuild the serving image from this code before using a current bundle in a later deployment;
older images reject the expanded manifest fields.

An identical training run reuses its verified immutable bundle; a cached rerun is not a fresh
training-duration measurement. Keep the same gold artifacts to reproduce input bytes. Physical row
order can change when rebuilding gold even with identical source files. Record dependency versions
alongside timing evidence; library versions and shared laptop load can affect results.

## Review before any promotion work

Inspect MAE/RMSE/MAPE, distance-bucket row counts and calibration, and single-row inference P95.
Explain failed gates rather than choosing a threshold after observing results. Static eligibility
is diagnostic; the original pilot configuration still selects the streaming-feature candidate by
default. For independently gated static publication, use the separate
[static promotion runbook](static-model-promotion.md). A streaming candidate's success does not
establish that its static sibling is eligible.

TLC distance is measured over the completed trip. The pilot does not validate a pre-trip route
distance estimator, streaming feature parity, HTTP latency or generalization to April. February is
now observed pilot evaluation data; tuning in response to its results requires a fresh final holdout.
