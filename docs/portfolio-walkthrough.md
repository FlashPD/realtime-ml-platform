# Five-minute portfolio walkthrough

This reviews the measured batch-serving release. Prepare the environment and artifact archive
with the [reproduction runbook](runbooks/reproduce-release.md) before a live demo. Installation,
data rebuilding and fault injection are outside the five-minute presentation.

## 0:00 — The problem and delivered scope

“I built a reproducible taxi-duration ML platform that carries data identity through evaluation,
model promotion and online serving, and makes broker failures visible to the caller.”

Show the [implemented architecture](../README.md#implemented-architecture). Follow the path from
TLC Parquet through silver, gold, MLflow, FastAPI and acknowledged publication. Airflow schedules
batch work; Prometheus and Grafana expose serving behavior. The selected model is static; the
streaming feature producer remains future work.

## 0:45 — Data correctness and the model decision

Open the [accepted release data evidence](validation/unknown-passenger-policy.md). January–March
contains 9,316,058 accepted training trips; April contains 3,419,441 holdout trips. Null passenger
counts are preserved under contract 1.1. Feature windows use trips completed strictly before pickup.

Show the [model comparison](validation/batch-release-model.md) and
[model card](validation/batch-release-model-card.md). Static MAE is 187.18 seconds, 25.78% below
the hierarchical median baseline. The unchanged accuracy, calibration and inference gates passed.
Rolling-feature accuracy is an offline comparison, not proof of live feature parity.

Explain that TLC distance is completed-trip distance, and the baseline lacks distance/passenger
inputs. This experiment establishes the recorded comparison, not live pre-trip ETA accuracy.

## 1:45 — Run the approved model

From the prepared checkout, use a new receipt path:

```bash
python scripts/portfolio-smoke.py --release-root release --output build/demo-smoke.json
```

Open `build/demo-smoke.json`. It records three real April requests—known positive, zero and
unknown passenger counts—and verifies each API prediction against native LightGBM inference.
Readiness identifies `98ef1dd9ef4447f7-static`, `static_primary`, and `gold-features-v2`.
The legacy schema rejects null counts with HTTP 422.

This demo loads a relocated local bundle and disables broker publication. Registry version is
null by design. Show the [separate Kubernetes receipt](validation/approved-model-deployment.md)
for the measured deployment's registry version 1 and broker readback.

## 2:45 — Load, monitoring and failure behavior

Show the [exported telemetry figure](validation/approved-model-operations.png), then the
[operational evidence](validation/approved-model-operations.md). The declared local load delivered
10,000 successes at 100 requests/s, 7.99 ms client P95, no errors/drops, and exact readback of
10,020 measured/warm-up events. Client latency includes dispatch lag and broker acknowledgment.

For a prepared cluster, open Grafana using the [monitoring runbook](runbooks/serving-monitoring.md).
Use the recorded measurement time range or send fresh traffic before describing a live panel.
Historical dashboard definitions, all 13 query exports and raw samples are in the archive;
the checked-in figure is a telemetry export, not a Grafana screenshot.

The isolated fault suite produced 120 explicit 503s and recovered acknowledgment in 6.25 seconds
without restarting the API. Its lower-rate profile is separate from the 100/s load run.
Failed HTTP delivery remains uncertain; acknowledgment does not establish exactly-once delivery.

## 4:15 — Reproducibility and trade-offs

Show the [clean-checkout receipt](validation/clean-checkout.md), archive checksum and manifest.
Explain the boundary: a fresh environment verified the packaged model and repository checks;
the full training and cluster measurements retain their original dates and identities.

Close with three trade-offs: acknowledgment creates broker dependence, single-node kind makes
failure behavior inspectable but does not establish availability, and static serving avoids a
live-feature dependency while the streaming producer and parity checks are still being built.

## Portfolio description

Built a reproducible NYC taxi-duration ML platform with versioned data contracts, point-in-time
features, guarded MLflow promotion, Kubernetes serving and Prometheus/Grafana monitoring. Evaluated
9.32M training and 3.42M holdout trips; the static model achieved 187.18 s MAE, 25.78% below its
declared baseline. Measured 100 requests/s at 7.99 ms client P95 with acknowledged broker delivery
on local kind, and verified bounded broker failures and recovery without restarting the API.
