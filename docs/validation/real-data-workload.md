# Real-data serving workload validation

On September 17, 2026, the workload builder scanned the local January 2024 TLC yellow-taxi silver
partition and generated 10,000 benchmark requests. This validates real-data input preparation;
it does **not** measure HTTP latency, real-data model accuracy, or online feature correctness.

| Measurement | Observed |
|---|---:|
| Accepted silver trips scanned | 2,757,364 |
| Eligible trips | 2,757,364 |
| Ambiguous/nonexistent New York pickup times excluded | 0 |
| Sample size | 10,000 |
| Sampling seed | 42 |
| Python | 3.12.12 |

The [captured manifest](real-data-workload.json) contains the population and sample histograms,
input checksums, output checksum, configuration fingerprint, and sampling parameters. Its silver
path is made repository-relative; the original local manifest's digest is also recorded.

| Distribution | Total variation distance |
|---|---:|
| Pickup hour of week | 0.046140 |
| Pickup zone | 0.036336 |
| Dropoff zone | 0.040495 |
| Distance bucket | 0.003049 |
| Passenger count | 0.003742 |

Total variation distance is 0 for identical distributions and 1 for disjoint distributions. These
are comparisons of marginals, not evidence of joint-distribution equivalence or rare-route coverage.
No distribution threshold was used to select or rerun this sample.

Request fixture SHA-256:

```text
483eb5189daed6149ac4dcd3918ecca366e3cf8221a4994c06b13b8861fe08e9
```

The raw local bundle is under `artifacts/workloads/tlc-2024-01-seed42/` (ignored by Git). Reproduce
from the same accepted silver bytes using a new output directory:

```bash
make benchmark-workload PYTHON=.venv/bin/python MONTH=2024-01 \
  OUTPUT=artifacts/workloads/tlc-jan-reproduction WORKLOAD_ARGS='--rows 10000 --seed 42'
```

Sampling is deterministic for the same input bytes, seed, and Python version. Unit tests verify
identical request bytes across Arrow batch sizes and different requests when the seed changes.
Ingestion timestamps and quality-report checksums can differ on a fresh ingestion. Compare the
silver checksum before expecting byte-identical fixtures.

All eligible records were validated against ETARequest, retaining historical New York pickup
offsets. No labels were included. Sampling uses bounded Arrow batches and a reservoir instead of
loading all monthly trips into Python memory. The benchmark can verify and preserve this lineage
with `--workload-manifest`; it still schedules constant arrivals and generates new trip IDs.

The next measurement requires a real-data model and an isolated serving deployment. Matching
event-time Redis features are additionally required for online-serving evidence. The synthetic
kind/HPA results remain documented separately and are not relabeled as real-data measurements.
