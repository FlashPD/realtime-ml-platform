# Unknown-passenger policy and release-data validation — 2026-09-18

The explicit contract-1.1 policy admits March and April into an isolated release data root while
preserving unknown passenger counts as null. Their remaining violation rates are **2.68%** and
**2.70%**, against the unchanged **10%** partition threshold. Original strict data, quarantines,
models and evidence remain intact.

All January–April gold partitions were built with `gold-features-v2`, providing **9,316,058 training
rows** for January–March and **3,419,441 April holdout rows**. No real-data version-2 model has been
trained, registered or deployed yet; this validates data preparation and the contract migration,
not release accuracy. The original pilot scores apply to its original population.

See the [machine-readable snapshot](unknown-passenger-policy.json),
[policy decision](../adr/0018-unknown-passenger-counts.md), and
[reproduction runbook](../runbooks/nullable-passenger-release.md).

## Partition comparison

| Partition | Strict contract violation rate | Contract 1.1 violation rate | Contract 1.1 accepted rows | Accepted unknown-count rows |
|---|---:|---:|---:|---:|
| December context | 7.61% | 2.40% | 3,295,606 | 175,870 |
| January training | 6.99% | 2.36% | 2,894,609 | 137,245 |
| February training | 8.40% | 2.42% | 2,934,817 | 179,810 |
| March training | 14.12% | 2.68% | 3,486,632 | 409,960 |
| April holdout | 13.88% | 2.70% | 3,419,441 | 392,879 |

The source file checksums match the original audit. In every month, the increase in valid rows is
exactly the count of otherwise valid trips with missing passenger counts. Known passenger ranges,
all other row rules and the partition threshold are unchanged. Missing passenger counts on rows
that violate other rules remain excluded: for example, April contains 408,576 source null counts,
of which 392,879 are valid under the new contract.

Observed zero counts remain distinct: April silver and gold both retain **38,520 zero-count rows**
and **392,879 unknown-count rows**. All four gold outputs match their silver partitions' total,
unknown-count and zero-count row counts. Every target/lookback input declares contract 1.1; dbt's
feature, timestamp and passenger-contract tests passed before each output was published.

## Prepared April workload

A seed-42 reservoir sample contains **10,000 requests**, including **1,178 unknown-count requests**,
from all **3,419,441** accepted April trips. Requests use schema 1.1 and preserve null counts.
The manifest includes an explicit `unknown` passenger bucket and input/fixture checksums. Passenger
distribution total variation distance is **0.01096**; this is descriptive sample evidence, not a
performance gate. No April pickups required daylight-saving exclusion.

The fixture is under `artifacts/workloads/april-passenger-v1_1/`. It is ready for a static-serving
benchmark after model evaluation and deployment. It does not establish HTTP latency, broker
delivery or Kubernetes capacity.

## Resource observations

| Command | Wall time | Peak child-process RSS |
|---|---:|---:|
| January gold | 79.57 s | 1.721 GiB |
| February gold | 91.01 s | 1.706 GiB |
| March gold | 109.34 s | 1.901 GiB |
| April gold | 98.85 s | 1.869 GiB |
| April workload | 69.00 s | 0.202 GiB |

Each ingestion took 7.3–8.5 seconds and stayed below 0.282 GiB peak child-process RSS. These are
single observations on a shared 16 GB ARM64 macOS laptop; some tests and workload sampling ran
concurrently with feature builds. Peak RSS is the largest child-process high-water mark, not
aggregate process-tree memory. The feature-build profile remains two workers, a 2 GB DuckDB buffer
limit and an 8 GB spill limit. No training-memory or speedup claim follows from these numbers.

## Software verification and provenance

The full quality gate passed **292 tests**, with six optional service tests skipped and **94.89%
coverage**, plus Ruff, mypy and Helm validation. New regression tests cover strict/default behavior,
null versus zero preservation, invalid known counts, continued quarantine of other failures, mixed
feature-contract rejection, point-in-time windows, schema exports, workload sampling, native-model
missing-value behavior, known/unknown cohort metrics, registry-to-HTTP inference, legacy-model
HTTP 422 rejection, broker header serialization and Redis population isolation.

The native-model tests use synthetic fixtures; their scores are not portfolio accuracy evidence.
They prove that the nullable API prediction agrees with native NaN inference and that an observed
zero can produce a different result. The existing strict model remains usable for known counts.
Rebuild serving images and update consumers before deploying schema-1.1 predictions.

The snapshot records both contracts' quality reports, every new gold manifest, workload provenance,
resource measurements, dependency versions and source-code digests. **Thirty files** were checked
or fingerprinted, including the original/new quality reports, source data, new silver/gold files
and request fixture. Repository paths are normalized. Original quality reports and input files
were read without mutation. Raw logs, resources and the capture helper remain under
`artifacts/releases/passenger-policy-20260918/`, ignored by Git.

Next, run the predeclared January–March / April model comparison and inspect both missingness
cohorts before publication. Missingness can reflect different reporting behavior; acceptance is
not evidence that the new population is unbiased or equally predictable. TLC's completed-trip
distance limitation and the need for live feature-parity validation remain unchanged.
