# Clean-checkout reproduction and portfolio package — 2026-09-19

The release candidate passed in a **new checkout and virtual environment**: **331 tests passed**,
**6 optional service tests skipped**, **94.89% coverage**, plus Ruff, formatting, strict mypy,
shell syntax and all three Helm lint/render profiles. The packaged model passed native/API parity
for all three April passenger-count cohorts. A fresh public January download reproduced the
original accepted population and source checksum.

This was a clean export of the candidate on the same macOS arm64 host, not a second-machine or
full-cluster rebuild. The base was `e118b6b`; the uncommitted candidate changes were copied into
a `git clone --no-hardlinks` checkout before the final checks. No commits or tags were created.
[Exact tested source hashes](reproduction/source-sha256.json) identify 106 source/configuration
files independently of the future commit. The [machine-readable receipt](clean-checkout.json)
records commands, scope, hashes and exclusions.

## What was actually rerun

| Check | Result | Evidence |
|---|---|---|
| Dependency installation | Fresh Python 3.12.12 venv, constrained install, `pip check` passed | [Installed packages](reproduction/packages.txt), [environment](reproduction/environment.json), [constraints](../../requirements/portfolio-python312.txt) |
| Repository quality gates | 331 passed / 6 skipped / 94.89% coverage | [Complete final log](reproduction/quality-gates.log) |
| Artifact portability | 196 files verified and extracted into a new directory; independently rebuilt payload manifest matched | Archive digest below; [packager](../../scripts/package-release.py) |
| Model smoke | Known-positive, zero and unknown-count requests matched native inference; schema 1.0 null returned 422 | [Predictions and readiness](reproduction/model-smoke.json), [log](reproduction/model-smoke.log) |
| Real public-data ingestion | 2,964,624 input rows; 2,894,609 accepted; 70,015 invalid; original checksum/counts matched | [January quality report](reproduction/january-ingestion.json) |
| Existing image identities | Read-only inventory; no cluster changes | [Container/image inventory](release-images.json) |

The smoke identifies `98ef1dd9ef4447f7-static`, `static_primary` and `gold-features-v2`.
It loads the original native model from the extracted archive, without accessing the author's
registry or original model directory. Registry version is null and publication is disabled.
This is an in-process API smoke, not a fresh network load or broker test.

The first fresh `make check` exposed an ordering issue: Helm was installed after the tests,
so 20 chart tests skipped in addition to the six optional service tests. The Makefile now runs
Helm installation/validation before pytest; the final run executed those chart tests. Nine new
archive tests cover extraction, changed bytes, duplicate/extra members, links and unsafe paths.

The first package installation was blocked by sandbox network access. Retrying with network
access succeeded. Initial attempt logs remain under
`artifacts/releases/portfolio-reproduction-20260919/`; the final quality log is checked in.
The two existing suite warnings are preserved in that log.

## Prepared artifact

- Filename: `tripml-batch-v0.1.0-artifacts.tar.gz`
- SHA-256: `3b9fee14ba3b95db8560c50ae3b30484f94154857a1480a5498b72e0be30e93d`
- Contents: original native baseline/static/rolling models and model card, April request sample,
  raw evaluation/deployment/operations evidence, dashboard exports, broker readback and historical
  validation receipts. Every one of the 196 members has a manifest checksum.
- Excluded: registry databases, credentials, TLC/gold partitions and Docker images.

The archive and checksum sidecar are prepared under `dist/` and intentionally ignored by Git.
The builder checks the model hashes against the committed evaluation and checks historical
deployment/operations hashes before packaging. A second build produced an identical payload
manifest; gzip metadata can change the archive checksum, so retain the exact verified file above.

The archive has not been published. Use the [maintainer commands](../runbooks/publish-release.md)
to commit, tag and upload it. The [reproduction runbook](../runbooks/reproduce-release.md) provides
local handoff and post-publication download commands. Source archive creation belongs to the final
tag, which the maintainer explicitly owns.

## Identity, cleanup and limits

The Python constraints are tested version pins, not a cross-platform hash lock. The image
inventory distinguishes local content IDs from publicly pullable registry digests. The original
serving image is identified but not distributed; a Dockerfile rebuild must record its own identity.

No full gold rebuild, retraining, registry publication, Kubernetes deployment, timed load or fault
injection was repeated in this pass. Their original [model](batch-release-model.md),
[deployment](approved-model-deployment.md) and [operations](approved-model-operations.md) receipts
remain the evidence for those claims. Full rebuild commands are documented separately and must
produce fresh evidence rather than inherit the historical model/image identity.

The TestClient closed normally, ingestion and checks exited, and no cluster resources were
created or changed. The disposable checkout at `/private/tmp/tripml-clean-20260919` is retained for
inspection and can be removed after review. Original data, model bundles and registries are unchanged.
Remote CI and downloading the published assets remain maintainer checks after publication.
