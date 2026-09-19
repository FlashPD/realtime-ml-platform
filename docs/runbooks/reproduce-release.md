# Reproduce the batch-serving portfolio release

Run from the repository root. Use the original checksummed artifacts to inspect the exact measured
model, or rebuild from TLC data to produce a new model identity. The
[validation receipt](../validation/clean-checkout.md) distinguishes executed checks from documented
rebuild steps. No command here depends on the author's absolute filesystem paths.

## Fresh source and dependencies

```bash
git clone https://github.com/FlashPD/realtime-ml-platform.git
cd realtime-ml-platform
# After the maintainer publishes it:
git checkout v0.1.0
python3.12 -m venv .venv
source .venv/bin/activate
# macOS prerequisite: brew install libomp
python -m pip install -c requirements/portfolio-python312.txt -e '.[dev]'
python -m pip check
make check PYTHON=.venv/bin/python
```

Before publication, use the maintainer's candidate source tree instead of the unavailable tag.
The constraints capture the tested Python 3.12 package versions. They are a version snapshot,
not a cross-platform wheel/hash lock; this pass was tested on macOS arm64. Python, OpenMP,
OS libraries and container images have separate identities. `make check` downloads a pinned,
checksum-verified Helm binary and renders three chart profiles. Optional service/cluster tests
require their own environments; skipped tests are not end-to-end evidence.

## Original model and evidence

The maintainer has prepared `tripml-batch-v0.1.0-artifacts.tar.gz` and its `.sha256` sidecar.
They contain native models, original evaluation, April requests, deployment receipts, raw
client/broker records and monitoring exports. They exclude registry databases, credentials,
raw/gold data and Docker images. Until publication, obtain these two files directly from the
maintainer's `dist/` directory. After publication:

```bash
mkdir -p dist
gh release download v0.1.0 --repo FlashPD/realtime-ml-platform \
  --pattern 'tripml-batch-v0.1.0-artifacts.tar.gz*' --dir dist
```

Verify the archive against the independently checked-in checksum in
[the validation receipt](../validation/clean-checkout.json), as well as its sidecar:

```bash
(cd dist && shasum -a 256 -c tripml-batch-v0.1.0-artifacts.tar.gz.sha256)
python scripts/package-release.py verify dist/tripml-batch-v0.1.0-artifacts.tar.gz \
  --extract release
python scripts/portfolio-smoke.py --release-root release --output build/portfolio-smoke.json
```

Extraction requires a new directory. Verification rejects missing/extra files, changed bytes,
duplicate paths, links and traversal paths before extraction. The internal manifest checks
integrity; the separately recorded archive digest binds it to this release. The smoke checks
original model gates, all three passenger-count cohorts against native inference, static-primary
identity, disabled publication, and legacy-null rejection. It creates no registry or cluster
resources and does not retrain. Original manifests retain historical absolute paths as provenance;
the serving loader resolves fixed model filenames inside the supplied bundle.

For an interactive API:

```bash
python -m tripml serve --config examples/training/batch-release.yaml \
  --bundle release/artifacts/training/passenger-v1_1/98ef1dd9ef4447f7 --port 8000
```

Open `http://localhost:8000/docs` or `/readyz`. Publication is disabled and registry version is
null in local-bundle mode. Stop with Ctrl-C. Remove only the disposable `release/`, `build/`
or fresh-checkout directory when finished; existing clusters and registries are not involved.

## Full public-data rebuild

This path needs network access and substantially more disk, RAM and time than the demo. The
original feature profile allows 2 GB DuckDB buffers plus up to 8 GB spill; training loads millions
of rows and is not bounded by that buffer setting. See resource measurements in
[model evaluation evidence](../validation/batch-release-model.md). Do not train during timed loads.

Use a fresh checkout/data root and stop at the first command failure:

```bash
set -e
for month in 2023-12 2024-01 2024-02 2024-03 2024-04; do
  python -m tripml ingest --month "$month" --config examples/training/batch-release.yaml
done
for month in 2024-01 2024-02 2024-03 2024-04; do
  python -m tripml features build --month "$month" --config examples/training/batch-release.yaml
done
python scripts/measure-command.py --output artifacts/rebuild/training -- \
  python -m tripml train --config examples/training/batch-release.yaml --no-track
python -m tripml benchmark-workload --month 2024-04 --rows 10000 --seed 42 \
  --config examples/training/batch-release.yaml --output artifacts/workloads/april-passenger-v1_1
python scripts/validate-release-model.py --config examples/training/batch-release.yaml \
  --training-report artifacts/rebuild/training/stdout.log \
  --workload artifacts/workloads/april-passenger-v1_1 --output artifacts/rebuild/registry-validation
```

The final command verifies inputs and gates, then writes to the new local registry. Inspect the
report and preserve failures; never assume a successful training process passed promotion.
Compare source hashes, populations, feature versions and metrics with the original receipt.
Rebuilt Parquet bytes, run IDs and timing can differ by environment; do not assign the old identity
or copy the old result onto a new run. April is an observed holdout, not a fresh tuning target.

For Kubernetes, follow [deployment](deploy-approved-model.md) and [operations](release-operations.md).
Those measured-run helpers deliberately bind the original bundle/image. A rebuilt candidate needs
its own reviewed identity constants, loaded image tag, declared plan and fresh evidence directory.
The recorded image ID is local content identity, not a publicly pullable registry digest; see
[image inventory](../validation/release-images.json). Rebuilding the Dockerfile resolves dependencies
again and must record a new identity. The exact historical image is not shipped.
