# Maintainer commit and release commands

The assistant has not committed, tagged, pushed or published anything. Run these commands from
the repository root after reviewing the diff. The candidate is version `0.1.0`; choose another
unused tag if that version is already taken, and update the release notes/asset names consistently.

## Review and commit

```bash
cd /Users/pushyanth/Desktop/Code/realtime-ml-platform
git diff --check
git diff --stat
git status --short
git add README.md Makefile docs requirements scripts/package-release.py \
  scripts/portfolio-smoke.py tests/test_release_package.py
git diff --cached --stat
git commit -m "Package batch-serving portfolio release and verify clean reproduction"
```

The archive is intentionally ignored by Git. Do not force-add model files, data, local state,
credentials or release archives. The `docs/` additions include the machine-readable verification
receipt and public reproduction logs; review those with the source changes.

## Tag and prepare assets

The prepared artifact archive contains the original model and historical evidence. Confirm its
SHA-256 agrees with `docs/validation/clean-checkout.json`, then verify every member:

```bash
(cd dist && shasum -a 256 -c tripml-batch-v0.1.0-artifacts.tar.gz.sha256)
python3.12 scripts/package-release.py verify dist/tripml-batch-v0.1.0-artifacts.tar.gz
git status --porcelain
# The output above must be empty before tagging.
git tag -a v0.1.0 -m "Batch-serving portfolio release"
git archive --format=tar.gz --prefix=realtime-ml-platform-v0.1.0/ \
  --output=dist/tripml-batch-v0.1.0-source.tar.gz v0.1.0
(cd dist && shasum -a 256 tripml-batch-v0.1.0-source.tar.gz \
  > tripml-batch-v0.1.0-source.tar.gz.sha256)
```

If the model/evidence archive needs rebuilding, all original inputs named in
`scripts/package-release.py` must be present. Build into a new location:

```bash
python3.12 scripts/package-release.py build \
  --output dist/rebuilt/tripml-batch-v0.1.0-artifacts.tar.gz
```

Gzip timestamps can change the archive digest even with identical members. A rebuilt archive
needs its own digest in the receipt and a new verification before committing/tagging.
Use the already verified archive for the prepared release.

## Push and publish

These are explicit maintainer actions. The commands push the current branch without assuming
its name, then upload the verified archives and checksum sidecars:

```bash
python3.12 - <<'PYNOTES'
import re
from pathlib import Path
from urllib.parse import urljoin
text = Path("docs/releases/v0.1.0.md").read_text().replace(
    "Prepared release candidate; the maintainer owns commits, tagging and GitHub publication.",
    "Released batch-serving portfolio milestone.",
)
base = "https://github.com/FlashPD/realtime-ml-platform/blob/v0.1.0/docs/releases/"
text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)",
              lambda m: f"[{m[1]}]({urljoin(base, m[2])})", text)
Path("dist/v0.1.0-github-notes.md").write_text(text)
PYNOTES
git push -u origin HEAD
git push origin v0.1.0
gh release create v0.1.0 --repo FlashPD/realtime-ml-platform --verify-tag \
  --title "v0.1.0 — Batch training and online serving" \
  --notes-file dist/v0.1.0-github-notes.md \
  dist/tripml-batch-v0.1.0-artifacts.tar.gz \
  dist/tripml-batch-v0.1.0-artifacts.tar.gz.sha256 \
  dist/tripml-batch-v0.1.0-source.tar.gz \
  dist/tripml-batch-v0.1.0-source.tar.gz.sha256
gh release view v0.1.0 --repo FlashPD/realtime-ml-platform --web
```

The generated notes mark the release as published and resolve documentation links against the
tag. The committed draft preserves its verification-time publication status.

Finally download the assets into a new directory using the [reproduction runbook](reproduce-release.md)
and compare their digests. The new remote Quality workflow result is independent of the local
verification receipt; inspect it before describing CI as passing.
