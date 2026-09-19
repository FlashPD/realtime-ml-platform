"""Build or verify the portable batch-release artifacts; no registry or Git writes."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import tarfile
from pathlib import Path, PurePosixPath

ROOTS = (
    "artifacts/training/passenger-v1_1/98ef1dd9ef4447f7",
    "artifacts/workloads/april-passenger-v1_1",
    "artifacts/deployments/batch-release-98ef1dd9ef4447f7",
    "artifacts/releases/batch-release-20260918",
    "artifacts/releases/batch-operations-20260919",
)
MANIFEST = "release-manifest.json"


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def safe_name(name: str) -> bool:
    path = PurePosixPath(name)
    return (
        bool(name)
        and not path.is_absolute()
        and ".." not in path.parts
        and str(path) == name
        and "\\" not in name
    )


def build(root: Path, output: Path) -> None:
    files: dict[str, str] = {}
    for directory in ROOTS:
        source = root / directory
        if not source.is_dir():
            raise ValueError(f"missing release input: {source}")
        for path in sorted(source.rglob("*")):
            if path.is_symlink():
                raise ValueError(f"symlinks are not release inputs: {path}")
            if path.is_file() and "__pycache__" not in path.parts:
                files[path.relative_to(root).as_posix()] = digest(path)
    # These committed receipts bind historical evidence to the original measurements.
    for name in ("approved-model-deployment", "approved-model-operations"):
        receipt_path = root / f"docs/validation/{name}.json"
        receipt = json.loads(receipt_path.read_bytes())
        for name, expected in receipt["raw_evidence_sha256"].items():
            relative = f"{receipt['raw_evidence_root']}/{name}"
            if files.get(relative) != expected:
                raise ValueError(f"historical evidence checksum mismatch: {relative}")
    evaluation = json.loads((root / "docs/validation/batch-release-model.json").read_bytes())
    for name, expected in evaluation["verified_file_checksums"].items():
        if name in files and files[name] != expected:
            raise ValueError(f"evaluation evidence checksum mismatch: {name}")
    training = evaluation["training"]
    bundle = ROOTS[0]
    for name, key in (
        ("baseline.json", "baseline_sha256"),
        ("static-model.txt", "static_model_sha256"),
        ("streaming-model.txt", "streaming_model_sha256"),
        ("model-card.md", "model_card_sha256"),
    ):
        if files[f"{bundle}/{name}"] != training[key]:
            raise ValueError(f"approved model checksum mismatch: {name}")
    for name in (
        "approved-model-deployment",
        "approved-model-operations",
        "batch-release-model",
        "kind-serving-load",
        "real-data-pilot",
        "real-data-workload",
        "static-model-promotion",
        "unknown-passenger-policy",
    ):
        path = root / f"docs/validation/{name}.json"
        files[path.relative_to(root).as_posix()] = digest(path)
    manifest = {
        "format_version": 1,
        "model_run_id": "98ef1dd9ef4447f7",
        "scope": "Native model, April requests, original evaluation/deployment/operations evidence",
        "excludes": ["raw TLC/gold partitions", "registry databases", "credentials", "images"],
        "files_sha256": dict(sorted(files.items())),
    }
    payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    output.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation preserves earlier attempts. Metadata is normalized for portability.
    with (
        output.open("xb") as destination,
        tarfile.open(fileobj=destination, mode="w:gz") as archive,
    ):
        for name in sorted(files):
            info = archive.gettarinfo(str(root / name), arcname=name)
            info.uid = info.gid = info.mtime = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            with (root / name).open("rb") as source:
                archive.addfile(info, source)
        info = tarfile.TarInfo(MANIFEST)
        info.size = len(payload)
        info.mode = 0o644
        archive.addfile(info, io.BytesIO(payload))
    verify(output)
    output.with_name(output.name + ".sha256").write_text(f"{digest(output)}  {output.name}\n")


def verify(archive_path: Path, destination: Path | None = None) -> dict[str, str]:
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if len(names) != len(set(names)):
            raise ValueError("duplicate archive paths")
        if any(not member.isfile() or not safe_name(member.name) for member in members):
            raise ValueError("archive must contain only safe regular files")
        stream = archive.extractfile(MANIFEST)
        if stream is None:
            raise ValueError("missing release manifest")
        manifest = json.load(stream)
        if manifest["format_version"] != 1:
            raise ValueError("unsupported release manifest")
        files: dict[str, str] = manifest["files_sha256"]
        if set(names) != set(files) | {MANIFEST}:
            raise ValueError("archive membership differs from manifest")
        for name, expected in files.items():
            source = archive.extractfile(name)
            if source is None or hashlib.file_digest(source, "sha256").hexdigest() != expected:
                raise ValueError(f"archive checksum mismatch: {name}")
        if destination is not None:
            destination.mkdir(parents=True, exist_ok=False)
            archive.extractall(destination, filter="data")
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    pack = commands.add_parser("build")
    pack.add_argument("--root", type=Path, default=Path.cwd())
    pack.add_argument("--output", type=Path, required=True)
    check = commands.add_parser("verify")
    check.add_argument("archive", type=Path)
    check.add_argument("--extract", type=Path, help="must be a new directory")
    args = parser.parse_args()
    if args.command == "build":
        build(args.root.resolve(), args.output)
        print(json.dumps({"archive": str(args.output), "sha256": digest(args.output)}))
    else:
        files = verify(args.archive, args.extract)
        print(json.dumps({"verified_files": len(files), "sha256": digest(args.archive)}))


if __name__ == "__main__":
    main()
