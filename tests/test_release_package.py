from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import tarfile
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture
def packager() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "package_release", Path(__file__).parents[1] / "scripts/package-release.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_archive(path: Path, name: str, *, damage: str | None = None) -> None:
    content = b"native model bytes"
    manifest = {
        "format_version": 1,
        "files_sha256": {name: hashlib.sha256(content).hexdigest()},
    }
    with tarfile.open(path, "w:gz") as archive:
        entries = [(name, b"changed" if damage == "checksum" else content)]
        if damage == "duplicate":
            entries.append((name, content))
        if damage == "extra":
            entries.append(("unlisted", content))
        entries.append(("release-manifest.json", json.dumps(manifest).encode()))
        for entry, payload in entries:
            info = tarfile.TarInfo(entry)
            info.size = len(payload)
            if damage == "symlink" and entry == name:
                info.type = tarfile.SYMTYPE
                info.linkname = "outside"
                info.size = 0
            archive.addfile(info, io.BytesIO(payload))


def test_verified_archive_extracts_once(packager: ModuleType, tmp_path: Path) -> None:
    archive = tmp_path / "release.tar.gz"
    destination = tmp_path / "unpacked"
    make_archive(archive, "model/native.txt")
    assert len(packager.verify(archive, destination)) == 1
    assert (destination / "model/native.txt").read_bytes() == b"native model bytes"
    with pytest.raises(FileExistsError):
        packager.verify(archive, destination)


@pytest.mark.parametrize("damage", ["checksum", "duplicate", "extra", "symlink"])
def test_invalid_archive_never_extracts(packager: ModuleType, tmp_path: Path, damage: str) -> None:
    archive = tmp_path / "release.tar.gz"
    destination = tmp_path / "unpacked"
    make_archive(archive, "model/native.txt", damage=damage)
    with pytest.raises(ValueError, match=r"checksum|duplicate|membership|safe regular files"):
        packager.verify(archive, destination)
    assert not destination.exists()


@pytest.mark.parametrize("name", ["../escape", "/absolute", "model/../escape", "./alias"])
def test_archive_rejects_unsafe_paths(packager: ModuleType, tmp_path: Path, name: str) -> None:
    archive = tmp_path / "release.tar.gz"
    make_archive(archive, name)
    with pytest.raises(ValueError, match="safe regular files"):
        packager.verify(archive, tmp_path / "unpacked")
