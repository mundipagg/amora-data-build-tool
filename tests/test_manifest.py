import json
from pathlib import Path
from tempfile import NamedTemporaryFile

from _hashlib import HASH

from amora.config import settings
from amora.manifest import generate_manifest, hash_file, load_manifest, save_manifest


def test_hash_file():
    with NamedTemporaryFile(suffix=".py") as f:
        hash = hash_file(Path(f.name))
        assert isinstance(hash, HASH)


def test_generate_manifest():
    manifest = generate_manifest()
    model_manifest_keys = sorted(["stat", "size", "hash", "path", "deps"])

    assert "models" in manifest

    for _, values in manifest["models"].items():
        assert model_manifest_keys == sorted(values.keys())


def test_save_manifest():
    manifest = {"a": 1}
    with NamedTemporaryFile(suffix=".json") as manifest_file:
        manifest_path = Path(manifest_file.name)
        settings.MANIFEST_PATH = manifest_path
        save_manifest(manifest)
        assert json.load(manifest_file) == manifest


def test_load_manifest():
    manifest = {"a": 1}
    with NamedTemporaryFile(suffix=".json") as manifest_file:
        manifest_path = Path(manifest_file.name)
        settings.MANIFEST_PATH = manifest_path
        save_manifest(manifest)
        assert json.load(manifest_file) == manifest


def test_load_manifest_not_found():
    manifest = {"a": 1}
    invalid_manifest_path = Path("not-a-real-file-path")
    settings.MANIFEST_PATH = invalid_manifest_path

    manifest = load_manifest()

    assert manifest == {}
