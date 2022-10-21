import json
from pathlib import Path
from tempfile import NamedTemporaryFile

from _hashlib import HASH

from amora.config import settings
from amora.manifest import Manifest, ModelMetadata, hash_file


def test_hash_file():
    with NamedTemporaryFile(suffix=".py") as f:
        hash = hash_file(Path(f.name))
        assert isinstance(hash, HASH)


def test_Manifest_from_project():
    manifest = Manifest.from_project()

    expected_models_keys = [
        "amora-data-build-tool.amora.step_count_by_source",
        "amora-data-build-tool.amora.health",
        "amora-data-build-tool.amora.array_repeated_fields",
        "amora-data-build-tool.amora.steps",
        "amora-data-build-tool.amora.heart_rate_agg",
        "amora-data-build-tool.amora.heart_rate",
        "amora-data-build-tool.amora.heart_rate_over100",
    ]

    assert sorted(list(manifest.models.keys())) == sorted(expected_models_keys)


def test_Manifest_save():
    model_metadata = ModelMetadata(
        stat=11.2, size=1.5, hash="abc", path="/some/path/a.py", deps=["a"]
    )
    manifest = Manifest(models={"a": model_metadata})

    with NamedTemporaryFile(suffix=".json") as manifest_file:
        manifest_path = Path(manifest_file.name)
        settings.MANIFEST_PATH = manifest_path
        manifest.save()

        assert json.load(manifest_file) == manifest


def test_Manifest_load():
    model_metadata = ModelMetadata(
        stat=11.2, size=1.5, hash="abc", path="/some/path/a.py", deps=["a"]
    )
    manifest = Manifest(models={"a": model_metadata})

    with NamedTemporaryFile(suffix=".json") as manifest_file:
        manifest_path = Path(manifest_file.name)
        settings.MANIFEST_PATH = manifest_path

        with open(manifest_path, "w+") as f:
            json.dump(manifest.dict(), f)

        assert manifest.load() == manifest


def test_Manifest_load_not_found():
    invalid_manifest_path = Path("not-a-real-file-path")
    settings.MANIFEST_PATH = invalid_manifest_path

    manifest = Manifest.load()

    assert manifest is None
