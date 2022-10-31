import json
from copy import deepcopy
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
from _hashlib import HASH

from amora.config import settings
from amora.manifest import Manifest, ModelMetadata, get_models_to_compile, hash_file
from amora.models import amora_model_for_path

from tests.models.health import Health
from tests.models.step_count_by_source import StepCountBySource
from tests.models.steps import Steps


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


@pytest.fixture()
def sample_manifest():
    return Manifest(
        models={
            "amora-data-build-tool.amora.health": ModelMetadata(
                stat=1664199318.9002378,
                size=1181,
                hash="1a3de26089812ca0b731588a7dd2f3f2",
                path="amora-data-build-tool/examples/amora_project/models/health.py",
                deps=[],
            ),
            "amora-data-build-tool.amora.steps": ModelMetadata(
                stat=1665147859.6682658,
                size=1937,
                hash="c8981d729c89dd8f6afcbe6671fa8c62",
                path="amora-data-build-tool/examples/amora_project/models/steps.py",
                deps=[],
            ),
        },
    )


@mock.patch("amora.manifest.list_models")
def test_get_models_to_compile_when_both_manifests_are_equal(
    list_models: mock.MagicMock, sample_manifest: Manifest
):
    """
    Case:
        previous_manifest == current_manifest
    Expected Output:
        no model need to recompile.
    """

    list_models.return_value = [
        (Steps, Steps.path()),
        (Health, Health.path()),
    ]

    models_to_compile = get_models_to_compile(sample_manifest, sample_manifest)

    assert len(models_to_compile) == 0


@mock.patch("amora.manifest.list_models")
def test_get_models_to_compile_when_has_new_model(
    list_models: mock.MagicMock, sample_manifest: Manifest
):
    """
    Case:
        current_manifest has a new model that isnt in previous_manifest
    Expected Output:
        recompile the new model.
    """

    list_models.return_value = [
        (Steps, Steps.path()),
        (Health, Health.path()),
        (StepCountBySource, StepCountBySource.path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models[StepCountBySource.unique_name()] = ModelMetadata(
        stat=1664199318.9035711,
        size=4714,
        hash="3df3f2761805fb1ece39581f21fbaf0a",
        path="amora-data-build-tool/examples/amora_project/models/steps_count_by_source.py",
        deps=[],
    )

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(StepCountBySource, StepCountBySource.path())}


@mock.patch("amora.manifest.list_models")
def test_get_models_to_compile_when_a_model_has_changed_size(
    list_models: mock.MagicMock, sample_manifest: Manifest
):
    """
    Case:
        a model has changed it size in the current_manifest.
    Expected Output:
        recompile that model.
    """

    list_models.return_value = [
        (Steps, Steps.path()),
        (Health, Health.path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.steps"].size = 4714

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(Steps, Steps.path())}


@mock.patch("amora.manifest.list_models")
def test_get_models_to_compile_when_a_model_has_changed_deps(
    list_models: mock.MagicMock, sample_manifest: Manifest
):
    """
    Case:
        a model has changed it dependencies in the current_manifest.
    Expected Output:
        recompile the model.
    """

    list_models.return_value = [
        (Steps, Steps.path()),
        (Health, Health.path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.steps"].deps = ["another-model"]

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(Steps, Steps.path())}


@mock.patch("amora.manifest.list_models")
def test_get_models_to_compile_when_a_model_stat_was_updated_and_has_no_target_file(
    list_models: mock.MagicMock, sample_manifest: Manifest
):
    """
    Case:
        a model has been updated (key: stat) in the current_manifest and the target
        file of that model doenst exists.
    Expected Output:
        recompile the model.
    """

    list_models.return_value = [
        (Steps, Steps.path()),
        (Health, Health.path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.steps"].stat = 2665147859.6682658

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(Steps, Steps.path())}


@mock.patch("amora.manifest.list_models")
def test_get_models_to_compile_when_a_model_stat_was_updated_and_hashes_are_different(
    list_models: mock.MagicMock, sample_manifest: Manifest
):
    """
    Case:
        a model has been updated (key: stat) and the hashes are different in the current_manifest
        and the target file of that model do exists.
    Expected Output:
        recompile the model.
    """

    list_models.return_value = [
        (Steps, Steps.path()),
        (Health, Health.path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.steps"].stat = 2665147859.6682658
    new_manifest.models[
        "amora-data-build-tool.amora.steps"
    ].hash = "some-hash-different-from-previous-one"

    target_path = Steps.target_path()
    target_path.write_text("SELECT 1")

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(Steps, Steps.path())}


@mock.patch("amora.manifest.list_models")
def test_get_models_to_compile_return_all_dependencies_from_a_changed_model(
    list_models: mock.MagicMock, sample_manifest: Manifest
):
    """
    Case:
        a model has changed in the current_manifest
    Expected Output:
        recompile the model and its dependencies.
    """

    expected_models_to_compile = [
        (amora_model_for_path(Steps.path()), Steps.path()),
        (amora_model_for_path(Health.path()), Health.path()),
        (
            amora_model_for_path(StepCountBySource.path()),
            StepCountBySource.path(),
        ),
    ]

    list_models.return_value = expected_models_to_compile

    sample_manifest.models["amora-data-build-tool.amora.health"].deps = [
        "amora-data-build-tool.amora.steps",
        "amora-data-build-tool.amora.step_count_by_source",
    ]
    sample_manifest.models["amora-data-build-tool.amora.steps"].deps = [
        "amora-data-build-tool.amora.step_count_by_source"
    ]
    sample_manifest.models[StepCountBySource.unique_name()] = ModelMetadata(
        stat=1664199318.9035711,
        size=4714,
        hash="3df3f2761805fb1ece39581f21fbaf0a",
        path="amora-data-build-tool/examples/amora_project/models/steps_count_by_source.py",
        deps=[],
    )
    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.health"].size = 1920

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 3

    # não podemos testar model == expected_model pois os modelos de "dependência"
    # serão carregados novamente com amora_model_for_path
    assert sorted(
        [
            "amora-data-build-tool.amora.health",
            "amora-data-build-tool.amora.steps",
            "amora-data-build-tool.amora.step_count_by_source",
        ]
    ) == sorted(model.unique_name() for model, _ in expected_models_to_compile)
