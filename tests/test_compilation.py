from copy import deepcopy
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
from sqlalchemy import Integer, String, func
from sqlalchemy_bigquery.base import BQArray

from amora.compilation import compile_statement, get_models_to_compile
from amora.manifest import Manifest, ModelMetadata
from amora.models import AmoraModel, amora_model_for_path, select
from amora.providers.bigquery import fixed_unnest

from tests.models.array_repeated_fields import ArrayRepeatedFields
from tests.models.health import Health
from tests.models.step_count_by_source import StepCountBySource
from tests.models.steps import Steps


def test_amora_model_for_path_with_invalid_file_path_type():
    with NamedTemporaryFile(suffix=".sql") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=rf".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_file_path():
    invalid_model_path = Path("not-a-real-file-name")
    with pytest.raises(ValueError, match=rf".*{invalid_model_path}.*"):
        amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_python_file_path():
    with NamedTemporaryFile(suffix=".py") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=rf".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_bytecode_python_file_path():
    with NamedTemporaryFile(suffix=".pyc") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=rf".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_valid_python_file_path():
    model = amora_model_for_path(path=Path(__file__).parent.joinpath("models/steps.py"))
    assert issubclass(model, AmoraModel)


def test_AmoraBigQueryCompiler_array__getitem__with_integer_item():
    version = func.split("123.45", ".", type_=BQArray(String))[2]
    statement = select(func.cast(version, Integer))
    query_str = compile_statement(statement)

    assert (
        query_str
        == "SELECT CAST((split('123.45', '.'))[OFFSET(2)] AS INT64) AS `anon_1`"
    )


def test_AmoraBigQueryCompiler_array__getitem__with_function_item():
    version = func.split("123.45", ".", type_=BQArray(String))[func.safe_ordinal(2)]
    statement = select(func.cast(version, Integer))
    query_str = compile_statement(statement)

    assert (
        query_str
        == "SELECT CAST((split('123.45', '.'))[safe_ordinal(2)] AS INT64) AS `anon_1`"
    )


def test_AmoraBigQueryCompiler_visit_function_with_offset():
    offset_alias = "off_alias"
    stmt = fixed_unnest(ArrayRepeatedFields.int_arr).table_valued(
        with_offset=offset_alias
    )
    compiled = compile_statement(stmt)
    assert compiled.endswith(f"OFFSET AS {offset_alias}")


def test_AmoraBigQueryCompiler_visit_function_without_offset():
    stmt = fixed_unnest(ArrayRepeatedFields.int_arr).table_valued()
    compiled = compile_statement(stmt)

    assert compiled == "unnest(`array_repeated_fields`.`int_arr`)"


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


@mock.patch("amora.compilation.list_models")
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
        (Steps, Steps.model_file_path()),
        (Health, Health.model_file_path()),
    ]

    models_to_compile = get_models_to_compile(sample_manifest, sample_manifest)

    assert len(models_to_compile) == 0


@mock.patch("amora.compilation.list_models")
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
        (Steps, Steps.model_file_path()),
        (Health, Health.model_file_path()),
        (StepCountBySource, StepCountBySource.model_file_path()),
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
    assert models_to_compile == {
        (StepCountBySource, StepCountBySource.model_file_path())
    }


@mock.patch("amora.compilation.list_models")
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
        (Steps, Steps.model_file_path()),
        (Health, Health.model_file_path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.steps"].size = 4714

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(Steps, Steps.model_file_path())}


@mock.patch("amora.compilation.list_models")
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
        (Steps, Steps.model_file_path()),
        (Health, Health.model_file_path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.steps"].deps = ["another-model"]

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(Steps, Steps.model_file_path())}


@mock.patch("amora.compilation.list_models")
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
        (Steps, Steps.model_file_path()),
        (Health, Health.model_file_path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.steps"].stat = 2665147859.6682658

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(Steps, Steps.model_file_path())}


@mock.patch("amora.compilation.list_models")
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
        (Steps, Steps.model_file_path()),
        (Health, Health.model_file_path()),
    ]

    new_manifest = deepcopy(sample_manifest)
    new_manifest.models["amora-data-build-tool.amora.steps"].stat = 2665147859.6682658
    new_manifest.models[
        "amora-data-build-tool.amora.steps"
    ].hash = "some-hash-different-from-previous-one"

    target_path = Steps.target_path(model_file_path=Steps.model_file_path())
    target_path.write_text("SELECT 1")

    models_to_compile = get_models_to_compile(sample_manifest, new_manifest)

    assert len(models_to_compile) == 1
    assert models_to_compile == {(Steps, Steps.model_file_path())}


@mock.patch("amora.compilation.list_models")
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
        (amora_model_for_path(Steps.model_file_path()), Steps.model_file_path()),
        (amora_model_for_path(Health.model_file_path()), Health.model_file_path()),
        (
            amora_model_for_path(StepCountBySource.model_file_path()),
            StepCountBySource.model_file_path(),
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
