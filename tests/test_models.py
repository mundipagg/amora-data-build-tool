from pathlib import Path

import pytest

from amora.compilation import compile_statement
from amora.models import (
    AmoraModel,
    Field,
    Label,
    ModelConfig,
    amora_model_for_name,
    amora_model_for_path,
    amora_model_for_target_path,
    select_models_with_label_keys,
    select_models_with_labels,
)

from tests.models.deeply.nested.array_repeated_fields import ArrayRepeatedFields
from tests.models.health import Health
from tests.models.step_count_by_source import StepCountBySource
from tests.models.steps import Steps


def test_target_path():
    path = Health.target_path()
    assert path.as_posix().endswith("target/health.sql")


def test_model_without_dependencies():
    assert Health.dependencies() == []


def test_model_with_dependencies():
    assert Steps.dependencies() == [Health.__table__]


def test_model_without_source():
    assert Health.source() is None


def test_model_with_compilable_source():
    assert Steps.source() is not None
    assert isinstance(compile_statement(Steps.source()), str)


def test_amora_model_for_name():
    model = amora_model_for_name(Health.unique_name())

    assert issubclass(model, AmoraModel)
    assert model.__table__ == Health.__table__


def test_amora_model_for_name_on_invalid_name():
    with pytest.raises(ValueError):
        amora_model_for_name("Apolo")


def test_amora_model_for_path():
    model = amora_model_for_path(Health.path())
    assert issubclass(model, AmoraModel)
    assert model.__table__ == Health.__table__


def test_amora_model_for_path_with_model_on_nested_folders():
    model = amora_model_for_path(ArrayRepeatedFields.path())
    assert issubclass(model, AmoraModel)
    assert model.__table__ == ArrayRepeatedFields.__table__


def test_amora_model_for_path_on_invalid_path():
    invalid_model_path = Path(__file__)

    with pytest.raises(ValueError):
        amora_model_for_path(invalid_model_path)


def test_amora_model_for_target_path():
    model = amora_model_for_target_path(Health.target_path())
    assert issubclass(model, AmoraModel)
    assert model.__table__ == Health.__table__


def test_amora_model_for_target_path_on_invalid_path():
    invalid_model_path = Path(__file__)

    with pytest.raises(ValueError):
        amora_model_for_target_path(invalid_model_path)


def test_select_models_with_labels():
    models = list(select_models_with_labels({Label("domain", "health")}))

    assert len(models) == 1
    [(model, model_path)] = models

    assert model.unique_name() == StepCountBySource.unique_name()
    assert model_path == StepCountBySource.path()


def test_select_models_with_labels_without_matches():
    assert not list(select_models_with_labels({Label("domain", "🐣")}))


def test_select_models_with_label_keys():
    models = list(select_models_with_label_keys({"downstream"}))

    assert len(models) == 1
    [(model, model_path)] = models

    assert model.unique_name() == StepCountBySource.unique_name()
    assert model_path == StepCountBySource.path()


def test_select_models_with_label_keys_without_matches():
    label_keys = {"🥚", "🐣", "🐥", "🐓", "🍗"}
    assert not list(select_models_with_label_keys(label_keys))


def test_label__eq__():
    assert Label("domain", "health") == Label("domain", "health")
    assert Label("domain", "health") == "domain:health"


def test_owner():
    class ModelWithOwner(AmoraModel):
        __model_config__ = ModelConfig(owner="John Doe <john@example.com")
        id: int = Field(primary_key=True)

    assert ModelWithOwner.owner() == "John Doe <john@example.com"


def test_without_owner():
    class ModelWithoutOwner(AmoraModel):
        id: int = Field(primary_key=True)

    assert ModelWithoutOwner.owner() == ""
