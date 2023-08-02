from pathlib import Path

import pytest
from sqlalchemy import Integer, text

from amora.compilation import compile_statement
from amora.models import (
    AmoraModel,
    Field,
    Label,
    ModelConfig,
    amora_model_for_name,
    amora_model_for_path,
    amora_model_for_target_path,
    amora_model_from_name_list,
    select_models_with_label_keys,
    select_models_with_labels,
)
from amora.protocols import Compilable

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


def test_amora_model_from_name_list():
    expected_models = [Health, Steps]
    expected_models_name_list = [model.unique_name() for model in expected_models]

    models = list(amora_model_from_name_list(expected_models_name_list))

    assert len(expected_models) == len(models)
    for (model, _), expected_model in zip(
        sorted(models, key=lambda x: x[0].unique_name()),
        sorted(expected_models, key=lambda x: x.unique_name()),
    ):
        assert issubclass(model, AmoraModel)
        assert expected_model.__table__ == model.__table__


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


def test_raw_sql_as_source():
    class RawSqlModel(AmoraModel):
        __depends_on__ = [Health]

        id: int = Field(Integer, primary_key=True, doc=Health.id.__doc__)

        @classmethod
        def source(cls) -> Compilable:
            return (
                text(
                    f"""
    with source as (
        select id from {Health.fully_qualified_name()}
    ) select * from source
    """
                )
                .columns(id=Integer)
                .subquery()
            )

    assert (
        compile_statement(RawSqlModel.source())
        == """
with source as
  (select id
   from amora-data-build-tool.amora.health)
select *
from source
""".strip()
    )
