import inspect
import itertools
from typing import List
from unittest.mock import patch, call, MagicMock

import pytest
from amora.compilation import clean_compiled_files
from amora.models import list_target_files
from typer.testing import CliRunner
from tests.models.heart_rate import HeartRate
from tests.models.steps import Steps

from amora.cli import app

runner = CliRunner()


def setup_function(module):
    clean_compiled_files()


def teardown_function(module):
    clean_compiled_files()


@pytest.mark.parametrize(
    "models, expected_exit_code, expected_target_files",
    [
        (["health"], 0, []),
        (["heart_rate", "health", "steps"], 0, ["heart_rate.sql", "steps.sql"]),
        (["modelo_que_nao_existe"], 0, []),
    ],
)
def test_compile_with_models_options(
    models: List[str], expected_exit_code: int, expected_target_files: List[str]
):
    # todo: implementar função clean_target
    # todo: clean_target()
    models_args = [("--model", model) for model in models]
    result = runner.invoke(
        app,
        ["compile", *itertools.chain.from_iterable(models_args)],
    )
    assert result.exit_code == expected_exit_code
    # todo: adicionar verbosidade e testes sobre o output em stdout
    generated_target_files = [path.name for path in list_target_files()]
    assert sorted(generated_target_files) == sorted(expected_target_files)


def test_compile_without_arguments_and_options():
    result = runner.invoke(
        app,
        ["compile"],
    )

    assert result.exit_code == 0

    generated_target_files = [path.name for path in list_target_files()]
    assert sorted(generated_target_files) == sorted(
        ["heart_agg.sql", "heart_rate.sql", "steps.sql"]
    )


@patch("amora.cli.materialization.materialize")
def test_materialize_without_arguments_and_options(materialize: MagicMock):

    for model in [HeartRate, Steps]:
        target_path = model.target_path(model_file_path=inspect.getfile(model))
        target_path.write_text("SELECT 1")

    result = runner.invoke(
        app,
        ["materialize"],
    )

    assert result.exit_code == 0

    [
        materialize_call_steps,
        materialize_call_heart_rate,
    ] = materialize.call_args_list
    assert materialize_call_steps[1]["model"].__table__ == Steps.__table__
    assert materialize_call_heart_rate[1]["model"].__table__ == HeartRate.__table__


@patch("amora.cli.materialization.materialize")
def test_materialize_with_model_options(materialize: MagicMock):

    for model in [HeartRate, Steps]:
        target_path = model.target_path(model_file_path=inspect.getfile(model))
        target_path.write_text("SELECT 1")

    result = runner.invoke(
        app,
        ["materialize", "--model", "steps"],
    )

    assert result.exit_code == 0
    materialize_call_model = materialize.mock_calls[0][2]["model"]
    assert materialize_call_model.__table__ == Steps.__table__


@patch("amora.cli.materialization.materialize")
@patch("amora.cli.materialization.DependencyDAG.draw")
def test_materialize_with_draw_dag_option(draw: MagicMock, _materialize: MagicMock):
    result = runner.invoke(
        app,
        ["materialize", "--draw-dag"],
    )

    assert result.exit_code == 0
    draw.assert_called_once()
