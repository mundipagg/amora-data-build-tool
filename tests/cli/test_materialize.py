import inspect
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from amora.cli import app
from amora.utils import clean_compiled_files

from tests.models.heart_rate import HeartRate
from tests.models.steps import Steps

runner = CliRunner()


def setup_function(module):
    clean_compiled_files()


def teardown_function(module):
    clean_compiled_files()


@patch("concurrent.futures.ProcessPoolExecutor")
@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
def test_materialize_without_arguments_and_options(
    materialize: MagicMock, compile: MagicMock, pool_mock: MagicMock
):
    executor_mock = MagicMock()
    executor_mock.map = MagicMock()
    pool_mock.return_value.__enter__.return_value = executor_mock

    models = [Steps, HeartRate]

    for model in models:
        target_path = model.target_path(model_file_path=inspect.getfile(model))
        target_path.write_text("SELECT 1")

    result = runner.invoke(
        app,
        ["materialize"],
    )

    assert result.exit_code == 0
    executor_mock.map.assert_called_once_with(
        materialize,
        ["SELECT 1"] * len(models),
        [model.unique_name() for model in models],
        [model.__model_config__ for model in models],
    )
    compile.assert_called_once_with(target=None, models=[])


@patch("concurrent.futures.ProcessPoolExecutor")
@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
def test_materialize_with_model_options(
    materialize: MagicMock, compile: MagicMock, pool_mock: MagicMock
):
    executor_mock = MagicMock()
    executor_mock.map = MagicMock()
    pool_mock.return_value.__enter__.return_value = executor_mock

    for model in [HeartRate, Steps]:
        target_path = model.target_path(model_file_path=inspect.getfile(model))
        target_path.write_text("SELECT 1")

    result = runner.invoke(
        app,
        ["materialize", "--model", "steps"],
    )

    assert result.exit_code == 0
    executor_mock.map.assert_called_once_with(
        materialize, ["SELECT 1"], [Steps.unique_name()], [Steps.__model_config__]
    )
    compile.assert_called_once_with(models=["steps"], target=None)


@patch("concurrent.futures.ProcessPoolExecutor")
@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
@patch("amora.dag.DependencyDAG.draw")
def test_materialize_with_draw_dag_option(
    draw: MagicMock, _materialize: MagicMock, _compile: MagicMock, _pool_mock: MagicMock
):

    result = runner.invoke(
        app,
        ["materialize", "--draw-dag"],
    )

    assert result.exit_code == 0
    draw.assert_called_once()


@patch("concurrent.futures.ProcessPoolExecutor")
@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
def test_materialize_with_no_compile_option(
    _materialize: MagicMock, compile: MagicMock, _pool_mock: MagicMock
):
    result = runner.invoke(
        app,
        ["materialize", "--no-compile"],
    )

    assert result.exit_code == 0
    compile.assert_not_called()
