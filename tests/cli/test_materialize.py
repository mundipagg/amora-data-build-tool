from unittest.mock import MagicMock, call, patch

from typer.testing import CliRunner

from amora.cli import app
from amora.compilation import remove_compiled_files

from tests.models.heart_rate import HeartRate
from tests.models.step_count_by_source import StepCountBySource
from tests.models.steps import Steps

runner = CliRunner()


def setup_function(module):
    remove_compiled_files()


def teardown_function(module):
    remove_compiled_files()


@patch("concurrent.futures.ProcessPoolExecutor")
@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
def test_materialize_without_arguments_and_options(
    materialize: MagicMock, compile: MagicMock, pool_mock: MagicMock
):
    executor_mock = MagicMock()
    executor_mock.map = MagicMock()
    pool_mock.return_value.__enter__.return_value = executor_mock

    models = [HeartRate, Steps, StepCountBySource]

    for model in models:
        target_path = model.target_path()
        target_path.write_text("SELECT 1")

    result = runner.invoke(
        app,
        ["materialize"],
    )

    assert result.exit_code == 0

    compile.assert_called_once_with(target=None, models=[])

    assert executor_mock.map.call_count == 2

    assert executor_mock.map.call_args_list == [
        call(
            materialize,
            ["SELECT 1"] * 2,
            [model.unique_name() for model in [HeartRate, Steps]],
            [model.__model_config__ for model in [HeartRate, Steps]],
        ),
        call(
            materialize,
            ["SELECT 1"],
            [StepCountBySource.unique_name()],
            [StepCountBySource.__model_config__],
        ),
    ]


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
        target_path = model.target_path()
        target_path.write_text("SELECT 1")

    result = runner.invoke(
        app,
        ["materialize", "--model", "steps"],
    )

    # breakpoint()
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
