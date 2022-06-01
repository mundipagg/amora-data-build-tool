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


@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
def test_materialize_without_arguments_and_options(
    materialize: MagicMock, compile: MagicMock
):

    for model in [HeartRate, Steps]:
        target_path = model.target_path(model_file_path=inspect.getfile(model))
        target_path.write_text("SELECT 1")

    result = runner.invoke(
        app,
        ["materialize"],
    )

    assert result.exit_code == 0

    tables = sorted(
        str(call[1]["model"].__table__) for call in materialize.call_args_list
    )
    assert tables == sorted([str(Steps.__table__), str(HeartRate.__table__)])
    compile.assert_called_once_with(target=None, models=tuple())


@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
def test_materialize_with_model_options(materialize: MagicMock, compile: MagicMock):

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

    compile.assert_called_once_with(models=("steps",), target=None)


@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
@patch("amora.dag.DependencyDAG.draw")
def test_materialize_with_draw_dag_option(
    draw: MagicMock, _materialize: MagicMock, _compile: MagicMock
):
    result = runner.invoke(
        app,
        ["materialize", "--draw-dag"],
    )

    assert result.exit_code == 0
    draw.assert_called_once()


@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize")
def test_materialize_with_no_compile_option(materialize: MagicMock, compile: MagicMock):
    result = runner.invoke(
        app,
        ["materialize", "--no-compile"],
    )

    assert result.exit_code == 0
    compile.assert_not_called()
