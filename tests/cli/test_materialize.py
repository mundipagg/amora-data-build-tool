import inspect
from unittest.mock import patch, MagicMock

from amora.utils import clean_compiled_files
from typer.testing import CliRunner
from tests.models.heart_rate import HeartRate
from tests.models.steps import Steps

from amora.cli import app

runner = CliRunner()


def setup_function(module):
    clean_compiled_files()


def teardown_function(module):
    clean_compiled_files()


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

    tables = sorted(
        [str(call[1]["model"].__table__) for call in materialize.call_args_list]
    )
    assert tables == sorted([str(Steps.__table__), str(HeartRate.__table__)])


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
