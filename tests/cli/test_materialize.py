from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from amora.cli import app
from amora.utils import clean_compiled_files

runner = CliRunner()


def setup_function(module):
    clean_compiled_files()


def teardown_function(module):
    clean_compiled_files()


@patch("amora.materialization.create_materialization_tasks")
@patch("amora.dag.DependencyDAG.from_tasks")
@patch("typer.echo")
@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize_dag")
def test_materialize_without_arguments_and_options(
    materialize_dag: MagicMock,
    compile: MagicMock,
    echo: MagicMock,
    from_tasks: MagicMock,
    create_materialization_tasks: MagicMock,
):
    tasks = MagicMock()
    dag = MagicMock()

    create_materialization_tasks.return_value = tasks
    from_tasks.return_value = dag

    result = runner.invoke(
        app,
        ["materialize"],
    )

    assert result.exit_code == 0

    dag.draw.assert_not_called()

    compile.assert_called_once_with(models=[], target=None)
    materialize_dag.assert_called_once_with(dag, tasks, echo)


@patch("amora.materialization.create_materialization_tasks")
@patch("amora.dag.DependencyDAG.from_tasks")
@patch("typer.echo")
@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize_dag")
def test_materialize_with_model_options(
    materialize_dag: MagicMock,
    compile: MagicMock,
    echo: MagicMock,
    from_tasks: MagicMock,
    create_materialization_tasks: MagicMock,
):
    tasks = MagicMock()
    dag = MagicMock()

    create_materialization_tasks.return_value = tasks
    from_tasks.return_value = dag

    result = runner.invoke(
        app,
        ["materialize", "--model", "steps"],
    )

    assert result.exit_code == 0

    dag.draw.assert_not_called()

    compile.assert_called_once_with(models=["steps"], target=None)
    materialize_dag.assert_called_once_with(dag, tasks, echo)


@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize_dag")
@patch("amora.dag.DependencyDAG.draw")
def test_materialize_with_draw_dag_option(
    draw: MagicMock, materialize_dag: MagicMock, _compile: MagicMock
):
    result = runner.invoke(
        app,
        ["materialize", "--draw-dag"],
    )

    assert result.exit_code == 0
    draw.assert_called_once()


@patch("amora.cli.typer_app.compile")
@patch("amora.materialization.materialize_dag")
def test_materialize_with_no_compile_option(
    materialize_dag: MagicMock, compile: MagicMock
):
    result = runner.invoke(
        app,
        ["materialize", "--no-compile"],
    )

    assert result.exit_code == 0
    compile.assert_not_called()
