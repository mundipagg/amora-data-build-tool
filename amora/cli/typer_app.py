from concurrent import futures
from typing import Dict, List, Optional

import pytest
import typer

from amora import materialization
from amora.cli import dash, feature_store, models
from amora.cli.shared_options import models_option, target_option
from amora.cli.type_specs import Models
from amora.compilation import compile_statement
from amora.config import settings
from amora.dag import DependencyDAG
from amora.models import list_models
from amora.utils import list_target_files

app = typer.Typer(
    help="Amora Data Build Tool enables engineers to transform data in their warehouses "
    "by defining schemas and writing select statements with SQLAlchemy. Amora handles turning these "
    "select statements into tables and views"
)


@app.command()
def compile(
    models: Optional[Models] = models_option,
    target: Optional[str] = target_option,
) -> None:
    """
    Generates executable SQL from model files. Compiled SQL files are written to the `./target` directory.
    """
    for model, model_file_path in list_models():
        if models and model_file_path.stem not in models:
            continue

        source_sql_statement = model.source()
        if source_sql_statement is None:
            typer.echo(f"⏭ Skipping compilation of model `{model_file_path}`")
            continue

        target_file_path = model.target_path(model_file_path)
        typer.echo(f"🏗 Compiling model `{model_file_path}` -> `{target_file_path}`")

        content = compile_statement(source_sql_statement)
        target_file_path.parent.mkdir(parents=True, exist_ok=True)
        target_file_path.write_text(content)


@app.command()
def materialize(
    models: Optional[Models] = models_option,
    target: str = target_option,
    draw_dag: bool = typer.Option(False, "--draw-dag"),
    no_compile: bool = typer.Option(
        False,
        "--no-compile",
        help="Don't run `amora compile` before the materialization",
    ),
) -> None:
    """
    Executes the compiled SQL against the current target database.

    """
    if not no_compile:
        compile(models=models, target=target)

    model_to_task: Dict[str, materialization.Task] = {}

    for target_file_path in list_target_files():
        if models and target_file_path.stem not in models:
            continue

        task = materialization.Task.for_target(target_file_path)
        model_to_task[task.model.unique_name()] = task

    dag = DependencyDAG.from_tasks(tasks=model_to_task.values())

    if draw_dag:
        dag.draw()

    with futures.ProcessPoolExecutor(
        max_workers=settings.MATERIALIZE_NUM_THREADS
    ) as executor:
        for models_to_materialize in dag.topological_generations():

            current_tasks: List[materialization.Task] = []
            for model_name in models_to_materialize:
                if model_name in model_to_task:
                    current_tasks.append(model_to_task[model_name])
                else:
                    typer.echo(f"⚠️  Skipping `{model_name}`")
                    continue

            if not current_tasks:
                continue

            results = executor.map(
                materialization.materialize,
                [current_task.sql_stmt for current_task in current_tasks],
                [current_task.model.unique_name() for current_task in current_tasks],
                [current_task.model.__model_config__ for current_task in current_tasks],
            )

            for result in results:
                if result:
                    typer.echo(f"✅  Created `{result.full_table_id}`")
                    typer.echo(f"    Rows: {result.num_rows}")
                    typer.echo(f"    Bytes: {result.num_bytes}")


@app.command()
def test(
    models: Optional[Models] = models_option,
) -> None:
    """
    Runs tests on data in deployed models. Run this after `amora materialize`
    to ensure that the date state is up-to-date.
    """
    return_code = pytest.main(["-n", "auto", "--verbose"])
    raise typer.Exit(return_code)


app.add_typer(dash.app, name="dash")
app.add_typer(models.app, name="models")
app.add_typer(feature_store.app, name="feature-store")
