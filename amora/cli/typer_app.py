from concurrent import futures
from typing import Dict, List, Optional

import pytest
import typer

from amora import compilation, manifest, materialization, utils
from amora.cli import dash, feature_store, models
from amora.cli.shared_options import force_option, models_option, target_option
from amora.cli.type_specs import Models
from amora.config import settings
from amora.dag import DependencyDAG
from amora.models import list_models

app = typer.Typer(
    pretty_exceptions_enable=False,
    help="Amora Data Build Tool enables engineers to transform data in their warehouses "
    "by defining schemas and writing select statements with SQLAlchemy. Amora handles turning these "
    "select statements into tables and views",
)


@app.command()
def compile(
    models: Optional[Models] = models_option,
    target: Optional[str] = target_option,
    force: Optional[bool] = force_option,
) -> None:
    """
    Generates executable SQL from model files. Compiled SQL files are written to the `./target` directory.
    """

    current_manifest = manifest.Manifest.from_project()
    previous_manifest = manifest.Manifest.load()

    if force or not previous_manifest:
        utils.clean_compiled_files()
        models_to_compile = list_models()
    else:
        compilation.clean_compiled_files_of_removed_models(
            list(previous_manifest.models.keys()), list(current_manifest.models.keys())
        )
        models_to_compile = current_manifest.get_models_to_compile(previous_manifest)

    for model, model_file_path in models_to_compile:
        if models and model_file_path.stem not in models:
            continue

        source_sql_statement = model.source()
        if source_sql_statement is None:
            typer.echo(f"⏭ Skipping compilation of model `{model_file_path}`")
            continue

        target_file_path = model.target_path()
        typer.echo(f"🏗 Compiling model `{model_file_path}` -> `{target_file_path}`")

        content = compilation.compile_statement(source_sql_statement)
        target_file_path.parent.mkdir(parents=True, exist_ok=True)
        target_file_path.write_text(content)

    current_manifest.save()


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

    for target_file_path in utils.list_target_files():
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
                    typer.echo(result)


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def test(ctx: typer.Context) -> None:
    """
    Runs tests on data in deployed models. Run this after `amora materialize`
    to ensure that the data state is up-to-date. Optional arguments are passed
    to pytest.
    """

    pytest_args = settings.DEFAULT_PYTEST_ARGS + ctx.args
    return_code = pytest.main(pytest_args)

    raise typer.Exit(return_code)


app.add_typer(dash.app, name="dash")
app.add_typer(models.app, name="models")
app.add_typer(feature_store.app, name="feature-store")
