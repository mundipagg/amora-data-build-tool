import json
from dataclasses import dataclass
from pathlib import Path

import pytest
import typer
from typing import Optional, List
from jinja2 import Environment, PackageLoader, select_autoescape
from rich.console import Console
from rich.table import Table

from amora.compilation import amora_model_for_path

from amora.config import settings
from amora.models import (
    list_model_files,
    is_py_model,
    AmoraModel,
    list_target_files,
)
from amora.compilation import compile_statement
from amora import materialization
from amora.providers.bigquery import (
    dry_run,
    get_schema,
    BIGQUERY_TYPES_TO_PYTHON_TYPES,
)

app = typer.Typer(
    help="Amora Data Build Tool enables engineers to transform data in their warehouses "
    "by defining schemas and writing select statements with SQLAlchemy. Amora handles turning these "
    "select statements into tables and views"
)

Models = List[str]
target_option = typer.Option(
    None,
    "--target",
    "-t",
    help="Target connection configuration as defined as an amora.target.Target",
)

models_option = typer.Option(
    [],
    "--model",
    help="A model to be compiled. This option can be passed multiple times.",
)


@app.command()
def compile(
    models: Optional[Models] = models_option,
    target: Optional[str] = target_option,
) -> None:
    """
    Generates executable SQL from model files. Compiled SQL files are written to the `./target` directory.
    """
    for model_file_path in list_model_files():
        if models and model_file_path.stem not in models:
            continue

        try:
            AmoraModel_class = amora_model_for_path(model_file_path)
        except ValueError:
            continue

        if not issubclass(AmoraModel_class, AmoraModel):  # type: ignore
            continue

        source_sql_statement = AmoraModel_class.source()
        if source_sql_statement is None:
            typer.echo(f"⏭ Skipping compilation of model `{model_file_path}`")
            continue

        target_file_path = AmoraModel_class.target_path(model_file_path)
        typer.echo(
            f"🏗 Compiling model `{model_file_path}` -> `{target_file_path}`"
        )

        content = compile_statement(source_sql_statement)
        target_file_path.write_text(content)


@app.command()
def materialize(
    models: Optional[Models] = models_option,
    target: str = target_option,
    draw_dag: bool = typer.Option(False, "--draw-dag"),
) -> None:
    """
    Executes the compiled SQL againts the current target database.
    """
    model_to_task = {}

    for target_file_path in list_target_files():
        if models and target_file_path.stem not in models:
            continue

        task = materialization.Task.for_target(target_file_path)
        model_to_task[task.model.__name__] = task

    dag = materialization.DependencyDAG.from_tasks(tasks=model_to_task.values())

    if draw_dag:
        dag.draw()

    for model in dag:
        try:
            task = model_to_task[model]
        except KeyError:
            typer.echo(f"⚠️  Skipping `{model}`")
            continue
        else:
            table = materialization.materialize(
                sql=task.sql_stmt, model=task.model
            )
            if table is None:
                continue

            typer.echo(f"✅  Created `{model}` as `{table.full_table_id}`")
            typer.echo(f"    Rows: {table.num_rows}")
            typer.echo(f"    Bytes: {table.num_bytes}")


@app.command()
def test(
    models: Optional[Models] = models_option,
) -> None:
    """
    Runs tests on data in deployed models. Run this after `amora materialize`
    to ensure that the date state is up-to-date.
    """
    return_code = pytest.main(["-n", "auto"])
    raise typer.Exit(return_code)


@app.command(name="list")
def list_models(
    format: str = typer.Option(
        "table",
        help="Output format. Options: json,table",
    ),
    with_total_bytes: bool = typer.Option(
        False,
        help="Uses BigQuery query dry run feature "
        "to gather model total bytes information",
    ),
) -> None:
    """
    List the resources in your project

    """

    @dataclass
    class ResultItem:
        model: AmoraModel
        dry_run_result: Optional[DryRunResult] = None

        def as_dict(self):
            return {
                "depends_on": self.depends_on,
                "has_source": self.has_source,
                "materialization_type": self.materialization_type,
                "model_name": self.model_name,
                "referenced_tables": self.referenced_tables,
                "total_bytes": self.total_bytes,
            }

        @property
        def model_name(self):
            return self.model.__name__

        @property
        def has_source(self):
            return self.model.source() is not None

        @property
        def depends_on(self) -> List[str]:
            return sorted(
                [
                    dependency.__name__
                    for dependency in self.model.dependencies()
                ]
            )

        @property
        def total_bytes(self) -> Optional[int]:
            if self.dry_run_result:
                return self.dry_run_result.total_bytes
            return None

        @property
        def referenced_tables(self) -> List[str]:
            if self.dry_run_result:
                return self.dry_run_result.referenced_tables
            return []

        @property
        def materialization_type(self) -> Optional[str]:
            if self.has_source:
                return self.model.__model_config__.materialized.value
            else:
                return None

    results = []
    for model_file_path in list_model_files():
        model = amora_model_for_path(model_file_path)
        if with_total_bytes:
            result_item = ResultItem(model=model, dry_run_result=dry_run(model))
        else:
            result_item = ResultItem(model=model, dry_run_result=None)

        results.append(result_item)

    if format == "table":
        table = Table(
            show_header=True,
            header_style="bold",
            show_lines=True,
            width=settings.CLI_CONSOLE_MAX_WIDTH,
            row_styles=["none", "dim"],
        )

        table.add_column("Model name", style="green bold", no_wrap=True)
        table.add_column("Total bytes", no_wrap=True)
        table.add_column("Referenced tables", no_wrap=True)
        table.add_column("Depends on", no_wrap=True)
        table.add_column("Has source?", no_wrap=True, justify="center")
        table.add_column("Materialization", no_wrap=True)

        for result in results:
            table.add_row(
                result.model_name,
                f"{result.total_bytes or '-'}",
                " , ".join(result.referenced_tables) or "-",
                " , ".join(result.depends_on) or "-",
                "🟢" if result.has_source else "🔴",
                result.materialization_type or "-",
            )

        console = Console(width=settings.CLI_CONSOLE_MAX_WIDTH)
        console.print(table)

    elif format == "json":
        output = {"models": [result.as_dict() for result in results]}
        typer.echo(json.dumps(output))


@app.command(name="model-create")
def model_create(
    table_reference: str = typer.Option(
        ...,
        "--table-reference",
        help="BigQuery unique table identifier. "
        "E.g.: project-id.dataset-id.table-id",
    ),
    model_name: str = typer.Argument(
        None,
        help="Canonical name of python module for the generated AmoraModel. "
        "A good pattern would be to use an unique "
        "and deterministic identifier, like: `project_id.dataset_id.table_id`",
    ),
    overwrite: bool = typer.Option(
        False, help="Overwrite the output file if one already exists"
    ),
):
    """
    Generates a new amora model file from an existing table/view
    """

    env = Environment(
        loader=PackageLoader("amora"), autoescape=select_autoescape()
    )
    template = env.get_template("new-model.py.jinja2")

    project, dataset, table = table_reference.split(".")

    model_source_code = template.render(
        BIGQUERY_TYPES_TO_PYTHON_TYPES=BIGQUERY_TYPES_TO_PYTHON_TYPES,
        dataset=dataset,
        dataset_id=f"{project}.{dataset}",
        model_name="".join((part.title() for part in table.split("_"))),
        project=project,
        schema=get_schema(table_reference),
        table=table,
    )

    destination_file_path = Path(settings.MODELS_PATH).joinpath(
        model_name.replace(".", "/") + ".py"
    )
    if destination_file_path.exists() and not overwrite:
        typer.echo(
            f"`{destination_file_path}` already exists. "
            f"Pass `--overwrite` to overwrite file.",
            err=True,
        )
        raise typer.Exit(1)

    destination_file_path.parent.mkdir(parents=True, exist_ok=True)
    destination_file_path.write_text(model_source_code)

    # validar que select * FROM modelo funciona
    # validar que pode ser carregado com `amora_model_for_path(destination_file_path)`


def main():
    return app()


if __name__ == "__main__":
    main()
