import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pytest
import typer
from jinja2 import Environment, PackageLoader, select_autoescape
from rich.console import Console
from rich.table import Table
from rich.text import Text

from amora import materialization
from amora.compilation import compile_statement
from amora.config import settings
from amora.dag import DependencyDAG
from amora.models import Model, list_models
from amora.providers.bigquery import (
    BIGQUERY_TYPES_TO_PYTHON_TYPES,
    DryRunResult,
    dry_run,
    estimated_query_cost_in_usd,
    estimated_storage_cost_in_usd,
    get_schema,
)
from amora.utils import list_target_files

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

    model_to_task = {}

    for target_file_path in list_target_files():
        if models and target_file_path.stem not in models:
            continue

        task = materialization.Task.for_target(target_file_path)
        model_to_task[task.model.unique_name] = task

    dag = DependencyDAG.from_tasks(tasks=model_to_task.values())

    if draw_dag:
        dag.draw()

    for model in dag:
        try:
            task = model_to_task[model]
        except KeyError:
            typer.echo(f"⚠️  Skipping `{model}`")
            continue
        else:
            table = materialization.materialize(sql=task.sql_stmt, model=task.model)
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
    return_code = pytest.main(["-n", "auto", "--verbose"])
    raise typer.Exit(return_code)


models = typer.Typer()
app.add_typer(models, name="models")


@models.command(name="list")
def models_list(
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
    List the models in your project as a human readable table
    or as a JSON serialized document

    ```shell
    amora models list
    ```
    You can also use the option `--with-total-bytes` to use
    BigQuery query dry run feature to gather model total bytes information

    ```shell
    amora models list --with-total-bytes
    ```

    """

    @dataclass
    class ResultItem:
        model: Model
        dry_run_result: Optional[DryRunResult] = None

        def as_dict(self):
            return {
                "depends_on": self.depends_on,
                "has_source": self.has_source,
                "materialization_type": self.materialization_type,
                "model_name": self.model_name,
                "referenced_tables": self.referenced_tables,
                "total_bytes": self.total_bytes,
                "estimated_query_cost_in_usd": self.estimated_query_cost_in_usd,
                "estimated_storage_cost_in_usd": self.estimated_storage_cost_in_usd,
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
                dependency.__name__ for dependency in self.model.dependencies()
            )

        @property
        def estimated_query_cost_in_usd(self) -> Optional[str]:
            if self.dry_run_result:
                cost = estimated_query_cost_in_usd(self.dry_run_result.total_bytes)
                return f"{cost:.{settings.MONEY_DECIMAL_PLACES}f}"
            return None

        @property
        def estimated_storage_cost_in_usd(self) -> Optional[str]:
            if self.dry_run_result:
                cost = estimated_storage_cost_in_usd(self.dry_run_result.total_bytes)
                return f"{cost:.{settings.MONEY_DECIMAL_PLACES}f}"
            return None

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
    placeholder = "-"

    for model, model_file_path in list_models():
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
        table.add_column("Estimated query cost", no_wrap=True)
        table.add_column("Estimated storage cost", no_wrap=True)
        table.add_column("Referenced tables")
        table.add_column("Depends on")
        table.add_column("Has source?", no_wrap=True, justify="center")
        table.add_column("Materialization", no_wrap=True)

        for result in results:
            table.add_row(
                result.model_name,
                f"{result.total_bytes or placeholder}",
                result.estimated_query_cost_in_usd or placeholder,
                result.estimated_storage_cost_in_usd or placeholder,
                Text(
                    "\n".join(result.referenced_tables) or placeholder,
                    overflow="fold",
                ),
                Text("\n".join(result.depends_on) or placeholder, overflow="fold"),
                "🟢" if result.has_source else "🔴",
                result.materialization_type or placeholder,
            )

        console = Console(width=settings.CLI_CONSOLE_MAX_WIDTH)
        console.print(table)

    elif format == "json":
        output = {"models": [result.as_dict() for result in results]}
        typer.echo(json.dumps(output))


@models.command(name="import")
def models_import(
    table_reference: str = typer.Option(
        ...,
        "--table-reference",
        help="BigQuery unique table identifier. "
        "E.g.: project-id.dataset-id.table-id",
    ),
    model_file_path: str = typer.Argument(
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

    ```shell
    amora models import --table-reference my_gcp_project.my_dataset.my_table my_gcp_project/my_dataset/my_table
    ```
    """

    env = Environment(loader=PackageLoader("amora"), autoescape=select_autoescape())
    template = env.get_template("new-model.py.jinja2")

    project, dataset, table = table_reference.split(".")
    model_name = "".join(part.title() for part in table.split("_"))

    destination_file_path = Path(settings.MODELS_PATH).joinpath(
        (model_file_path or model_name.replace(".", "/")) + ".py"
    )

    if destination_file_path.exists() and not overwrite:
        typer.echo(
            f"`{destination_file_path}` already exists. "
            f"Pass `--overwrite` to overwrite file.",
            err=True,
        )
        raise typer.Exit(1)

    model_source_code = template.render(
        BIGQUERY_TYPES_TO_PYTHON_TYPES=BIGQUERY_TYPES_TO_PYTHON_TYPES,
        dataset=dataset,
        dataset_id=f"{project}.{dataset}",
        model_name=model_name,
        project=project,
        schema=get_schema(table_reference),
        table=table,
    )

    destination_file_path.parent.mkdir(parents=True, exist_ok=True)
    destination_file_path.write_text(model_source_code)

    typer.secho(
        f"🎉 Amora Model `{model_name}` (`{table_reference}`) imported!",
        fg=typer.colors.GREEN,
        bold=True,
    )
    typer.secho(f"Current File Path: `{destination_file_path.as_posix()}`")


feature_store = typer.Typer()
app.add_typer(feature_store, name="feature-store")


@feature_store.command(name="plan")
def feature_store_plan():
    """
    Dry-run registering objects to the Feature Registry

    The plan method dry-runs registering one or more definitions (e.g.: Entity, Feature View)
    and produces a list of all the changes that would be introduced in the Feature Registry
    by an `amora feature-store apply` execution.

    The changes computed by the `plan` command are informational, and are not actually applied to the registry.
    """
    from amora.feature_store import fs
    from amora.feature_store.registry import get_repo_contents

    registry_diff, infra_diff, infra = fs._plan(
        desired_repo_contents=get_repo_contents()
    )

    typer.echo("Amora: Feature Store :: Registry diff")
    typer.echo(registry_diff.to_string())

    typer.echo("Amora: Feature Store :: Infrastructure diff")
    typer.echo(infra_diff.to_string())


#
# @feature_store.command(name="list")
# def feature_store_list():
#     """
#     Lists all Amora Feature Views with details about the last materialization, stored
#     data both on Online Storage and Offline Storage
#     """
#     from feast.cli import feature_view_list
#     from amora.feature_store import fs
#     from amora.feature_store.registry import get_repo_contents
#
#     for feature_view in get_repo_contents().feature_views:
#         typer.echo(feature_view)
#         # todo: exibir tabela


@feature_store.command(name="apply")
def feature_store_apply():
    """
    1. Scans Python files in your amora project and find all models defined as
    feature views.

    2. Validate your feature definitions

    3. Sync the metadata about feature store objects to the feature registry.
    If a registry does not exist, then it will be instantiated.
    The standard registry is a simple protobuf binary file
    that is stored on disk (locally or in an object store).

    4. Create all necessary feature store infrastructure.
    The exact infrastructure that is deployed or configured depends
    on the provider configuration. For example, setting local as
    your provider will result in a sqlite online store being created.
    """
    from feast.repo_operations import apply_total_with_repo_instance

    from amora.feature_store import fs
    from amora.feature_store.registry import get_repo_contents

    apply_total_with_repo_instance(
        store=fs,
        project=fs.project,
        registry=fs.registry,
        repo=get_repo_contents(),
        skip_source_validation=False,
    )


@feature_store.command(name="materialize")
def feature_store_materialize(
    start_ts: str = typer.Argument(
        None,
        help="Start timestamp on ISO 8601 format. E.g.: '2022-01-01T01:00:00'",
    ),
    end_ts: str = typer.Argument(
        None,
        help="End timestamp on ISO 8601 format. E.g.: '2022-01-02T01:00:00'",
    ),
    models: Optional[Models] = models_option,
):
    """
    Run a (non-incremental) materialization job to ingest data into the online
    store. All data between `start_ts` and `end_ts` will be read from the offline
    store and written into the online store. If you don't specify feature view
    names using `--models`, all registered Feature Views will be materialized.
    """
    from amora.feature_store import fs
    from amora.feature_store.registry import get_repo_contents

    repo_contents = get_repo_contents()

    if models:
        views_to_materialize = [
            fv.name for fv in repo_contents.feature_views if fv.name in models
        ]
    else:
        views_to_materialize = [fv.name for fv in repo_contents.feature_views]

    fs.materialize(
        feature_views=views_to_materialize,
        start_date=datetime.fromisoformat(start_ts),
        end_date=datetime.fromisoformat(end_ts),
    )


@feature_store.command(name="serve")
def feature_store_serve():
    """
    Starts the feature server HTTP app.

    Routes:

        - `POST /get-online-features`

        `curl -XPOST -H "Content-type: application/json" -d '{"features": ["step_count_by_source:value_avg", "step_count_by_source:value_sum", "step_count_by_source:value_count"], "entities": {"source_name": ["Mi Fit", "Diogo iPhone", "An invalid source"]}}' 'http://localhost:8666/get-online-features'`

        ```json
        {
          "metadata": {
            "feature_names": [
              "source_name",
              "value_count",
              "value_sum",
              "value_avg"
            ]
          },
          "results": [
            {
              "values": [
                "Mi Fit",
                6.0,
                809.0,
                134.8333282470703
              ],
              "statuses": [
                "PRESENT",
                "PRESENT",
                "PRESENT",
                "PRESENT"
              ],
              "event_timestamps": [
                "1970-01-01T00:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z"
              ]
            },
            {
              "values": [
                "Diogo iPhone",
                2.0,
                17.0,
                8.5
              ],
              "statuses": [
                "PRESENT",
                "PRESENT",
                "PRESENT",
                "PRESENT"
              ],
              "event_timestamps": [
                "1970-01-01T00:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z"
              ]
            },
            {
              "values": [
                "An invalid source",
                null,
                null,
                null
              ],
              "statuses": [
                "PRESENT",
                "NOT_FOUND",
                "NOT_FOUND",
                "NOT_FOUND"
              ],
              "event_timestamps": [
                "1970-01-01T00:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z"
              ]
            }
          ]
        }
        ```

        More on: https://docs.feast.dev/v/v0.9-branch/user-guide/getting-online-features

        - `GET /list-feature-views`. E.g.:

        `curl http://localhost:8666/list-feature-views | jq`

        ```json
        [
            {
                "name": "step_count_by_source",
                "features": [
                    "step_count_by_source:value_avg",
                    "step_count_by_source:value_sum",
                    "step_count_by_source:value_count"
                ],
                "entities": [
                    "source_name"
                ]
            }
        ]
        ```
    """
    import uvicorn
    from feast.feature_server import get_app

    from amora.feature_store import fs
    from amora.feature_store.config import settings

    app = get_app(store=fs)

    @app.get("/list-feature-views")
    def list_feature_views():
        fvs = fs.list_feature_views()
        return [
            {
                "name": fv.name,
                "features": [f"{fv.name}:{feature.name}" for feature in fv.features],
                "entities": fv.entities,
            }
            for fv in fvs
        ]

    uvicorn.run(
        app,
        host=settings.HTTP_SERVER_HOST,
        port=settings.HTTP_SERVER_PORT,
        access_log=settings.HTTP_ACCESS_LOG_ENABLED,
    )


def main():
    return app()


if __name__ == "__main__":
    main()
