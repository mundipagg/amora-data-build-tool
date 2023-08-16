import json
from dataclasses import dataclass
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.text import Text

from amora.config import settings
from amora.models import Model, list_models
from amora.providers import bigquery
from amora.providers.bigquery import (
    DryRunResult,
    dry_run,
    estimated_query_cost_in_usd,
    estimated_storage_cost_in_usd,
)

app = typer.Typer(help="List or import Amora Models")


@app.command(name="list")
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
            return sorted(dependency.name for dependency in self.model.dependencies())

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

            return None

    results = []
    placeholder = "-"

    for model, _model_file_path in list_models():
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


models_import = typer.Typer(help="Import models")
app.add_typer(models_import, name="import")


@models_import.command("table", help="Generate an AmoraModel file from a table")
def models_import_table(
    table_reference: str = typer.Argument(
        None,
        help="BigQuery unique table identifier. "
        "E.g.: `amora-data-build-tool.amora.health`",
    ),
    overwrite: bool = typer.Option(
        False, help="Overwrite the output file if one already exists"
    ),
):
    """
    Generates an `AmoraModel` file from a table in BigQuery. Imports the table metadata and generates the
    `AmoraModel` file using the `bigquery.import_table()` method.

    Args:
        table_reference (str): Represents the unique identifier of the BigQuery table from which to
        generate the AmoraModel file. The format for table_reference is `project_id.dataset_id.table_id`.
        overwrite (bool): Determines whether or not to overwrite the output file if it already exists.
        he default value is `False`. If `True`, the function will overwrite the output file.

    Examples:
        ```shell
        amora models import table "amora-data-build-tool.amora.health"
        ```
    """
    typer.echo(f"🏗 Generating AmoraModel file from table `{table_reference}`")
    bigquery.import_table(table_reference, overwrite)


@models_import.command(
    "dataset", help="Generate AmoraModel files for dataset contents."
)
def models_import_dataset(
    dataset_reference: str = typer.Argument(
        None,
        help="BigQuery unique dataset identifier. "
        "E.g.: `amora-data-build-tool.amora`",
    ),
    overwrite: bool = typer.Option(
        False, help="Overwrite the output file if one already exists"
    ),
):
    """
    Generates `AmoraModel` files for a dataset in BigQuery. Imports the table metadata for all tables
    in the dataset and generates `AmoraModel` files using the `bigquery.import_table()` method.

    Args:
        dataset_reference (str): Represents the unique identifier of the BigQuery dataset for which
        to generate AmoraModel files. The format for dataset_reference is `project_id.dataset_id`.
        overwrite (bool): Determines whether or not to overwrite the output file if it already exists.
        The default value is `False`. If `True`, the function will overwrite the output file.

    Examples:
        ```shell
        amora models import dataset "amora-data-build-tool.amora"
        ```
    """
    typer.echo(f"🏗 Generating AmoraModel files from dataset `{dataset_reference}`")
    for table in bigquery.list_tables(dataset_reference):
        bigquery.import_table(table, overwrite)


@models_import.command(
    "project", help="Generate AmoraModel files for the project contents."
)
def models_import_project(
    project_id: str = typer.Argument(
        None,
        help="BigQuery project id." "E.g.: `amora-data-build-tool`",
    ),
    overwrite: bool = typer.Option(
        False, help="Overwrite the output file if one already exists"
    ),
):
    """
    Generates `AmoraModel` files for the contents of a Google Cloud project. Iterates through all the
    datasets and tables in the project and generates an `AmoraModel` file for each table.

    Args:
        project_id (str): The ID of the Google Cloud project that the BigQuery dataset belongs to.
            The format for project_id is `project_id`. For example: `amora-data-build-tool`.
        overwrite (bool): Determines whether to overwrite the output file if it already exists.
            The default value is `False`. If `True`, the function will overwrite the output file.

    Examples:
        ```shell
        amora models import project "amora-data-build-tool"
        ```
    """
    typer.echo(f"🏗 Generating AmoraModel files from project `{project_id}`")
    for dataset in bigquery.list_datasets(project_id):
        for table in bigquery.list_tables(dataset):
            bigquery.import_table(table, overwrite)
