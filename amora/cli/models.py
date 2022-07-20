import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import typer
from jinja2 import Environment, PackageLoader, select_autoescape
from rich.console import Console
from rich.table import Table
from rich.text import Text
from shed import shed

from amora.config import settings
from amora.models import Model, list_models
from amora.providers.bigquery import (
    BIGQUERY_TYPES_TO_PYTHON_TYPES,
    DryRunResult,
    dry_run,
    estimated_query_cost_in_usd,
    estimated_storage_cost_in_usd,
    get_schema,
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


@app.command(name="import")
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

    env = Environment(
        loader=PackageLoader("amora"),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
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
    formatted_source_code = shed(model_source_code)

    destination_file_path.parent.mkdir(parents=True, exist_ok=True)
    destination_file_path.write_text(data=formatted_source_code)

    typer.secho(
        f"🎉 Amora Model `{model_name}` (`{table_reference}`) imported!",
        fg=typer.colors.GREEN,
        bold=True,
    )
    typer.secho(f"Current File Path: `{destination_file_path.as_posix()}`")
