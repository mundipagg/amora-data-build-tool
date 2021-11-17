import inspect
import json
from unittest.mock import MagicMock, patch

import rich
from google.cloud.bigquery.exceptions import BigQueryError
from typer.testing import CliRunner

from amora.cli import app
from amora.models import list_model_files

runner = CliRunner()


@patch("amora.cli.dry_run", return_value=None)
@patch("amora.cli.Console")
def test_list_without_options(Console: MagicMock, dry_run: MagicMock):
    result = runner.invoke(
        app,
        ["list"],
    )

    assert result.exit_code == 0, result.stderr

    console_print = Console.return_value.print

    console_print.assert_called_once()
    table: rich.table.Table = Console.return_value.print.call_args_list[0][0][0]
    assert table.row_count == len(list(list_model_files()))

    dry_run.assert_not_called()


@patch("amora.cli.dry_run", return_value=None)
def test_list_with_json_format(dry_run: MagicMock):
    result = runner.invoke(
        app,
        ["list", "--format", "json"],
    )

    assert result.exit_code == 0, result.stderr

    models_as_json = json.loads(result.stdout)
    assert models_as_json
    assert len(models_as_json["models"]) == len(list(list_model_files()))

    dry_run.assert_not_called()


@patch("amora.cli.dry_run", return_value=None)
def test_list_with_total_bytes_option(dry_run: MagicMock):
    result = runner.invoke(
        app,
        ["list", "--with-total-bytes"],
    )

    assert result.exit_code == 0, result.stderr

    assert dry_run.call_count == len(list(list_model_files()))


@patch("amora.cli.dry_run", return_value=None)
def test_list_json_format_and_with_total_bytes_option(dry_run: MagicMock):
    result = runner.invoke(
        app,
        ["list", "--format", "json", "--with-total-bytes"],
    )

    assert result.exit_code == 0, result.stderr

    models_as_json = json.loads(result.stdout)
    assert models_as_json
    assert len(models_as_json["models"]) == len(list(list_model_files()))
    assert dry_run.call_count == len(list(list_model_files()))


exc = BigQueryError()


@patch("amora.cli.dry_run", side_effect=exc)
def test_list_with_total_bytes_option_and_dry_run_error(dry_run: MagicMock):
    result = runner.invoke(
        app,
        ["list", "--format", "json", "--with-total-bytes"],
    )

    assert result.exit_code == 1, result.stderr
    assert result.exception == exc
    dry_run.assert_called_once()
