from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import patch, MagicMock
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField
from typer.testing import CliRunner
from amora.cli import app
from amora.compilation import amora_model_for_path
from amora.models import AmoraModel

runner = CliRunner()


exc = NotFound("Table not found")


@patch("amora.cli.get_schema", side_effect=exc)
def test_models_import_with_invalid_table_reference(get_schema: MagicMock):
    table_reference = "project.dataset.table"

    with NamedTemporaryFile(suffix=".py") as fp:
        output = Path(fp.name).parent.joinpath(Path(fp.name).stem)
        result = runner.invoke(
            app,
            [
                "models",
                "import",
                "--overwrite",
                "--table-reference",
                table_reference,
                output.as_posix(),
            ],
        )

        assert result.exit_code == 1, result.stderr
        assert result.exception == exc
        assert not fp.read()
        get_schema.assert_called_once_with(table_reference)


mock_schema = [
    SchemaField(name="ARRAY_COLUMN", field_type="ARRAY"),
    SchemaField(name="BIGNUMERIC_COLUMN", field_type="BIGNUMERIC"),
    SchemaField(name="BOOL_COLUMN", field_type="BOOL"),
    SchemaField(name="BOOLEAN_COLUMN", field_type="BOOLEAN"),
    SchemaField(name="BYTES_COLUMN", field_type="BYTES"),
    SchemaField(name="DATE_COLUMN", field_type="DATE"),
    SchemaField(name="DATETIME_COLUMN", field_type="DATETIME"),
    SchemaField(name="FLOAT64_COLUMN", field_type="FLOAT64"),
    SchemaField(name="FLOAT_COLUMN", field_type="FLOAT"),
    SchemaField(name="GEOGRAPHY_COLUMN", field_type="GEOGRAPHY"),
    SchemaField(name="INT64_COLUMN", field_type="INT64"),
    SchemaField(name="INTEGER_COLUMN", field_type="INTEGER"),
    SchemaField(name="JSON_COLUMN", field_type="JSON"),
    SchemaField(name="STRING_COLUMN", field_type="STRING"),
    SchemaField(name="TIME_COLUMN", field_type="TIME"),
    SchemaField(name="TIMESTAMP_COLUMN", field_type="TIMESTAMP"),
]


@patch("amora.cli.get_schema", return_value=mock_schema)
def test_models_import_with_valid_table_reference_and_existing_destination_file_path(
    get_schema: MagicMock,
):
    with NamedTemporaryFile(suffix=".py") as fp:
        output = Path(fp.name).parent.joinpath(Path(fp.name).stem)

        result = runner.invoke(
            app,
            [
                "models",
                "import",
                "--table-reference",
                "project.dataset.table",
                output.as_posix(),
            ],
        )

        assert result.exit_code == 1, result.stderr
        assert not get_schema.called
        assert not fp.read()
        assert "Pass `--overwrite` to overwrite file." in result.stdout


@patch("amora.cli.get_schema", return_value=mock_schema)
def test_models_import_with_valid_table_reference_and_existing_destination_file_path_and_overwrite(
    get_schema: MagicMock,
):
    with NamedTemporaryFile(suffix=".py") as fp:
        output_path = Path(fp.name)
        output = output_path.parent.joinpath(output_path.stem)

        table_reference = "project.dataset.table"

        result = runner.invoke(
            app,
            [
                "models",
                "import",
                "--overwrite",
                "--table-reference",
                table_reference,
                output.as_posix(),
            ],
        )

        assert result.exit_code == 0, result.stderr
        assert output_path.read_text()
        assert issubclass(amora_model_for_path(path=output_path), AmoraModel)
