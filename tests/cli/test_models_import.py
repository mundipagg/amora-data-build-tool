from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField
from typer.testing import CliRunner

from amora.cli import app
from amora.models import AmoraModel, amora_model_for_path

runner = CliRunner()


exc = NotFound("Table not found")


@patch("amora.providers.bigquery.get_schema", side_effect=exc)
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
    SchemaField(name="BOOLEAN_COLUMN", field_type="BOOLEAN"),
    SchemaField(name="BOOL_COLUMN", field_type="BOOL"),
    SchemaField(name="BYNARY_COLUMN", field_type="BYNARY"),
    SchemaField(name="DATE_COLUMN", field_type="DATE"),
    SchemaField(name="DATETIME_COLUMN", field_type="DATETIME"),
    SchemaField(name="FLOAT_COLUMN", field_type="FLOAT"),
    SchemaField(name="INTEGER_COLUMN", field_type="INTEGER"),
    SchemaField(name="JSON_COLUMN", field_type="JSON"),
    SchemaField(name="STRING_COLUMN", field_type="STRING"),
    SchemaField(name="TIME_COLUMN", field_type="TIME"),
    SchemaField(name="TIMESTAMP_COLUMN", field_type="TIMESTAMP"),
    SchemaField(name="INTEGER_ARRAY_COLUMN", field_type="INTEGER", mode="REPEATED"),
    SchemaField(name="STRING_ARRAY_COLUMN", field_type="STRING", mode="REPEATED"),
    SchemaField(name="FLOAT_ARRAY_COLUMN", field_type="FLOAT64", mode="REPEATED"),
    SchemaField(
        name="ARRAY_OF_STRUCTS_COLUMN",
        field_type="RECORD",
        mode="REPEATED",
        fields=(
            SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="x", field_type="INTEGER", mode="NULLABLE"),
            SchemaField(name="y", field_type="INTEGER", mode="NULLABLE"),
        ),
    ),
    SchemaField(
        name="STRUCT_COLUMN",
        field_type="RECORD",
        fields=(
            SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="x", field_type="INTEGER", mode="NULLABLE"),
            SchemaField(name="y", field_type="INTEGER", mode="NULLABLE"),
        ),
    ),
]


@patch("amora.providers.bigquery.get_schema", return_value=mock_schema)
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


@patch("amora.providers.bigquery.get_schema", return_value=mock_schema)
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
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.stderr
        assert output_path.read_text()
        import sys

        sys.path.insert(0, output_path.parent.as_posix())
        assert issubclass(amora_model_for_path(path=output_path), AmoraModel)  # type: ignore
