from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField
from typer.testing import CliRunner

from amora.cli import app
from amora.config import settings
from amora.models import AmoraModel, amora_model_for_path

runner = CliRunner()


exc = NotFound("Table not found")


@patch("amora.cli.models.get_schema", side_effect=exc)
def test_models_import_with_invalid_table_reference(get_schema: MagicMock):
    table_reference = "project.dataset.table"

    with NamedTemporaryFile(suffix=".py", dir=settings.models_path) as fp:
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


@patch("amora.cli.models.get_schema", return_value=mock_schema)
def test_models_import_with_valid_table_reference_and_existing_destination_file_path(
    get_schema: MagicMock,
):
    with NamedTemporaryFile(suffix=".py", dir=settings.models_path) as fp:
        result = runner.invoke(
            app,
            [
                "models",
                "import",
                "--table-reference",
                "project.dataset.table",
                Path(fp.name).as_posix(),
            ],
        )

        assert result.exit_code == 1
        assert not get_schema.called
        assert not fp.read()
        assert "Pass `--overwrite` to overwrite file." in result.stdout


@patch("amora.cli.models.get_schema", return_value=mock_schema)
def test_models_import_fails_when_destination_file_path_is_an_absolute_unrelated_path(
    get_schema: MagicMock,
):
    result = runner.invoke(
        app,
        [
            "models",
            "import",
            "--overwrite",
            "--table-reference",
            "project.dataset.table",
            "/tmp/absolute-and-unrelated-to-models-path",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    get_schema.assert_not_called()


@patch("amora.cli.models.get_schema", return_value=mock_schema)
def test_models_import_with_valid_table_reference_and_existing_destination_file_path_and_overwrite(
    get_schema: MagicMock,
):
    with NamedTemporaryFile(suffix=".py", dir=settings.MODELS_PATH) as fp:
        table_reference = "project.dataset.table"
        output_path = Path(fp.name)
        result = runner.invoke(
            app,
            [
                "models",
                "import",
                "--overwrite",
                "--table-reference",
                table_reference,
                output_path.as_posix(),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.stderr
        assert issubclass(amora_model_for_path(path=output_path), AmoraModel)  # type: ignore
