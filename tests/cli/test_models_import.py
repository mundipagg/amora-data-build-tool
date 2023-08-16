from typer.testing import CliRunner

from amora.cli import app

runner = CliRunner()


def test_models_import_with_invalid_table_reference():
    table_reference = "project.dataset.table"

    result = runner.invoke(
        app,
        [
            "models",
            "import",
            "table",
            table_reference,
        ],
    )

    assert result.exit_code == 1, result.stderr


def test_models_import_table():
    table_reference = (
        "amora-data-build-tool.amora.HKQuantityTypeIdentifierHeartRate_diogommachado"
    )

    result = runner.invoke(
        app, ["models", "import", "table", table_reference, "--overwrite"]
    )

    assert result.exit_code == 0
    assert (
        f"🏗 Generating AmoraModel file from table `{table_reference}`" in result.stdout
    )


def test_models_import_dataset():
    dataset_reference = "amora-data-build-tool.amora"

    result = runner.invoke(
        app,
        ["models", "import", "dataset", dataset_reference, "--overwrite"],
    )

    assert result.exit_code == 0
    assert (
        f"🏗 Generating AmoraModel files from dataset `{dataset_reference}`"
        in result.stdout
    )


def test_models_import_project():
    project_id = "amora-data-build-tool"

    result = runner.invoke(
        app, ["models", "import", "project", project_id, "--overwrite"]
    )

    assert result.exit_code == 0
    assert f"🏗 Generating AmoraModel files from project `{project_id}`" in result.stdout
