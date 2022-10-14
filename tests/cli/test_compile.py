import itertools
from typing import List
from unittest import mock

import pytest
from typer.testing import CliRunner

from amora.cli import app
from amora.config import settings
from amora.utils import clean_compiled_files, list_target_files

runner = CliRunner(mix_stderr=False)


def setup_function(_module):
    clean_compiled_files()
    settings.manifest_path.unlink(missing_ok=True)


def teardown_function(_module):
    clean_compiled_files()
    settings.manifest_path.unlink(missing_ok=True)


@mock.patch("amora.utils.clean_compiled_files")
def test_compile_with_force_clean_compiled_files(clean_compiled_files: mock.MagicMock):
    result = runner.invoke(
        app,
        ["compile", "--force"],
    )

    clean_compiled_files.assert_called_once()


@mock.patch("amora.manifest.load_manifest")
@mock.patch("amora.utils.clean_compiled_files")
def test_compile_call_clean_compiled_files_when_manifest_doesnt_exists(
    clean_compiled_files: mock.MagicMock, load_manifest: mock.MagicMock
):
    load_manifest.return_value = {}

    result = runner.invoke(
        app,
        ["compile"],
    )

    clean_compiled_files.assert_called_once()


@mock.patch("amora.manifest.save_manifest")
def test_compile_call_save_manifest(save_manifest: mock.MagicMock):
    result = runner.invoke(
        app,
        ["compile"],
    )

    save_manifest.assert_called_once()


@mock.patch("amora.manifest.load_manifest")
@mock.patch("amora.manifest.generate_manifest")
@mock.patch("amora.compilation.get_models_to_compile")
@mock.patch("amora.compilation.clean_compiled_files_of_removed_models")
def test_compile_call_clean_compiled_files_of_removed_models(
    clean_compiled_files_of_removed_models: mock.MagicMock,
    get_models_to_compile: mock.MagicMock,
    generate_manifest: mock.MagicMock,
    load_manifest: mock.MagicMock,
):
    current_manifest: dict = {"models": {}}
    previous_manifest: dict = {"models": {"1": "1"}}

    generate_manifest.return_value = current_manifest
    load_manifest.return_value = previous_manifest

    result = runner.invoke(
        app,
        ["compile"],
    )

    clean_compiled_files_of_removed_models.assert_called_once_with(
        previous_manifest["models"].keys(), current_manifest["models"].keys()
    )
    get_models_to_compile.assert_called_once_with(previous_manifest, current_manifest)


@pytest.mark.parametrize(
    "models, expected_exit_code, expected_target_files",
    [
        (["health"], 0, []),
        (["heart_rate", "health", "steps"], 0, ["heart_rate.sql", "steps.sql"]),
        (["modelo_que_nao_existe"], 0, []),
    ],
)
def test_compile_with_models_options(
    models: List[str], expected_exit_code: int, expected_target_files: List[str]
):
    models_args = [("--model", model) for model in models]
    result = runner.invoke(
        app, ["compile", *itertools.chain.from_iterable(models_args)]
    )
    assert result.exit_code == expected_exit_code
    # todo: adicionar verbosidade e testes sobre o output em stdout
    generated_target_files = [path.name for path in list_target_files()]
    assert sorted(generated_target_files) == sorted(expected_target_files)


def test_compile_without_arguments_and_options():
    result = runner.invoke(
        app,
        ["compile"],
    )

    assert result.exit_code == 0

    generated_target_files = [path.name for path in list_target_files()]
    assert sorted(generated_target_files) == sorted(
        [
            "array_repeated_fields.sql",
            "heart_agg.sql",
            "heart_rate.sql",
            "step_count_by_source.sql",
            "steps.sql",
        ]
    )
