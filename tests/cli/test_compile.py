import itertools
from typing import List

import pytest
from amora.utils import clean_compiled_files, list_target_files
from typer.testing import CliRunner

from amora.cli import app


runner = CliRunner()


def setup_function(module):
    clean_compiled_files()


def teardown_function(module):
    clean_compiled_files()


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
    # todo: implementar função clean_target
    # todo: clean_target()
    models_args = [("--model", model) for model in models]
    result = runner.invoke(
        app,
        ["compile", *itertools.chain.from_iterable(models_args)],
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
        ["heart_agg.sql", "heart_rate.sql", "steps.sql"]
    )
