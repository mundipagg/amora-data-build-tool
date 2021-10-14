import os
import sqlparse

from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
from typing import Union, Any, Optional

from sqlalchemy_bigquery import BigQueryDialect


from amora.config import settings
from amora.models import AmoraModel, Compilable, list_target_files

dialect = BigQueryDialect()


def compile_statement(statement: Compilable) -> str:
    raw_sql = str(
        statement.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    )
    formatted_sql = sqlparse.format(raw_sql, reindent=True, indent_columns=True)
    return formatted_sql


def model_path_for_target_path(path: Path) -> Path:
    return Path(
        str(path)
        .replace(settings.TARGET_PATH, settings.DBT_MODELS_PATH)
        .replace(".sql", ".py"),
    )


def target_path_for_model_path(path: Path) -> Path:
    return Path(
        str(path)
        .replace(settings.DBT_MODELS_PATH, settings.TARGET_PATH)
        .replace(".py", ".sql")
    )


def py_module_for_path(path: Path) -> Any:
    spec = spec_from_file_location(path.stem, path)
    module = module_from_spec(spec)
    # todo: medo dessa execução
    spec.loader.exec_module(module)

    return module


def py_module_for_target_path(path: Path) -> Any:
    model_path = model_path_for_target_path(path)
    return py_module_for_path(model_path)


def clean_compiled_files():
    for sql_file in list_target_files():
        os.remove(sql_file)
