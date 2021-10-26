import inspect
import os
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Optional, Protocol, runtime_checkable

import sqlparse
from amora.config import settings
from amora.models import AmoraModel, Compilable, list_target_files
from sqlalchemy_bigquery import BigQueryDialect

dialect = BigQueryDialect()


@runtime_checkable
class CompilableProtocol(Protocol):
    def source(self) -> Compilable:
        ...


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


def py_module_for_path(path: Path) -> Optional[CompilableProtocol]:
    spec = spec_from_file_location(path.stem, path)
    module = module_from_spec(spec)
    # todo: medo dessa execução
    spec.loader.exec_module(module)
    compilables = inspect.getmembers(
        module,
        lambda x: isinstance(x, CompilableProtocol)
        and inspect.isclass(x)
        and issubclass(x, AmoraModel),
    )
    classes = [class_ for _name, class_ in compilables]
    if classes:
        return classes[-1]

    return None


def py_module_for_target_path(path: Path) -> Optional[CompilableProtocol]:
    model_path = model_path_for_target_path(path)
    return py_module_for_path(model_path)


def clean_compiled_files():
    for sql_file in list_target_files():
        os.remove(sql_file)
