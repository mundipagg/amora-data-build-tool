import inspect
import os
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Optional, Protocol, runtime_checkable, Iterable

import sqlparse
from amora.config import settings
from amora.models import (
    AmoraModel,
    Compilable,
    list_target_files,
)
from sqlalchemy_bigquery import BigQueryDialect
from sqlalchemy_bigquery.base import BigQueryCompiler


class AmoraBigQueryCompiler(BigQueryCompiler):
    def visit_getitem_binary(self, binary, operator_, **kw):
        left = self.process(binary.left, **kw)
        right = self.process(binary.right, **kw)

        try:
            # Only integer values should be wrapped in OFFSET
            return f"{left}[OFFSET({int(right)})]"
        except ValueError:
            return f"{left}[{right}]"


dialect = BigQueryDialect()
dialect.statement_compiler = AmoraBigQueryCompiler


@runtime_checkable
class CompilableProtocol(Protocol):
    def source(self) -> Compilable:
        ...


def compile_statement(statement: Compilable) -> str:
    raw_sql = str(
        statement.compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        )
    )
    formatted_sql = sqlparse.format(raw_sql, reindent=True, indent_columns=True)
    return formatted_sql


def model_path_for_target_path(path: Path) -> Path:
    return Path(
        str(path)
        .replace(settings.TARGET_PATH, settings.MODELS_PATH)
        .replace(".sql", ".py"),
    )


def target_path_for_model_path(path: Path) -> Path:
    return Path(
        str(path)
        .replace(settings.MODELS_PATH, settings.TARGET_PATH)
        .replace(".py", ".sql")
    )


def amora_model_for_path(path: Path) -> AmoraModel:
    spec = spec_from_file_location(path.stem, path)
    if spec is None:
        raise ValueError(f"Invalid path `{path}`. Not a valid Python file.")

    module = module_from_spec(spec)
    # if module is None:
    #     raise ValueError(f"Invalid path `{path}`")

    if spec.loader is None:
        raise ValueError(f"Invalid path `{path}`. Unable to load module.")

    try:
        spec.loader.exec_module(module)  # type: ignore
    except ImportError as e:
        raise ValueError(
            f"Invalid path `{path}`. Unable to load module."
        ) from e

    compilables = inspect.getmembers(
        module,
        lambda x: isinstance(x, CompilableProtocol)
        and inspect.isclass(x)
        and issubclass(x, AmoraModel),  # type: ignore
    )

    for _name, class_ in compilables:
        try:
            # fixme: Quando carregamos o código em inspect, não existe um arquivo associado,
            #  ou seja, ao iterar sobre as classes de um arquivo, a classe que retornar um TypeError,
            #  é uma classe definida no arquivo
            class_.model_file_path()
        except TypeError:
            return class_

    raise ValueError(f"Invalid path `{path}`")


def amora_model_for_target_path(path: Path) -> AmoraModel:
    model_path = model_path_for_target_path(path)
    return amora_model_for_path(model_path)


def clean_compiled_files():
    for sql_file in list_target_files():
        os.remove(sql_file)
