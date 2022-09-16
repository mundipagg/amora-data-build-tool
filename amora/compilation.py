import os
from os.path import exists
from pathlib import Path
from typing import Any, Set, Tuple

import sqlparse
from sqlalchemy_bigquery import STRUCT, BigQueryDialect
from sqlalchemy_bigquery.base import BigQueryCompiler

from amora.models import Model, amora_model_from_name_list, list_models
from amora.protocols import Compilable
from amora.utils import get_model_key_from_file, get_target_path_from_model_file


class AmoraBigQueryCompiler(BigQueryCompiler):
    def visit_getitem_binary(self, binary, operator_, **kwargs):
        left = self.process(binary.left, **kwargs)
        right = self.process(binary.right, **kwargs)

        try:
            # Only integer values should be wrapped in OFFSET
            return f"{left}[OFFSET({int(right)})]"
        except ValueError:
            return f"{left}[{right}]"

    def visit_array(self, element, **kwargs) -> str:
        return "ARRAY[%s]" % self.visit_clauselist(element, **kwargs)

    def visit_struct(self, element, **kwargs) -> str:
        clause_list = self.visit_clauselist(element, **kwargs)
        return f"{element.type.get_col_spec()}{clause_list}"

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        text = super().visit_function(func, add_to_result_map=None, **kwargs)
        if hasattr(func, "_with_offset") and func._with_offset is not None:
            text += f" WITH OFFSET AS {func._with_offset}"
        return text

    def render_literal_value(self, value, type_):
        if isinstance(type_, STRUCT):
            values = ",".join(
                self.render_literal_value(v, type_._STRUCT_byname[k])
                for k, v in value.items()
            )
            return f"({values})"
        return super().render_literal_value(value, type_)


dialect = BigQueryDialect()
dialect.statement_compiler = AmoraBigQueryCompiler


def compile_statement(statement: Compilable) -> str:
    raw_sql = str(
        statement.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    )
    formatted_sql = sqlparse.format(raw_sql, reindent=True, indent_columns=True)
    return formatted_sql


def delete_removed_models_from_target(
    previous_sources: list, all_sources: list
) -> None:
    removed_sources = set(previous_sources) - set(all_sources)
    for model_file in removed_sources:
        print(
            f"Removing non existent model: {get_model_key_from_file(model_file)} from target: {model_file}"
        )
        os.remove(get_target_path_from_model_file(model_file))


def split_list_by_element(list_: list, element: Any) -> list:
    return [] if element not in list_ else list_[list_.index(element) + 1 :]


def get_deps_names(current_manifest: dict, model_id_to_compile: str) -> set:
    model_deps_to_compile: list = []

    for _, model_manifest in current_manifest["models"].items():
        model_deps_to_compile.extend(
            split_list_by_element(model_manifest["deps"], model_id_to_compile)
        )

    return set(model_deps_to_compile)


def get_models_to_compile(
    previous_manifest: dict, current_manifest: dict
) -> Set[Tuple[Model, Path]]:
    models_to_compile: Set[Tuple[Model, Path]] = set()
    deps_names_to_compile: set = set()

    delete_removed_models_from_target(
        previous_manifest["all_sources"], current_manifest["all_sources"]
    )

    for model, model_file_path in list_models():
        model_unique_name = model.unique_name()
        compile_model = False

        model_current_manifest = current_manifest["models"][model_unique_name]
        model_previous_manifest = previous_manifest["models"].get(
            model_unique_name
        )  # model could not exist in previous

        compile_model = not model_previous_manifest or (
            model_current_manifest["size"] != model_previous_manifest["size"]
            or model_current_manifest["deps"] != model_previous_manifest["deps"]
            or (
                model_current_manifest["stat"] > model_previous_manifest["stat"]
                and (
                    not exists(model.target_path(model_file_path))
                    or model_current_manifest["hash"] != model_previous_manifest["hash"]
                )
            )
        )

        if compile_model:
            models_to_compile.add((model, model_file_path))
            deps_names_to_compile = deps_names_to_compile.union(
                get_deps_names(current_manifest, model_unique_name)
            )

    deps_to_compile: set[Tuple[Model], Path] = set(
        amora_model_from_name_list(deps_names_to_compile)
    )
    models_to_compile = models_to_compile.union(deps_to_compile)

    return models_to_compile
