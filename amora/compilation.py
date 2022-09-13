import os
from os.path import exists
from pathlib import Path
from typing import Tuple

import sqlparse
from sqlalchemy_bigquery import STRUCT, BigQueryDialect
from sqlalchemy_bigquery.base import BigQueryCompiler

from amora.config import settings
from amora.models import Model, list_files, list_models
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


def get_models_to_compile(
    previous_manifest: dict, current_manifest: dict
) -> Tuple[Model, Path]:
    models_to_compile: Tuple = []

    delete_removed_models_from_target(
        previous_manifest["all_sources"], current_manifest["all_sources"]
    )

    for model, model_file_path in list_models():
        model_unique_name = model.unique_name()

        if model_unique_name not in previous_manifest["models"]:
            models_to_compile.append((model, model_file_path))
            continue

        model_current_manifest = current_manifest["models"][model_unique_name]
        model_previous_manifest = previous_manifest["models"][model_unique_name]

        if model_current_manifest["size"] != model_previous_manifest["size"] or (
            model_current_manifest["stat"] > model_previous_manifest["stat"]
            and (
                not exists(model.target_path())
                or model_current_manifest["hash"] != model_previous_manifest["hash"]
            )
        ):
            models_to_compile.append((model, model_file_path))

    return models_to_compile
