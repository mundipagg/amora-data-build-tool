import sqlparse
from sqlalchemy_bigquery import STRUCT, BigQueryDialect
from sqlalchemy_bigquery.base import BigQueryCompiler

from amora.types import Compilable


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
