import sqlparse
from sqlalchemy_bigquery import BigQueryDialect
from sqlalchemy_bigquery.base import BigQueryCompiler

from amora.types import Compilable


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


def compile_statement(statement: Compilable) -> str:
    raw_sql = str(
        statement.compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        )
    )
    formatted_sql = sqlparse.format(raw_sql, reindent=True, indent_columns=True)
    return formatted_sql
