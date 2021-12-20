from amora.compilation import compile_statement
from amora.models import select
from amora.providers.bigquery import cte_from_rows, get_client
from amora.tests.assertions import is_numeric, that
from amora.transformations import remove_non_numbers


def test_remove_non_numbers():
    cte = cte_from_rows(
        [
            {"col": "a123"},
            {"col": "1a23"},
            {"col": "123a"},
            {"col": "123.456.789-45"},
            {"col": "+55(21)123456-78945"},
            {"col": "31.752.270/0001-82"},
        ]
    )

    statement = select(
        cte.c.col,
        remove_non_numbers(cte.c.col).label("col_with_numbers_only"),
    )

    assert that(statement.cte().c.col_with_numbers_only, is_numeric)

    sql = compile_statement(statement)
    query_job = get_client().query(sql)
    result = query_job.result()

    assert [tuple(row) for row in result] == [
        ("a123", "123"),
        ("1a23", "123"),
        ("123a", "123"),
        ("123.456.789-45", "12345678945"),
        ("+55(21)123456-78945", "552112345678945"),
        ("31.752.270/0001-82", "31752270000182"),
    ]
