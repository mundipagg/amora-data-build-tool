import pytest
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.selectable import CTE

from amora.compilation import compile_statement
from amora.providers.bigquery import cte_from_rows, estimated_query_cost_in_usd


def test_cte_from_rows_with_single_row():
    cte = cte_from_rows([{"x": 666, "y": 666}])

    assert isinstance(cte, CTE)
    assert cte.c.keys() == ["x", "y"]
    assert compile_statement(cte)


def test_cte_from_rows_with_multiple_rows():
    cte = cte_from_rows([{"x": n, "y": n} for n in range(5)])

    assert isinstance(cte, CTE)
    assert cte.c.keys() == ["x", "y"]
    assert compile_statement(cte)


def test_cte_from_rows_with_distinguished_schema_rows():
    cte = cte_from_rows([{"x": 1, "y": 1}, {"x": 2, "y": 2, "z": 2}])
    assert isinstance(cte, CTE)

    with pytest.raises(CompileError):
        compile_statement(cte)


ONE_TERABYTE = 1 * 1024 ** 4


@pytest.mark.parametrize(
    "total_bytes, expected_cost",
    [
        (0, 0),
        (ONE_TERABYTE, 5.0),
        (ONE_TERABYTE * 10, 50.0),
        (ONE_TERABYTE / 2, 2.5),
    ],
)
def test_estimated_query_cost_in_usd(total_bytes: int, expected_cost: float):
    assert estimated_query_cost_in_usd(total_bytes) == expected_cost
