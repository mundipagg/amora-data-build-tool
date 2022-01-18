import pytest
from google.api_core.exceptions import BadRequest

from amora.providers.bigquery import cte_from_rows
from amora.tests.assertions import is_numeric, that


def test_is_numeric_with_numeric_column():
    cte = cte_from_rows(
        [
            {"numeric_column": "123"},
            {"numeric_column": "234"},
            {"numeric_column": "345"},
        ]
    )

    assert that(cte.c.numeric_column, is_numeric)


def test_is_numeric_with_numeric_column_and_single_row():
    cte = cte_from_rows(
        [
            {"numeric_column": "123"},
        ]
    )

    assert that(cte.c.numeric_column, is_numeric)


def test_is_numeric_with_non_numeric_values_on_column():
    bad_int = "23a4"
    cte = cte_from_rows(
        [
            {"numeric_column": "123"},
            {"numeric_column": bad_int},
            {"numeric_column": "356"},
        ]
    )

    assert not that(cte.c.numeric_column, is_numeric, raise_on_fail=False)
