from amora.providers.bigquery import cte_from_rows
from amora.tests.assertions import (
    is_numeric,
    that,
    has_at_least_one_not_null_value,
    is_a_non_empty_string,
)


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


def test_has_at_least_one_not_null_value_with_non_null_value():
    cte = cte_from_rows(
        [
            {"col": None},
            {"col": None},
            {"col": "356"},
            {"col": None},
        ]
    )

    assert that(cte.c.col, has_at_least_one_not_null_value)


def test_has_at_least_one_not_null_value_without_non_null_value():
    cte = cte_from_rows(
        [
            {"col": None},
            {"col": None},
            {"col": None},
            {"col": None},
        ]
    )

    assert not that(cte.c.col, has_at_least_one_not_null_value, raise_on_fail=False)


def test_is_a_non_empty_string():
    cte = cte_from_rows(
        [
            {"col": "abc"},
            {"col": "asdasdas  "},
            {"col": "a"},
            {"col": "b"},
        ]
    )

    assert that(cte.c.col, is_a_non_empty_string)


def test_is_a_non_empty_string_with_empty_strings():
    cte = cte_from_rows(
        [
            {"col": "abc"},
            {"col": "asdasdas  "},
            {"col": ""},
            {"col": "      "},
        ]
    )

    assert not that(cte.c.col, is_a_non_empty_string, raise_on_fail=False)
