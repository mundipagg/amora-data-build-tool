from sqlalchemy import func, literal

from amora.providers.bigquery import array, cte_from_rows
from amora.tests.assertions import (are_unique_together, expression_is_true,
                                    has_accepted_values,
                                    has_at_least_one_not_null_value,
                                    has_the_same_array_length,
                                    is_a_non_empty_string, is_non_negative,
                                    is_not_null, is_numeric, is_unique,
                                    relationship, that)


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


def test_expression_is_true():
    cte = cte_from_rows(
        [
            {"col": 2},
            {"col": 4},
            {"col": 6},
            {"col": 8},
        ]
    )
    assert expression_is_true(func.mod(cte.c.col, 2) == literal(0))


def test_expression_is_true_with_condition():
    cte = cte_from_rows(
        [
            {"col": 7},
            {"col": 20},
            {"col": 40},
            {"col": 60},
            {"col": 80},
        ]
    )
    assert expression_is_true(
        expression=func.mod(cte.c.col, 2) == literal(0),
        condition=cte.c.col > literal(10),
    )


def test_has_accepted_values_with_valid_values():
    cte = cte_from_rows(
        [
            {"col": 0},
            {"col": 1},
            {"col": 0},
            {"col": 1},
            {"col": 0},
        ]
    )
    assert that(cte.c.col, has_accepted_values, values=[0, 1])


def test_has_accepted_values_with_invalid_values():
    cte = cte_from_rows([{"col": n} for n in range(10)])
    assert not that(cte.c.col, has_accepted_values, values=[0, 1], raise_on_fail=False)


def test_is_not_null_with_null_values():
    cte = cte_from_rows([{"col": 1}, {"col": None}, {"col": 3}])
    assert not that(cte.c.col, is_not_null, raise_on_fail=False)


def test_is_not_null_without_null_values():
    cte = cte_from_rows([{"col": n} for n in range(10)])
    assert that(cte.c.col, is_not_null)


def test_is_unique_with_unique_values():
    cte = cte_from_rows([{"col": n} for n in range(10)])
    assert that(cte.c.col, is_unique)


def test_is_unique_without_unique_values():
    cte = cte_from_rows([{"col": 1}, {"col": 2}, {"col": 2}])
    assert not that(cte.c.col, is_unique, raise_on_fail=False)


def test_valid_relationship():
    cte1 = cte_from_rows(
        [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
            {"id": 5},
        ]
    )

    cte2 = cte_from_rows(
        [
            {"col": 1, "fk": 1},
            {"col": 1, "fk": 2},
            {"col": 1, "fk": 3},
            {"col": 1, "fk": 4},
        ]
    )

    assert relationship(from_=cte2.c.fk, to=cte1.c.id)


def test_is_non_negative_with_valid_values():
    cte = cte_from_rows([{"col": n} for n in range(10)])
    assert that(cte.c.col, is_non_negative)


def test_is_non_negative_with_invalid_values():
    cte = cte_from_rows([{"col": n} for n in range(-5, 5)])
    assert not that(cte.c.col, is_non_negative, raise_on_fail=False)


def test_are_unique_together():
    cte = cte_from_rows(
        [
            {"name": "Diogo", "document_number": "123"},
            {"name": "Diogo", "document_number": "321"},
            {"name": "Lorena", "document_number": "456"},
            {"name": "Lorena", "document_number": "654"},
        ]
    )
    assert that([cte.c.name, cte.c.document_number], are_unique_together)


def test_has_the_same_array_length():
    cte = cte_from_rows(
        [
            {
                "arr1": array(["a", "b"]),
                "arr2": array([1, 2]),
                "arr3": array(["1", "2"]),
            },
            {
                "arr1": array(["c", "d"]),
                "arr2": array([3, 4]),
                "arr3": array(["1", "2"]),
            },
        ]
    )

    assert that([cte.c.arr1, cte.c.arr2, cte.c.arr3], has_the_same_array_length)
