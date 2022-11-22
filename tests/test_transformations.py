from sqlalchemy import select

from amora.providers.bigquery import cte_from_rows, run
from amora.tests.assertions import is_numeric, that
from amora.transformations import (
    parse_numbers,
    remove_leading_zeros,
    remove_non_numbers,
)


def test_remove_non_numbers():
    cte = cte_from_rows(
        [
            {"col": "a123"},
            {"col": "aaaa"},
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

    assert [tuple(row) for row in run(statement).rows] == [
        ("a123", "123"),
        ("aaaa", ""),
        ("1a23", "123"),
        ("123a", "123"),
        ("123.456.789-45", "12345678945"),
        ("+55(21)123456-78945", "552112345678945"),
        ("31.752.270/0001-82", "31752270000182"),
    ]


def test_remove_leading_zeros():
    cte = cte_from_rows(
        [
            {"col": "00009123372036854775807"},
            {"col": "00009023372036854775807"},
            {"col": "00009033372036854775807"},
            {"col": "00009223372036854775807"},
        ]
    )

    statement = select(
        cte.c.col,
        remove_leading_zeros(cte.c.col).label("col_without_leading_zeros"),
    )

    assert that(statement.cte().c.col_without_leading_zeros, is_numeric)

    assert [tuple(row) for row in run(statement).rows] == [
        (
            "00009123372036854775807",
            "9123372036854775807",
        ),
        (
            "00009023372036854775807",
            "9023372036854775807",
        ),
        (
            "00009033372036854775807",
            "9033372036854775807",
        ),
        (
            "00009223372036854775807",
            "9223372036854775807",
        ),
    ]


def test_parse_numbers():
    cte = cte_from_rows(
        [
            {"col": "aaa"},
            {"col": "1a23"},
            {"col": "123a"},
            {"col": "123.456.789-45"},
            {"col": "+55(21)123456-78945"},
            {"col": "00031.752.270/0001-82"},
        ]
    )

    statement = select(
        cte.c.col,
        parse_numbers(cte.c.col).label("col_with_numbers_only"),
    )

    assert that(statement.cte().c.col_with_numbers_only, is_numeric)

    assert [tuple(row) for row in run(statement).rows] == [
        ("aaa", None),
        ("1a23", "123"),
        ("123a", "123"),
        ("123.456.789-45", "12345678945"),
        ("+55(21)123456-78945", "552112345678945"),
        ("00031.752.270/0001-82", "31752270000182"),
    ]
