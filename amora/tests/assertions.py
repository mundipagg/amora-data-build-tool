from typing import Iterable

import pytest
from mypy.api import Callable
from sqlalchemy import func, and_, Table
from sqlalchemy.orm import InstrumentedAttribute
from sqlmodel.sql.expression import SelectOfScalar

from amora.models import select, Compilable, AmoraModel
from amora.compilation import compile_statement
from amora.providers.bigquery import get_client

Column = InstrumentedAttribute
Test = Callable[[Column, ...], SelectOfScalar]


def _test(statement: Compilable) -> bool:
    sql_stmt = compile_statement(statement=statement)

    query_job = get_client().query(sql_stmt)
    result = query_job.result()

    if result.total_rows == 0:
        return True
    else:
        pytest.fail(
            f"{result.total_rows} rows failed the test assertion."
            f"\n==========="
            f"\nTest query:"
            f"\n==========="
            f"\n{sql_stmt}",
            pytrace=False,
        )


def that(
    column: Column,
    test: Test,
    **test_kwargs,
) -> bool:
    """
    >>> assert that(HeartRate.value, is_not_null)

    Executes the test, returning True if the test is successful and raising a pytest fail otherwise
    """
    return _test(statement=test(column, **test_kwargs))


def is_not_null(column: Column) -> Compilable:
    """
    >>> is_not_null(HeartRate.id)

    The `id` column in the `HeartRate` model should not contain `null` values

    Results in the following query:

    ```sql
        SELECT *
        FROM {{ model }}
        WHERE {{ column_name }} IS NULL
    ```
    """
    return select(column).where(column == None)


def is_unique(column: Column) -> Compilable:
    """
    >>> is_unique(HeartRate.id)

    The `id` column in the `HeartRate` model should be unique

    ```sql
        SELECT *
        FROM (
            SELECT {{ column_name }}
            FROM {{ model }}
            WHERE {{ column_name }} IS NOT NULL
            GROUP BY {{ column_name }}
            HAVING COUNT(*) > 1
        ) validation_errors
    ```
    """
    return select(column).group_by(column).having(func.count(column) > 1)


def has_accepted_values(column: Column, values: Iterable) -> Compilable:
    """
    >>> has_accepted_values(HeartRate.source, values=["iPhone", "Mi Band"])

    The `source` column in the `HeartRate` model should be one of
    'iPhone' or 'MiBand'
    """
    return select(column).where(~column.in_(values))


def relationship(
    from_: Column, to: Column, from_condition=and_(True), to_condition=and_(True)
) -> Compilable:
    """
    >>> relationship(HeartRate.id, to=Health.id)

    Each `id` in the `HeartRate` model exists as an `id` in the `Health`
    table (also known as referential integrity)

    This test validates the referential integrity between two relations
    with a predicate to filter out some rows from the test. This is
    useful to exclude records such as test entities, rows created in the
    last X minutes/hours to account for temporary gaps due to data
    ingestion limitations, etc.

    """
    left_table = (
        select(from_.label("id"))
        .where(from_ != None)
        .where(from_condition)
        .cte("left_table")
    )
    right_table = (
        select(to.label("id")).where(to != None).where(to_condition).cte("right_table")
    )

    exceptions = (
        select([left_table.c["id"].label(from_.key)])
        .select_from(
            left_table.join(
                right_table,
                onclause=left_table.c["id"] == right_table.c["id"],
                isouter=True,
            )
        )
        .where(right_table.c["id"] == None)
    )

    return _test(statement=exceptions)


def is_non_negative(column: Column) -> Compilable:
    """
    >>> is_non_negative(HeartRate.value)
    True

    Each not null `value` in `HeartRate` model is >= 0

    ```sql
        SELECT *
        FROM {{ model }}
        WHERE {{ column_name }} < 0
    ```
    """
    return select(column.parent.entity).where(column < 0)


def expression_is_true(expression, condition=and_(True)) -> bool:
    """
    >>> expression_is_true(StepsAgg._sum > StepsAgg._avg, condition=StepsAgg.year == 2021)

    Asserts that a expression is True for all records.
    This is useful when checking integrity across columns, for example,
    that a total is equal to the sum of its parts, or that at least one column is true.

    ```
    """
    return _test(statement=select(["*"]).where(condition).where(~expression))
