from typing import Iterable

import pytest
from mypy.api import Callable
from sqlalchemy import func
from sqlalchemy.orm import InstrumentedAttribute
from sqlmodel.sql.expression import SelectOfScalar

from amora.models import select, Compilable
from amora.compilation import compile_statement
from amora.providers.bigquery import get_client

Column = InstrumentedAttribute


def that(
    column: Column,
    test: Callable[[Column, ...], SelectOfScalar],
    **test_kwargs,
):
    """
    >>> assert that(HeartRate.value, is_not_null)

    Executes the test, returning True if the test is successful and raising a pytest fail otherwise
    """
    sql_stmt = compile_statement(statement=test(column, **test_kwargs))

    query_job = get_client().query(sql_stmt)
    result = query_job.result()

    if result.total_rows == 0:
        return True
    else:
        pytest.fail(f"{result.total_rows} rows failed the test assertion.")


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
    return select(column.parent.entity).where(column == None)


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
    return select(column.parent.entity).group_by(column).having(func.count(column) > 1)


def has_accepted_values(column: Column, values: Iterable) -> Compilable:
    """
    >>> has_accepted_values(HeartRate.source, values=["iPhone", "Mi Band"])

    The `source` column in the `HeartRate` model should be one of
    'iPhone' or 'MiBand'
    """
    return select(column.parent.entity).where(~column.in_(values))


def relates_to(column: Column, to: Column) -> Compilable:
    """
    >>> relates_to(HeartRate.id, to=Health.id)

    Each `id` in the `HeartRate` model exists as an `id` in the `Health`
    table (also known as referential integrity)
    """
    pass


def is_non_negative(column: Column) -> Compilable:
    """
    >>> is_non_negative(HeartRate.value)

    Each not null `value` in `HeartRate` model is >= 0

    ```sql
        SELECT *
        FROM {{ model }}
        WHERE {{ column_name }} < 0
    ```
    """
    return select(column.parent.entity).where(column < 0)
