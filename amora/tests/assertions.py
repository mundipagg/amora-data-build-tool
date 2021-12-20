from typing import Iterable, Optional, Callable

import pytest
from sqlalchemy import (
    and_,
    union_all,
    Integer,
    func,
)
from sqlalchemy.orm import InstrumentedAttribute
from sqlmodel.sql.expression import SelectOfScalar
from amora.models import select, AmoraModel
from amora.types import Compilable
from amora.compilation import compile_statement
from amora.providers.bigquery import get_client

Column = InstrumentedAttribute
Columns = Iterable[Column]
Test = Callable[..., SelectOfScalar]


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
        SELECT {{ column_name }}
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
        SELECT {{ column_name }}
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

    ```sql
        SELECT {{ column_name }}
        FROM {{ model }}
        WHERE {{ column_name }} NOT IN {{ values }}
    ```
    """
    return select(column).where(~column.in_(values))


def relationship(
    from_: Column,
    to: Column,
    from_condition=and_(True),
    to_condition=and_(True),
) -> bool:
    """
    >>> relationship(HeartRate.id, to=Health.id)

    Each `id` in the `HeartRate` model exists as an `id` in the `Health`
    table (also known as referential integrity)

    This test validates the referential integrity between two relations
    with a predicate (`from_condition` and `to_condition`) to filter out
    some rows from the test. This is useful to exclude records such as
    test entities, rows created in the last X minutes/hours to account
    for temporary gaps due to data ingestion limitations, etc.

    ```sql
        WITH left_table AS (
          SELECT
            {{from_column_name}} AS id
          FROM {{from_table}}
          WHERE
            {{from_column_name}} IS NOT NULL
            AND {{from_condition}}
        ),
        right_table AS (
          SELECT
            {{to_column_name}} AS id
          FROM {{to_table}}
          WHERE
            {{to_column_name}} IS NOT NULL
            AND {{to_condition}}
        ),
        exceptions as (
          SELECT
            left_table.id AS {{from_column_name}}}
          FROM
            left_table
          LEFT JOIN
            right_table
            ON left_table.id = right_table.id
          WHERE
            right_table.id IS NULL
        )

        SELECT * FROM exceptions
    ```

    """
    left_table = (
        select(from_.label("id"))
        .where(from_ != None)
        .where(from_condition)
        .cte("left_table")
    )
    right_table = (
        select(to.label("id"))
        .where(to != None)
        .where(to_condition)
        .cte("right_table")
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


def is_numeric(column: Column) -> Compilable:
    """

    :param column:
    :return:
    """
    int_col_or_null = (
        select(func.cast(column, Integer).label("col"))
        .where(column != None)
        .cte("int_col_or_null")
    )

    return select(int_col_or_null.c.col).where(int_col_or_null.c.col == None)


def is_non_negative(column: Column) -> Compilable:
    """
    >>> is_non_negative(HeartRate.value)
    True

    Each not null `value` in `HeartRate` model is >= 0

    ```sql
        SELECT {{ column_name }}
        FROM {{ model }}
        WHERE {{ column_name }} < 0
    ```
    """
    return select(column).where(column < 0)


def expression_is_true(expression, condition=and_(True)) -> bool:
    """
    >>> expression_is_true(StepsAgg._sum > StepsAgg._avg, condition=StepsAgg.year == 2021)

    Asserts that a expression is TRUE for all records.
    This is useful when checking integrity across columns, for example,
    that a total is equal to the sum of its parts, or that at least one column is true.

    Optionally assert `expression` only for rows where `condition` is met.
    ```
    """
    return _test(statement=select(["*"]).where(condition).where(~expression))


def equality(
    model_a: AmoraModel,
    model_b: AmoraModel,
    compare_columns: Optional[Iterable[Column]] = None,
) -> bool:
    """
    This schema test asserts the equality of two models. Optionally specify a subset of columns to compare.

    """

    raise NotImplementedError

    def comparable_columns(model: AmoraModel) -> Iterable[Column]:
        if not compare_columns:
            return model
        return [getattr(model, column_name) for column_name in compare_columns]

    a = select(comparable_columns(model_a)).cte("a")
    b = select(comparable_columns(model_b)).cte("b")

    # fixme: google.api_core.exceptions.BadRequest: 400 EXCEPT must be followed by ALL, DISTINCT, or "(" at [34:4]
    a_minus_b = select(a).except_(select(b))
    b_minus_a = select(b).except_(select(a))

    diff_union = union_all(a_minus_b, b_minus_a)

    return _test(statement=diff_union)


def has_at_least_one_not_null_value(column: Column) -> Compilable:
    """
    Asserts if column has at least one value.

    ```sql

    SELECT
        count({{ column_name }}) as filler_column
    FROM
        {{ model }}
    HAVING
        count({{ column_name }}) = 0
    ```
    """
    return select(func.count(column, type_=Integer)).having(
        func.count(column) == 0
    )


def are_unique_together(columns: Iterable[Column]) -> Compilable:
    """
    This test confirms that the combination of columns is unique.
    For example, the combination of month and product is unique,
    however neither column is unique in isolation.

    """
    return (
        select(columns).group_by(*columns).having(func.count(type_=Integer) > 1)
    )
