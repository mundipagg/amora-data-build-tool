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
from amora.providers.bigquery import run

Column = InstrumentedAttribute
Columns = Iterable[Column]
Test = Callable[..., SelectOfScalar]


def _test(statement: Compilable) -> bool:
    run_result = run(statement)

    if run_result.rows.total_rows == 0:
        return True
    else:
        pytest.fail(
            f"{run_result.rows.total_rows} rows failed the test assertion."
            f"\n==========="
            f"\nTest query:"
            f"\n==========="
            f"\n{run_result.query}",
            pytrace=False,
        )


def that(
    column: Column,
    test: Test,
    **test_kwargs,
) -> bool:
    """
    Executes the test, returning `True` if the test is successful and raising a pytest fail otherwise

    Example:

    ```python
    assert that(HeartRate.value, is_not_null)
    ```
    """
    return _test(statement=test(column, **test_kwargs))


def is_not_null(column: Column) -> Compilable:
    """
    Asserts that the `column` does not contain `null` values

    Results in the following query:

    ```sql
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
    ```

    Example:

    ```python
    is_not_null(HeartRate.id)
    ```

    """
    return select(column).where(column == None)


def is_unique(column: Column) -> Compilable:
    """
    Assert that the `column` values are unique

    Example SQL:

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

    Example:

    ```python
    is_unique(HeartRate.id)
    ```
    """
    return select(column).group_by(column).having(func.count(column) > 1)


def has_accepted_values(column: Column, values: Iterable) -> Compilable:
    """
    Assert that the values from the `column` should be one of the provided `values`

    Example SQL:

    ```sql
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} NOT IN {{ values }}
    ```

    Example:

    ```python
    has_accepted_values(HeartRate.source, values=["iPhone", "Mi Band"])
    ```
    """
    return select(column).where(~column.in_(values))


def relationship(
    from_: Column,
    to: Column,
    from_condition=None,
    to_condition=None,
) -> bool:
    """
    Each value of the `from_` column exists as a value in the `to` column.
    Also known as referential integrity.

    This test validates the referential integrity between two relations
    with a predicate (`from_condition` and `to_condition`) to filter out
    some rows from the test. This is useful to exclude records such as
    test entities, rows created in the last X minutes/hours to account
    for temporary gaps due to data ingestion limitations, etc.

    Example SQL:

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

    Example:

    ```python
    relationship(HeartRate.id, to=Health.id)
    ```

    """
    left_table = (
        select(from_.label("id"))
        .where(from_ != None)
        .where(from_condition or and_(True))
        .cte("left_table")
    )
    right_table = (
        select(to.label("id"))
        .where(to != None)
        .where(to_condition or and_(True))
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
    Asserts that each not null value is a number

    Example SQL:

    ```sql
    WITH `int_col_or_null` AS (
        SELECT
            CAST({{ column }}, INT64) AS `col`
        FROM
            {{ model }}
        WHERE
            {{ column }} IS NOT NULL
    )

    SELECT
        col
    FROM
        int_col_or_null
    WHERE
        col IS NULL
    ```

    Example:

    ```python
    is_numeric(func.cast(Health.value, String).label('value_as_str'))
    ```

    """
    int_col_or_null = (
        select(func.cast(column, Integer).label("col"))
        .where(column != None)
        .cte("int_col_or_null")
    )

    return select(int_col_or_null.c.col).where(int_col_or_null.c.col == None)


def is_non_negative(column: Column) -> Compilable:
    """
    Asserts that every column value should be >= 0

    Example SQL:

    ```sql
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} < 0
    ```

    Example:

    ```python
    is_non_negative(HeartRate.value)
    ```
    """
    return select(column).where(column < 0)


def expression_is_true(expression, condition=None) -> bool:
    """
    Asserts that a expression is TRUE for all records.
    This is useful when checking integrity across columns, for example,
    that a total is equal to the sum of its parts, or that at least one column is true.

    Optionally assert `expression` only for rows where `condition` is met.

    Arguments:
        condition (object): A query filter

    Example:

    ```python
    expression_is_true(StepsAgg._sum > StepsAgg._avg, condition=StepsAgg.year == 2021)
    ```

    """
    return _test(
        statement=select(["*"])
        .where(condition or and_(True))
        .where(~expression)
    )


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

    Example SQL:

    ```sql
    SELECT
        count({{ column_name }}) as filler_column
    FROM
        {{ model }}
    HAVING
        count({{ column_name }}) = 0
    ```

    Example:

    ```python
    has_at_least_one_not_null_value(Health.value)
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

    Example:

    ```python
    are_unique_together([HeartRateAgg.year, HeartRateAgg.month])
    ```

    """
    return (
        select(columns).group_by(*columns).having(func.count(type_=Integer) > 1)
    )
