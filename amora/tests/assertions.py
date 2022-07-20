import itertools
import json
import os
from typing import Callable, Iterable, Optional

import pytest
from sqlalchemy import ARRAY, Integer, and_, func, literal, or_, union_all
from sqlmodel.sql.expression import SelectOfScalar

from amora.config import settings
from amora.models import AmoraModel, ColumnElement, Session, select
from amora.providers.bigquery import RunResult, estimated_query_cost_in_usd, run
from amora.storage import local_engine
from amora.tests.audit import AuditLog
from amora.types import Compilable

Test = Callable[..., SelectOfScalar]


def _log_result(run_result: RunResult) -> AuditLog:
    with Session(local_engine) as session:
        log = AuditLog(
            bytes_billed=run_result.total_bytes,
            estimated_cost_in_usd=estimated_query_cost_in_usd(run_result.total_bytes),
            execution_time_in_ms=run_result.execution_time_in_ms,
            query=run_result.query,
            referenced_tables=json.dumps(run_result.referenced_tables),
            test_node_id=os.getenv("PYTEST_CURRENT_TEST"),
            test_run_id=settings.TEST_RUN_ID,
            user_email=run_result.user_email,
        )
        session.add(log)
        session.commit()
        return log


def _test(statement: Compilable, raise_on_fail: bool = True) -> bool:
    """
    :param statement: A str with a valid SQL compiled statement
    :param raise_on_fail: By default, the test will raise a pytest Fail exception, with a debug message. Default `True`.
    :return: `True` if the test passed, `False` otherwise
    """
    run_result = run(statement)
    _log_result(run_result)

    if run_result.rows.total_rows == 0:
        return True
    elif raise_on_fail:
        pytest.fail(
            f"{run_result.rows.total_rows} rows failed the test assertion."
            f"\n==========="
            f"\nTest query:"
            f"\n==========="
            f"\n{run_result.query}",
            pytrace=False,
        )
    else:
        return False


def that(
    column: ColumnElement,
    test: Test,
    raise_on_fail: bool = True,
    **test_kwargs,
) -> bool:
    """
    Executes the test, returning `True` if the test is successful and raising a pytest fail otherwise

    Example:

    ```python
    assert that(HeartRate.value, is_not_null)
    ```
    :param column: An AmoraModel column to test
    :param test: The test assertion function
    :param raise_on_fail: By default, the test will raise a pytest Fail exception, with a debug message.  Default `True`.
    :param test_kwargs: Keyword arguments passed to the `test` function

    """
    return _test(statement=test(column, **test_kwargs), raise_on_fail=raise_on_fail)


def is_not_null(column: ColumnElement) -> Compilable:
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


def is_unique(column: ColumnElement) -> Compilable:
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


def has_accepted_values(column: ColumnElement, values: Iterable) -> Compilable:
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
    from_: ColumnElement,
    to: ColumnElement,
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


def is_numeric(column: ColumnElement) -> Compilable:
    """
    Asserts that each not null value is a number

    Example SQL:

    ```sql
        SELECT
            {{ column }}
        FROM
            {{ model }}
        WHERE
            REGEXP_CONTAINS({{ column }}, "[^0-9]")
    ```

    Example:

    ```python
    is_numeric(func.cast(Health.value, String).label("value_as_str"))
    ```
    """
    return select(column).where(func.REGEXP_CONTAINS(column, "[^0-9]"))


def is_non_negative(column: ColumnElement) -> Compilable:
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


def is_a_non_empty_string(column: ColumnElement) -> Compilable:
    """
    Asserts that the column isn't an empty string

    Example SQL:

    ```sql
    SELECT
         {{ column_name }}
    FROM
         {{ model }}
    WHERE
         TRIM({{ column_name }}) = ""
    ```

    Example:

    ```python
    is_a_non_empty_string(Health.source)
    ```
    """
    return select(column).where(func.trim(column) == literal(""))


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
    statement = select(["*"]).where(~expression)

    if condition is not None:
        statement = statement.where(condition)

    return _test(statement)


def equality(
    model_a: AmoraModel,
    model_b: AmoraModel,
    compare_columns: Optional[Iterable[ColumnElement]] = None,
) -> bool:
    """
    This schema test asserts the equality of two models. Optionally specify a subset of columns to compare.

    """

    raise NotImplementedError

    def comparable_columns(model: AmoraModel) -> Iterable[ColumnElement]:
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


def has_at_least_one_not_null_value(column: ColumnElement) -> Compilable:
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
    return select(func.count(column, type_=Integer)).having(func.count(column) == 0)


def are_unique_together(columns: Iterable[ColumnElement]) -> Compilable:
    """
    This test confirms that the combination of columns is unique.
    For example, the combination of month and product is unique,
    however neither column is unique in isolation.

    Example:

    ```python
    are_unique_together([HeartRateAgg.year, HeartRateAgg.month])
    ```

    """
    return select(columns).group_by(*columns).having(func.count(type_=Integer) > 1)


def has_the_same_array_length(columns: Iterable[ColumnElement[ARRAY]]) -> Compilable:
    """
    Asserts that all array columns has the same length

    Example SQL:

    ```sql
    SELECT
        arr_col1,
        arr_col2,
        arr_col3
    FROM
        {{ model }}
    WHERE
        ARRAY_LENGTH(arr_col1) != ARRAY_LENGTH(arr_col2)
        OR ARRAY_LENGTH(arr_col1) != ARRAY_LENGTH(arr_col3)
        OR ARRAY_LENGTH(arr_col2) != ARRAY_LENGTH(arr_col3)
    ```

    Example:

    ```python
    assert that([Model.arr_col1, Model.arr_col2, Model.arr_col3], has_the_same_array_length)
    ```
    """
    conditions = (
        func.array_length(a) != func.array_length(b)
        for (a, b) in itertools.combinations(columns, 2)
    )
    return select(columns).where(or_(*conditions))
