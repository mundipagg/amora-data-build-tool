from typing import Iterable


def not_null(column):
    """
    >>> not_null(HeartRate.id)

    The `id` column in the `HeartRate` model should not contain `null` values

    Results in the following query:

    ```sql
        SELECT *
        FROM {{ model }}
        WHERE {{ column_name }} IS NULL
    ```
    """
    pass


def unique(column):
    """
    >>> unique(HeartRate.id)

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
    pass


def accepted_values(column, values: Iterable):
    """
    >>> accepted_values(HeartRate.source, values=["iPhone", "Mi Band"])

    The `source` column in the `HeartRate` model should be one of
    'iPhone' or 'MiBand'
    """
    pass


def relationships(column, to):
    """
    >>> relationships(HeartRate.id, to=Health.id)

    Each `id` in the `HeartRate` model exists as an `id` in the `Health`
    table (also known as referential integrity)
    """
    pass


def is_non_negative(column):
    """
    >>> is_non_negative(HeartRate.value)

    Each not null `value` in `HeartRate` model is >= 0
    """
    pass