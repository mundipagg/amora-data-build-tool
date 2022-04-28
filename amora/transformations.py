from sqlalchemy import Date, DateTime, String, Time, func
from sqlalchemy.sql.functions import Function

from amora.models import ColumnElement
from amora.providers.bigquery import TimePart


def remove_non_numbers(column: ColumnElement) -> Function:
    """
    The column string value with numeric characters only.

     E.g: "31.752.270/0001-82" -> "31752270000182"
    """
    return func.regexp_replace(column, "[^0-9]", "", type_=String)


def remove_leading_zeros(column: ColumnElement) -> Function:
    """
    The column string value without leading zeros.

     E.g: "00001000000" -> "1000000"
    """
    return func.regexp_replace(column, "^0+", "", type_=String)


def parse_numbers(column: ColumnElement) -> Function:
    """
    Parses a string column as a number, returning NULL if value contains 0 numbers

    E.g: "0031.752.270/0001-82" -> "31752270000182"
         "IM_A_STRING_WITH_NO_NUMBERS" -> NULL
    """
    return func.nullif(
        remove_leading_zeros(remove_non_numbers(column)), "", type_=String
    )


def datetime_trunc_hour(column: ColumnElement) -> Function:
    """
    Truncate a datetime column by its HOUR part.
    "2019-12-09T16:15:20" -> "2019-12-09T16:00:00"

    E.g.: `datetime_trunc_hour(Steps.creationDate)`

    ```sql
    DATETIME(
        DATE(`steps`.`creationDate`),
        TIME_TRUNC(TIME(`steps`.`creationDate`), HOUR)
    )
    ```
    """
    date_part = func.date(column, type_=Date)
    hour_part = func.time_trunc(func.time(column), TimePart.HOUR.value, type_=Time)
    return func.datetime(date_part, hour_part, type_=DateTime)
