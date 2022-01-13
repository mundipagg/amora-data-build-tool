from sqlalchemy import func, String
from amora.models import Column
from sqlalchemy.sql.functions import Function


def remove_non_numbers(column: Column) -> Function:
    """
    The column string value with numeric characters only.

     E.g: "31.752.270/0001-82" -> "31752270000182"
    """
    return func.regexp_replace(column, "[^0-9]", "", type_=String)


def remove_leading_zeros(column: Column) -> Function:
    """
    The column string value without leading zeros.

     E.g: "00001000000" -> "1000000"
    """
    return func.regexp_replace(column, "^0+", "", type_=String)


def parse_numbers(column: Column) -> Function:
    """
    Parses a string column as a number, returning NULL if value contains 0 numbers

    E.g: "0031.752.270/0001-82" -> "31752270000182"
         "IM_A_STRING_WITH_NO_NUMBERS" -> NULL
    """
    return func.nullif(
        remove_leading_zeros(remove_non_numbers(column)), "", type_=String
    )
