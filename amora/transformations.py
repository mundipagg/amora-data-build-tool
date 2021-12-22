from sqlalchemy import func, String, Column
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
