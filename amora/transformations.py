from sqlalchemy import func, String


def remove_non_numbers(column):
    """
    The column string value with numeric characters only. E.g: "31.752.270/0001-82" -> "31752270000182"
    """
    return func.regexp_replace(column, "[^0-9]", "", type_=String)
