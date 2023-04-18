import sqlparse


def format_sql(sql: str) -> str:
    """
    Formats the SQL string for human readability

    Args:
        sql: Raw SQL to be formatted

    Returns: The formatted SQL statement as string.
    """
    return sqlparse.format(sql, reindent=True, indent_columns=True)
