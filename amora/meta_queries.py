import pandas as pd
from sqlalchemy import Float, Integer, Numeric, func, literal

from amora.models import AmoraModel, Column, select
from amora.providers.bigquery import run


def summarize(model: AmoraModel) -> pd.DataFrame:
    return pd.concat([summarize_column(column) for column in model.__table__.columns])


def summarize_column(column: Column) -> pd.DataFrame:
    is_numeric = isinstance(column.type, (Numeric, Integer, Float))

    stmt = select(
        func.min(column).label("min"),
        func.max(column).label("max"),
        func.count(column.distinct()).label("unique_count"),
        (func.avg(column) if is_numeric else literal(None)).label("avg"),
        (func.stddev(column) if is_numeric else literal(None)).label("stddev"),
        ((literal(100) * func.countif(column == None)) / func.count(column)).label(
            "null_percentage"
        ),
    )
    result = run(stmt)

    df = pd.DataFrame.from_dict({k: [v] for k, v in dict(result.rows.next()).items()})
    df["column_name"] = column.name
    df["column_type"] = str(column.type)
    return df
