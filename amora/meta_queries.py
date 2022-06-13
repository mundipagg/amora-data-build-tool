import pandas as pd
from sqlalchemy import Float, Integer, Numeric, func, literal

from amora.feature_store.protocols import FeatureViewSourceProtocol
from amora.models import Column, Model, select
from amora.providers.bigquery import run


def summarize(model: Model) -> pd.DataFrame:
    return pd.concat(
        [summarize_column(model, column) for column in model.__table__.columns]
    )


def summarize_column(model: Model, column: Column) -> pd.DataFrame:
    is_numeric = isinstance(column.type, (Numeric, Integer, Float))

    stmt = select(
        func.min(column).label("min"),
        func.max(column).label("max"),
        func.count(column.distinct()).label("unique_count"),
        (func.avg(column) if is_numeric else literal(None)).label("avg"),  # type: ignore
        (func.stddev(column) if is_numeric else literal(None)).label("stddev"),  # type: ignore
        ((literal(100) * func.countif(column == None)) / func.count(column)).label(
            "null_percentage"
        ),
    )
    result = run(stmt)

    df = pd.DataFrame.from_dict({k: [v] for k, v in dict(result.rows.next()).items()})
    df["column_name"] = column.name
    df["column_type"] = str(column.type)

    if isinstance(model, FeatureViewSourceProtocol):
        df["is_fv_feature"] = column.name in (
            c.name for c in model.feature_view_features()
        )
        df["is_fv_entity"] = column.name in (
            c.name for c in model.feature_view_entities()
        )
        df["is_fv_event_timestamp"] = (
            column.name == model.feature_view_event_timestamp().name
        )
    return df
