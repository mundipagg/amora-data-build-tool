from datetime import date

import pandas as pd
from numpy import nan
from sqlalchemy import ARRAY, Float, Integer, Numeric, String, cast, func, literal

from amora.feature_store.protocols import FeatureViewSourceProtocol
from amora.models import Column, Model, select
from amora.providers.bigquery import run
from amora.storage import cache


@cache(suffix=lambda model: f"{model.unique_name}.{date.today()}")
def summarize(model: Model) -> pd.DataFrame:
    return pd.concat(
        [summarize_column(model, column) for column in model.__table__.columns]
    )


@cache(suffix=lambda model, column: f"{model.unique_name}.{column.name}.{date.today()}")
def summarize_column(model: Model, column: Column) -> pd.DataFrame:
    df = pd.DataFrame(
        columns=[
            "column_name",
            "column_type",
            "min",
            "max",
            "avg",
            "unique_count",
            "null_percentage",
            "stddev",
        ]
    )

    is_array = isinstance(column.type, ARRAY)
    if is_array:
        # fixme:
        return pd.DataFrame()

    is_numeric = isinstance(column.type, (Numeric, Integer, Float))

    stmt = select(
        cast(func.min(column), String).label("min"),
        cast(func.max(column), String).label("max"),
        func.count(column.distinct()).label("unique_count"),
        (cast(func.avg(column), String) if is_numeric else literal(None)).label("avg"),  # type: ignore
        (func.stddev(column) if is_numeric else literal(None)).label("stddev"),  # type: ignore
        func.safe_divide(
            (literal(100) * func.countif(column == None)), func.count(column)
        ).label("null_percentage"),
    )
    result = run(stmt)

    df = df.append(result.to_dataframe(), ignore_index=True)
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
    else:
        feature_view_columns = [
            "is_fv_feature",
            "is_fv_entity",
            "is_fv_event_timestamp",
        ]
        df[feature_view_columns] = False

    return df.replace({nan: None})
