from datetime import date

import pandas as pd
from numpy import nan
from sqlalchemy import (
    ARRAY,
    Float,
    Integer,
    Numeric,
    String,
    cast,
    func,
    literal,
    union_all,
)
from sqlalchemy_bigquery import STRUCT

from amora.feature_store.protocols import FeatureViewSourceProtocol
from amora.models import Model, select
from amora.providers.bigquery import run
from amora.storage import cache


@cache(suffix=lambda model: f"{model.unique_name()}.{date.today()}")
def summarize(model: Model) -> pd.DataFrame:
    return _summarize_columns(model)


def _summarize_columns(model: Model) -> pd.DataFrame:
    stmts = []

    for column in model.__table__.columns:
        is_supported = not isinstance(column.type, (ARRAY, STRUCT))
        is_numeric = isinstance(column.type, (Numeric, Integer, Float))

        if isinstance(model, FeatureViewSourceProtocol):
            is_fv_feature = column.name in (
                c.name for c in model.feature_view_features()
            )
            is_fv_entity = column.name in (
                c.name for c in model.feature_view_entities()
            )
            is_fv_event_timestamp = (
                column.name == model.feature_view_event_timestamp().name
            )

        else:
            is_fv_feature = False
            is_fv_entity = False
            is_fv_event_timestamp = False

        _min = cast(func.min(column), String) if is_supported else literal(None)
        _max = cast(func.max(column), String) if is_supported else literal(None)
        _unique_count = func.count(column.distinct()) if is_supported else literal(None)
        _avg = cast(func.avg(column), String) if is_numeric else literal(None)
        _stddev = func.stddev(column) if is_numeric else literal(None)
        _null_percentage = (
            func.safe_divide(
                (literal(100) * func.countif(column == None)), func.count(0)
            )
            if is_supported
            else literal(None)
        )

        stmt = select(
            literal(column.name).label("column_name"),
            literal(str(column.type)).label("column_type"),
            _min.label("min"),  # type: ignore
            _max.label("max"),  # type: ignore
            _unique_count.label("unique_count"),  # type: ignore
            _avg.label("avg"),  # type: ignore
            _stddev.label("stddev"),  # type: ignore
            _null_percentage.label("null_percentage"),  # type: ignore
            literal(is_fv_feature).label("is_fv_feature"),
            literal(is_fv_entity).label("is_fv_entity"),
            literal(is_fv_event_timestamp).label("is_fv_event_timestamp"),
        )
        stmts.append(stmt)

    all_stmts = union_all(*stmts)

    result = run(all_stmts)
    df = result.to_dataframe()

    return df.replace({nan: None})
