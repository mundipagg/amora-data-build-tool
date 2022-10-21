import numpy
import pandas as pd
from dash import dash_table, html

from amora.models import Model
from amora.providers.bigquery import sample


def _adapt_dataframe_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma object em strings, porque dash_table.DataTable suporta apenas os tipos string, number e boolean
    """
    for col in df.columns:
        if df[col].dtype == numpy.dtype("O"):
            df[col] = df[col].astype(str)

    return df


def component(model: Model) -> dash_table.DataTable:
    try:
        df = sample(model, percentage=1)
    except ValueError:
        return html.Div(f"Sample not implemented for model {model.unique_name()}")

    df = _adapt_dataframe_values(df)

    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": col, "id": col} for col in sorted(df.columns.values)],
        export_format="csv",
        sort_action="native",
    )
