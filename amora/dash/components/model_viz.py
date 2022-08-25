from typing import Callable, List, Any, Optional

import pandas as pd

from numerize.numerize import numerize

from pandas.api.types import (
    is_string_dtype,
    is_numeric_dtype,
    is_datetime64_any_dtype,
    is_bool_dtype,
)

import plotly.express as px
from plotly.graph_objects import Figure

from dash import html, dcc
from dash.development.base_component import Component
import dash_bootstrap_components as dbc

from amora.models import Model
from amora.providers.bigquery import sample


def figure_apply_default_layout(figure: Figure) -> Figure:
    figure.update_layout(
        showlegend=False,
        bargap=0,
        margin=dict(l=0, r=0, b=0, t=0),
        yaxis_title=None,
        xaxis_title=None,
        width=200,
        height=200,
    )

    figure.update_yaxes(visible=False, showticklabels=False)

    return figure


def value_counts_series_to_dataframe(series: pd.Series) -> pd.DataFrame:
    df_value_counts = series.to_frame().reset_index()
    df_value_counts.set_axis(["x", "count"], axis=1, inplace=True)
    return df_value_counts


def cut_formatter(cut: pd.Series, formatter_function: Callable) -> list[str]:
    """
    Apply a formatter function in both `left` and `right` Interval.
    Sometimes the pd.cuts generates long Interval strings, and we may
    want to format each part of it. 
    """
    humanized_cut = []
    for _, interval in cut.iteritems():
        humanized_cut.append(
            f"{formatter_function(interval.left)}-{formatter_function(interval.right)}"
        )

    return humanized_cut


def get_binned_dataframe(
    series: pd.Series,
    formatter_function: Optional[Callable] = None,
) -> pd.DataFrame:
    bins = min(series.nunique(), 10)
    cut = pd.cut(x=series, bins=bins, include_lowest=True, precision=0)

    if formatter_function:
        cut = cut_formatter(cut, formatter_function)

    frequencies = pd.value_counts(cut, sort=False)

    df_frequencies = frequencies.to_frame().reset_index()

    df_frequencies.set_axis(["x", "count"], axis=1, inplace=True)

    df_frequencies["x"] = df_frequencies["x"].astype(str)

    return df_frequencies


def datetime_series_aggregation(series: pd.Series) -> pd.Series:
    days_diff = (series.max() - series.min()).days

    if days_diff > 365:
        agg_type = "Y"
    elif days_diff > 31:
        agg_type = "M"
    else:
        agg_type = "D"

    return series.dt.to_period(agg_type).astype(str)


def create_component_for_one_unique_value(series: pd.Series) -> html.P:

    return html.P(f"One unique value: {series[0]}", style={"color": "red"})


def get_most_common_values(
    df_value_counts_normalized: pd.DataFrame,
) -> List[html.P]:

    elements = []

    for _, row in df_value_counts_normalized.head(2).iterrows():
        elements.append(html.P(f"{row['x']}        {round((row['count']*100), 2)}%"))

    if df_value_counts_normalized.shape[0] > 2:
        other = round(
            (
                1
                - (
                    df_value_counts_normalized["count"][0]
                    + df_value_counts_normalized["count"][1]
                )
            )
            * 100,
            2,
        )
        elements.append(html.P(f"other        {other}%"))

    return elements


def create_bar_plot(
    df_value_counts: pd.DataFrame,
    **bar_kwargs: Any,
) -> dcc.Graph:

    figure = figure_apply_default_layout(
        px.bar(
            df_value_counts,
            x="x",
            y="count",
            **bar_kwargs,
        )
    )

    return dcc.Graph(figure=figure)


def create_component_bool_type(series: pd.Series) -> dcc.Graph:
    series_value_counts = series.value_counts()
    df_value_counts = value_counts_series_to_dataframe(series_value_counts)
    return create_bar_plot(
        df_value_counts,
        color="x",
        barmode="stack",
    )


def create_component_numeric_type(series: pd.Series) -> dcc.Graph:
    df_bin = get_binned_dataframe(series, numerize)
    return create_bar_plot(
        df_bin,
    )


def create_component_datetime_type(series: pd.Series) -> dcc.Graph:
    datetime_series_agg = datetime_series_aggregation(series)
    series_value_counts = datetime_series_agg.value_counts()
    df_value_counts = value_counts_series_to_dataframe(series_value_counts)
    return create_bar_plot(
        df_value_counts,
    )


def create_component_string_type(series: pd.Series) -> dcc.Graph:
    series_value_counts = series.value_counts(normalize=True)
    df_value_counts_normalized = value_counts_series_to_dataframe(
        series_value_counts,
    )
    return get_most_common_values(df_value_counts_normalized)


def create_df_visualizations(df: pd.DataFrame) -> Component:
    col_list: list = []

    for column_name in df:
        column_name = str(column_name)
        column_series: pd.Series = df[column_name]

        if column_series.nunique() == 1:
            component_title = column_name
            component_viz = create_component_for_one_unique_value(column_series)
        else:
            if is_bool_dtype(column_series):
                component_title = f"✔️ {column_name}"
                component_viz = create_component_bool_type(column_series)

            elif is_numeric_dtype(column_series):
                component_title = f"🔢 {column_name}"
                component_viz = create_component_numeric_type(column_series)

            elif is_datetime64_any_dtype(column_series):
                component_title = f"📅 {column_name}"
                component_viz = create_component_datetime_type(column_series)

            elif is_string_dtype(column_series):
                component_title = f"🔤 {column_name}"
                component_viz = create_component_string_type(column_series)

            else:
                component_title = f"❓ {column_name}"
                component_viz = html.P("No viz implemented for this type of column 😔")

        col_list.append(
            dbc.Col(
                [
                    html.Div(
                        [
                            html.B(component_title),
                            html.Div(component_viz),
                        ]
                    )
                ]
            )
        )

    return dbc.Row(col_list)


def component(model: Model) -> Component:
    try:
        df = sample(model, percentage=1)
    except ValueError:
        return html.Div(
            f"This component depends Sample and it is not implemented for model {model.unique_name}"
        )
    else:
        return create_df_visualizations(df)
