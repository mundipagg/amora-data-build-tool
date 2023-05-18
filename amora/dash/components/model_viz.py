from typing import Any, Callable, List, Optional

import dash_bootstrap_components as dbc
import pandas as pd
import plotly
import plotly.express as px
from dash import dcc, html
from dash.development.base_component import Component
from pandas.api.types import (
    is_array_like,
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_string_dtype,
)
from plotly.graph_objects import Figure

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
    return series.to_frame().reset_index().set_axis(["x", "count"], axis=1)


def cut_formatter(cut: pd.Series, formatter_function: Callable) -> List[str]:
    """
    Apply a formatter function in both `left` and `right` Interval.
    Sometimes the pd.cuts generates long Interval strings, and we may
    want to format each part of it.
    """
    return [
        f"{formatter_function(interval.left)}-{formatter_function(interval.right)}"
        for _, interval in cut.iteritems()
    ]


def get_binned_dataframe(
    series: pd.Series,
    formatter_function: Optional[Callable] = None,
) -> pd.DataFrame:
    bins = min(series.nunique(), 10)
    cut = pd.cut(x=series, bins=bins, include_lowest=True, precision=0, right=False)

    if formatter_function:
        formatted = cut_formatter(cut, formatter_function)
        frequencies = pd.value_counts(formatted, sort=False)
    else:
        frequencies = pd.value_counts(cut, sort=False)

    df_frequencies = (
        frequencies.to_frame()
        .reset_index()
        .set_axis(["x", "count"], axis=1)
        .sort_values(by="x")
    )

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
) -> List[Component]:
    elements = []

    for _, row in df_value_counts_normalized.head(2).iterrows():
        percentage = round((row["count"] * 100), 2)
        elements.append(
            dbc.Row(
                [
                    dbc.Col(
                        f"{row['x']}",
                        style={
                            "text-overflow": "ellipsis",
                            "overflow": "hidden",
                            "white-space": "nowrap",
                            "max-width": "200px",
                        },
                    ),
                    dbc.Col(html.B(f"{percentage}%")),
                ]
            )
        )

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
        elements.append(dbc.Row([dbc.Col("other"), dbc.Col(html.B(f"{other}%"))]))

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
    series_value_counts.index = series_value_counts.index.map(str)
    df_value_counts = value_counts_series_to_dataframe(series_value_counts)
    return create_bar_plot(
        df_value_counts,
    )


def create_component_numeric_type(series: pd.Series) -> dcc.Graph:
    if series.nunique() <= 2:
        return create_component_bool_type(series)
    else:
        figure = plotly.hist_series(series)
        return dcc.Graph(figure=figure_apply_default_layout(figure))


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


def visualization_for_series(series: pd.Series) -> dbc.Card:
    if is_array_like(series[0]):
        component_title = f"⚠️ {series.name}"
        component_viz = dbc.Alert(
            "Visualization not implemented for arrays", color="info"
        )
    elif series.nunique() == 1:
        component_title = str(series.name)
        component_viz = create_component_for_one_unique_value(series)
    elif is_bool_dtype(series):
        component_title = f"✔️ {series.name}"
        component_viz = create_component_bool_type(series)

    elif is_numeric_dtype(series):
        component_title = f"🔢 {series.name}"
        component_viz = create_component_numeric_type(series)

    elif is_datetime64_any_dtype(series):
        component_title = f"📅 {series.name}"
        component_viz = create_component_datetime_type(series)

    elif is_string_dtype(series):
        component_title = f"🔤 {series.name}"
        component_viz = create_component_string_type(series)
    else:
        component_title = f"❓ {series.name}"
        component_viz = html.P("No viz implemented for this type of column 😔")

    return dbc.Card(
        [dbc.CardHeader(component_title), dbc.CardBody(component_viz)],
        className="w-75 mb-3",
    )


def create_df_visualizations(df: pd.DataFrame) -> Component:
    col_list = [
        dbc.Col(
            visualization_for_series(series=df[column_name]),
            width=4,
        )
        for column_name in df
    ]

    return dbc.Row(col_list)


def component(model: Model) -> Component:
    try:
        df = sample(model, percentage=1)
    except ValueError:
        return html.Div(
            f"This component depends on `amora.providers.bigquery.sample`, which isn't available for model `{model.unique_name}`"
        )
    else:
        return dbc.Row(
            [
                dbc.Row(
                    dbc.Alert(
                        "The visualizations bellow are generated from a 1% model sample data",
                        color="info",
                    )
                ),
                dbc.Row(create_df_visualizations(df)),
            ]
        )
