from typing import List, Any

import numpy as np
import pandas as pd


from dash import html, dcc
from dash.development.base_component import Component

import plotly.express as px
import plotly.graph_objects as go

from amora.models import Model
from amora.providers.bigquery import sample


def create_component_with_stacked_bar_for_two_unique_values(
    df_value_counts: pd.DataFrame, column: str
) -> Component:
    """
    Create component for boolean values.
    Returns a stacked bar plot.
    """

    index_values = df_value_counts[column].values
    values = df_value_counts["count"].values

    stacked_bar_plot = go.Figure(
        data=[
            go.Bar(name=index_values[0], x=[column], y=[values[0]]),
            go.Bar(name=index_values[1], x=[column], y=[values[1]]),
        ]
    )
    stacked_bar_plot.update_layout(barmode="stack")

    return stacked_bar_plot


def create_component_with_text_for_unique_values(value: Any, column: str) -> Component:
    """
    Return component for columns with unique values of any type.
    The component is a html.H1 element with the text "100% of values: {value}".
    """
    return html.P(f"{column} has 100% of values: {value}", style={"color": "red"})


def create_component_with_most_frequent_values(
    df: pd.DataFrame, column_name: str
) -> Component:
    """
    Create a component for a column with multiple values with string type.
    The component will be a html.P with the format:
        {value}        X%
        {value2}       Y%
        other          (100 - (X + Y))%
    """

    value_counts_normalized = df[column_name].value_counts(normalize=True)
    df_value_counts_normalized = (
        value_counts_normalized.to_frame()
        .reset_index()
        .rename(columns={column_name: "count", "index": column_name})
    )

    elements = []

    for _, row in df_value_counts_normalized.head(2).iterrows():
        elements.append(
            html.P(f"{row[column_name]}        {round((row['count']*100), 2)}%")
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
        elements.append(html.P(f"other        {other}%"))
    return html.Div(elements)


def bin_series(
    column_series: pd.Series, column: str, number_of_unique_values: int
) -> pd.DataFrame:
    """
    Creates interval of the series and return a dataframe with the columns:
        `count`: frequency
        `index`: interval`
    """
    k = number_of_unique_values if number_of_unique_values <= 10 else 10
    frequencies = pd.value_counts(
        pd.cut(x=column_series, bins=k, include_lowest=True), sort=False
    )
    df_frequencies = (
        frequencies.to_frame()
        .reset_index()
        .rename(columns={column: "count", "index": column})
    )
    df_frequencies[column] = df_frequencies[column].astype(str)
    return df_frequencies


def create_component_with_bar_chart(df: pd.DataFrame, column: str) -> Component:
    """
    Create component with bar plot with its frequencies.
    """
    return dcc.Graph(figure=px.bar(df, x=column, y="count"))


def create_component_without_chart(column_name: str) -> Component:
    """
    Returns a component indication error for that column
    """
    return html.P(f"Column {column_name} dont have viz yet. 😔")


def create_component_list(df: pd.DataFrame) -> List[Component]:
    """
    Given a dataframe, create a html component (viz) for each column.
    # TODO: return a dict instead of a list (list can cause column order)
    """

    summary_plots = []

    for column_name in df:
        column_name = str(column_name)
        column_type = df[column_name].dtypes
        value_counts = df[column_name].value_counts()

        unique_values = len(value_counts)

        if unique_values == 1:
            column_component = create_component_with_text_for_unique_values(
                df[column_name][0], column_name
            )
        else:
            if column_type in [float, int]:
                df_bin = bin_series(df[column_name], column_name, unique_values)
                column_component = create_component_with_bar_chart(df_bin, column_name)
            elif column_type == str:
                column_component = create_component_with_most_frequent_values(
                    df, column_name
                )
            elif column_type == bool:
                df_value_counts = (
                    value_counts.to_frame()
                    .reset_index()
                    .rename(columns={column_name: "count", "index": column_name})
                )
                column_component = create_component_with_stacked_bar_for_two_unique_values(
                    df_value_counts, column_name
                )
            else:
                column_component = create_component_without_chart(column_name)

        summary_plots.append(column_component)

    return summary_plots


def component(model: Model) -> List[Component]:
    """
    Get a sample of the model and create basic viz.
    """
    try:
        df = sample(model, percentage=1)
    except ValueError:
        return html.Div(f"Kaggle needs sample! {model.unique_name}")
    else:
        return create_component_list(df)
