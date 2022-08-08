from typing import List

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html
from dash.development.base_component import Component

from amora.dash.components import question_details
from amora.dash.config import Color
from amora.questions import QUESTIONS

dash.register_page(
    __name__, name="Data Questions", fa_icon="fa-circle-question", location="sidebar"
)


def questions_selector() -> Component:
    return dcc.Dropdown(
        id="questions-selector",
        placeholder="🔍 Search or select",
        options=[
            {"label": question.name, "value": question.uid}
            for question in sorted(QUESTIONS, key=lambda q: q.name)
        ],
        multi=True,
        searchable=True,
    )


def layout() -> Component:
    return dbc.Row(
        [
            html.H1("Data Questions"),
            dbc.Alert(
                f"There are {len(QUESTIONS)} questions registered in this project",
                color=Color.info,
                dismissable=True,
            ),
            dbc.Row(questions_selector()),
            dbc.Row(id="selected-questions"),
        ]
    )


@callback(
    Output("selected-questions", "children"),
    Input("questions-selector", "value"),
    prevent_initial_call=True,
)
def select_value(value: List[str]):
    return [
        dbc.Col(question_details.component(question))
        for question in QUESTIONS
        if question.uid in value
    ]
