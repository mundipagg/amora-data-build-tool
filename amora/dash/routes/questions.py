import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from amora.dash.components import question_details
from amora.models import list_models
from amora.questions import QUESTIONS


def content() -> Component:
    list(list_models())
    return html.Div(
        id="questions-content",
        children=dbc.CardGroup(
            children=[question_details.component(question) for question in QUESTIONS]
        ),
    )
