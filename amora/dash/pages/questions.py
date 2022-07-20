import dash
import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from amora.dash.components import question_details
from amora.models import list_models
from amora.questions import QUESTIONS

dash.register_page(
    __name__, title="Data Questions", fa_icon="fa-circle-question", location="sidebar"
)


def layout() -> Component:
    list(list_models())
    return html.Div(
        id="questions-content",
        children=dbc.CardGroup(
            children=[question_details.component(question) for question in QUESTIONS]
        ),
    )
