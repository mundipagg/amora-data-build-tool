import dash
from dash import html
from dash.development.base_component import Component

from amora.dash.components import question_details
from amora.questions import QUESTIONS

dash.register_page(
    __name__, title="Data Questions", fa_icon="fa-circle-question", location="sidebar"
)


def layout() -> Component:
    return html.Div(
        id="questions-content",
        children=html.Div(
            children=[question_details.component(question) for question in QUESTIONS]
        ),
    )
