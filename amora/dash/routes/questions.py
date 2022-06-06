from functools import lru_cache

from dash import html
from dash.development.base_component import Component

from amora.dash.components import question_details
from amora.models import list_models
from amora.questions import QUESTIONS


@lru_cache()
def content() -> Component:
    list(list_models())
    return html.Div([question_details.component(question) for question in QUESTIONS])
