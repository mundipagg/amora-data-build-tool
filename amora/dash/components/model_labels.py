import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from amora.models import Model


def component(model: Model) -> Component:
    return html.Span(
        [dbc.Badge(str(label), color="info") for label in model.__model_config__.labels]
    )
