import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from amora.models import Model


def component(model: Model) -> Component:
    return html.Span(
        [
            dbc.Badge(f"{key}: {value}", color="info")
            for key, value in model.__model_config__.labels.items()
        ]
    )
