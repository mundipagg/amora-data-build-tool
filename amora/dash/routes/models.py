from typing import List

import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.development.base_component import Component

from amora.dash.components import model_details
from amora.models import Model, list_models


def models_list(models: List[Model]) -> Component:
    options = [model.unique_name for model in models]

    return dcc.Dropdown(
        options=options,
        id="model-select-dropdown",
        value=options[0],
        placeholder="Select a model",
    )


def content() -> Component:
    models = [model for (model, _path) in list_models()]

    return html.Div(
        id="models-content",
        children=[
            dbc.Row(
                [
                    dbc.Col(
                        [
                            models_list(models),
                            html.Div(
                                model_details.component(models[0]),
                                id="model-details",
                            ),
                        ]
                    ),
                ],
                align="start",
            )
        ],
    )
