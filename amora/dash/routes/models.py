from typing import List

import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.development.base_component import Component

from amora.dash.components import model_details
from amora.models import AmoraModel, list_models


def models_list(models: List[AmoraModel]) -> Component:
    return dcc.Dropdown(
        [model.unique_name for model in models],
        id="model-select-dropdown",
        value=models[0].unique_name,
    )


def content() -> Component:
    models = [model for (model, _path) in list_models()]

    return html.Div(
        [
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
        ]
    )
