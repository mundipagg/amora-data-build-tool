from typing import List

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash.development.base_component import Component

from amora.dash.components import model_details
from amora.models import Model, amora_model_for_name, list_models

dash.register_page(__name__, fa_icon="fa-database", location="sidebar")


def models_list(models: List[Model]) -> Component:
    options = [model.unique_name for model in models]

    return dcc.Dropdown(
        options=options,
        id="model-select-dropdown",
        value=None,
        placeholder="Select a model",
    )


def layout() -> Component:
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


@dash.callback(
    Output("model-details", "children"),
    Input("model-select-dropdown", "value"),
    prevent_initial_call=True,
)
def update_model_details(value: str) -> Component:
    return model_details.component(model=amora_model_for_name(value))
