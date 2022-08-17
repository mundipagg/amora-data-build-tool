import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash.development.base_component import Component
from dash_extensions import Lottie

from amora.dag import DependencyDAG
from amora.dash.components import dependency_dag, model_details
from amora.dash.components.animation import Lotties
from amora.models import amora_model_for_name, list_models

dash.register_page(
    __name__,
    fa_icon="fa-database",
    location="sidebar",
    name="Data Models",
    path_template="/models/<unique_identifier>",
    path="/models",
)


def models_selector() -> dcc.Dropdown:
    options = [model.unique_name() for (model, _path) in list_models()]

    return dcc.Dropdown(
        options=options,
        id="model-select-dropdown",
        value=None,
        placeholder="Select a model to begin the exploration",
    )


def layout(unique_identifier: str = None) -> Component:
    if unique_identifier:
        return model_details.component(model=amora_model_for_name(unique_identifier))

    return dbc.Row(
        id="models-content",
        children=[
            dbc.Row(html.H1("Data Models")),
            dbc.Row(models_selector()),
            dbc.Row(dependency_dag.component(dag=DependencyDAG.from_target())),
            dbc.Row(
                id="model-details",
                children=[
                    Lottie(
                        options=dict(
                            loop=True,
                            autoplay=True,
                            rendererSettings=dict(preserveAspectRatio="xMidYMid slice"),
                        ),
                        width="50%",
                        url=Lotties.metaverse_explorer,
                    ),
                ],
            ),
        ],
    )


@dash.callback(
    Output("model-details", "children"),
    Input("model-select-dropdown", "value"),
    prevent_initial_call=True,
)
def update_model_details(value: str) -> Component:
    return model_details.component(model=amora_model_for_name(value))
