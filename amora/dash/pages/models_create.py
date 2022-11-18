from pathlib import Path
from typing import List

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc
from dash.development.base_component import Component

from amora.dash.components import (
    file_browser,
    label_browser,
    model_datatable,
    model_details,
)
from amora.models import amora_model_for_path

dash.register_page(
    __name__,
    fa_icon="fa-vial",
    location="sidebar",
    name="Data Model Create",
    path="/models-create",
)


def layout() -> Component:
    return dbc.Container(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [file_browser.layout(), label_browser.layout()],
                        width=4,
                    ),
                    dbc.Col(id="file-browser-model-code", width=6),
                    dbc.Col(id="model-create-results"),
                    dcc.Store(id="selected-model-file-path"),
                ]
            )
        ]
    )


@dash.callback(
    Output("file-browser-model-code", "children"),
    Output("selected-model-file-path", "data"),
    Input(file_browser.component_id, "selectedKeys"),
    Input(label_browser.component_id, "selectedKeys"),
    prevent_initial_call=True,
)
def render_model_code(file_browser_keys: List[str], label_browser_keys: List[str]):
    model_file_path = Path(
        file_browser_keys[0] if file_browser_keys else label_browser_keys[0]
    )
    model = amora_model_for_path(model_file_path)

    return (
        model_details.component(model),
        model_file_path.as_posix(),
    )


@dash.callback(
    Output("model-create-results", "children"),
    Input("run-model", "n_clicks"),
    Input("selected-model-file-path", "data"),
    prevent_initial_call=True,
)
def run_model_code(n_clicks: int, selected_model_file_path: str) -> Component:
    model = amora_model_for_path(Path(selected_model_file_path))
    return model_datatable.component(model)
