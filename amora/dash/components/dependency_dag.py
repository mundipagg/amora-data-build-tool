from typing import Iterable, Tuple

import dash_bootstrap_components as dbc
import dash_cytoscape
from dash import Input, Output, callback, dcc
from dash.development.base_component import Component

from amora.dag import CytoscapeElements, DependencyDAG
from amora.feature_store.protocols import FeatureViewSourceProtocol
from amora.models import MaterializationTypes, Model, amora_model_for_name


def _style_elements(elements: CytoscapeElements) -> CytoscapeElements:
    def border_color_for_model(model: Model) -> str:
        if isinstance(model, FeatureViewSourceProtocol):
            return "green"
        return "grey"

    def background_color_for_model(model: Model) -> str:
        return {
            MaterializationTypes.table: "black",
            MaterializationTypes.view: "grey",
            MaterializationTypes.ephemeral: "white",
        }[model.__model_config__.materialized]

    def node_models() -> Iterable[Tuple[str, Model]]:
        for elem in elements:
            if elem["group"] != "nodes":
                continue

            model_name = elem["data"]["id"]
            model = amora_model_for_name(model_name)

            yield model_name, model

    return [
        *(
            {
                "data": {
                    "id": node,
                    "label": node,
                },
                "style": {
                    "border-color": border_color_for_model(model),
                    "border-width": 3,
                    "background-color": background_color_for_model(model),
                },
            }
            for node, model in node_models()
        ),
        *(edge for edge in filter(lambda e: e["group"] == "edges", elements)),
    ]


def component(dag: DependencyDAG, height: str = "400px") -> Component:
    elements = dag.to_cytoscape_elements()
    elements = _style_elements(elements)

    return dbc.Row(
        className="cy-container",
        children=[
            dash_cytoscape.Cytoscape(
                id="cytoscape-layout",
                elements=elements,
                layout={
                    "name": "breadthfirst",
                    "refresh": 20,
                    "fit": True,
                    "padding": 30,
                    "randomize": False,
                },
                style={"width": "100%", "height": height},
                stylesheet=[
                    {"selector": "node", "style": {"label": "data(label)"}},
                    {
                        "selector": "edge",
                        "style": {
                            "curve-style": "bezier",
                            "target-arrow-shape": "triangle",
                        },
                    },
                ],
                responsive=True,
            ),
            dbc.Row(id="cytoscape-output"),
        ],
    )


@callback(
    Output("cytoscape-output", "children"),
    Input("cytoscape-layout", "tapNodeData"),
    prevent_initial_call=True,
)
def redirect_to_model(data):
    unique_identifier = data["id"]
    return dcc.Location(pathname=f"/models/{unique_identifier}", id=unique_identifier)
