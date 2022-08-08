import dash_bootstrap_components as dbc
import dash_cytoscape
from dash import Input, Output, callback, dcc
from dash.development.base_component import Component

from amora.dag import DependencyDAG


def component(dag: DependencyDAG, height: str = "400px") -> Component:
    return dbc.Row(
        className="cy-container",
        children=[
            dash_cytoscape.Cytoscape(
                id="cytoscape-layout",
                elements=dag.to_cytoscape_elements(),
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
