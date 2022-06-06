import dash_cytoscape
from dash import html
from dash.development.base_component import Component

from amora.dag import DependencyDAG


def component(dag: DependencyDAG) -> Component:

    return html.Div(
        className="cy-container",
        # style=styles["cy-container"],
        children=[
            dash_cytoscape.Cytoscape(
                id="cytoscape-layout",
                elements=dag.to_cytoscape_elements(),
                layout={
                    "name": "breadthfirst",
                    "roots": f'[id = "{dag.root()}"]',
                    "refresh": 20,
                    "fit": True,
                    "padding": 30,
                    "randomize": False,
                },
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
            )
        ],
    )
