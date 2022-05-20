import dash_cytoscape as cyto
from dash import Dash, dcc, html

from amora.config import settings
from amora.dag import DependencyDAG

dash_app = Dash(__name__)
server = dash_app.server

dependency_dag = DependencyDAG.from_target()

styles = {
    "container": {
        "position": "fixed",
        "display": "flex",
        "flex-direction": "column",
        "height": "100%",
        "width": "100%",
    },
    "cy-container": {"flex": "1", "position": "relative"},
    "cytoscape": {
        "position": "absolute",
        "width": "100%",
        "height": "100%",
        "z-index": 999,
    },
}


def cytoscape_view():
    return html.Div(
        className="cy-container",
        style=styles["cy-container"],
        children=[
            cyto.Cytoscape(
                id="cytoscape-layout",
                elements=dependency_dag.to_cytoscape_elements(),
                style=styles["cytoscape"],
                layout={
                    "name": "breadthfirst",
                    "roots": f'[id = "{dependency_dag.root()}"]',
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


# App
dash_app.layout = html.Div(
    style=styles["container"],
    children=[
        html.Div(
            [
                dcc.Markdown(f"# Dependency DAG"),
                dcc.Markdown(f"## `{settings.TARGET_PATH}`"),
            ]
        ),
        cytoscape_view(),
    ],
)


if __name__ == "__main__":
    dash_app.run(debug=True)
