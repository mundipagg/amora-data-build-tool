import dash
from dash import html
from dash.development.base_component import Component

from amora.config import settings
from amora.dag import DependencyDAG
from amora.dash.components import dependency_dag

dash.register_page(__name__, path="/", fa_icon="fa-house", location="sidebar")


def layout() -> Component:
    return html.Div(
        [
            html.H1(f"Project: {settings.TARGET_PROJECT}", id="title"),
            dependency_dag.component(dag=DependencyDAG.from_target()),
        ]
    )
