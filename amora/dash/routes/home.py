from dash import html
from dash.development.base_component import Component

from amora.config import settings
from amora.dag import DependencyDAG
from amora.dash.components import dependency_dag


def content() -> Component:
    return html.Div(
        [
            html.H1(f"Project: {settings.TARGET_PROJECT}", id="title"),
            dependency_dag.component(dag=DependencyDAG.from_target()),
        ]
    )
