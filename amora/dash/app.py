import dash_bootstrap_components as dbc
from dash import Dash, Input, Output, dcc, html
from dash.development.base_component import Component

from amora.dash.components import side_bar
from amora.dash.components.main_content import content
from amora.dash.css_styles import styles
from amora.dash.routes.router import ROUTER

dash_app = Dash(
    __name__, external_stylesheets=[dbc.themes.MATERIA, dbc.icons.FONT_AWESOME]
)


# App
dash_app.layout = html.Div(
    style=styles["container"],
    children=[
        html.Div(
            [
                dcc.Location(id="url"),
                side_bar.component(),
                content
            ],
        )
    ],
)


@dash_app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname: str) -> Component:
    try:
        route_content = ROUTER[pathname]
        return route_content()
    except KeyError:
        # If the user tries to reach a different page, return a 404 message
        return html.Div(
            [
                html.H1("404: Not found", className="text-danger"),
                html.Hr(),
                html.P(f"The pathname `{pathname}` was not recognised..."),
            ]
        )
