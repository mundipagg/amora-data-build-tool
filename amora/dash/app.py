from dash import Dash, Input, Output, dcc, html
from dash.development.base_component import Component

from amora.dash.components import model_details, side_bar
from amora.dash.components.main_content import content
from amora.dash.config import settings
from amora.dash.css_styles import styles
from amora.dash.routes.router import ROUTER
from amora.models import amora_model_for_name

dash_app = Dash(
    __name__,
    external_stylesheets=settings.external_stylesheets,
)


# App
dash_app.layout = html.Div(
    style=styles["container"],
    children=[
        html.Div(
            [dcc.Location(id="url"), side_bar.component(), content],
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
                html.H1("404: Not found", className="text-danger", id="page-not-found"),
                html.Hr(),
                html.P(f"The pathname `{pathname}` was not recognised..."),
            ]
        )


@dash_app.callback(
    Output("model-details", "children"),
    Input("model-select-dropdown", "value"),
    prevent_initial_call=True,
)
def update_model_details(value: str) -> Component:
    return model_details.component(model=amora_model_for_name(value))
