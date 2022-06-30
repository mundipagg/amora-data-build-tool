import dash
from dash import Dash, Input, Output, dcc, html
from dash.development.base_component import Component

from amora.dash.authentication import add_auth0_login
from amora.dash.components import model_details, side_bar
from amora.dash.config import settings
from amora.dash.css_styles import styles
from amora.models import amora_model_for_name

dash_app = Dash(
    __name__, external_stylesheets=settings.external_stylesheets, use_pages=True
)

if settings.auth0_login_enabled:
    add_auth0_login(dash_app)

# App
dash_app.layout = html.Div(
    style=styles["container"],
    children=[
        html.Div(
            [
                dcc.Location(id="url"),
                side_bar.component(),
                html.Div(
                    dash.page_container,
                    style={
                        "margin-left": "24rem",
                        "margin-right": "2rem",
                        "padding": "2rem 1rem",
                        "overflow": "scroll",
                    },
                    id="page-content",
                ),
            ],
        ),
    ],
)


@dash_app.callback(
    Output("model-details", "children"),
    Input("model-select-dropdown", "value"),
    prevent_initial_call=True,
)
def update_model_details(value: str) -> Component:
    return model_details.component(model=amora_model_for_name(value))
