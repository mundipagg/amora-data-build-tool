import dash
from dash import Dash, dcc, html

from amora.dash.authentication import add_auth0_login
from amora.dash.components import side_bar
from amora.dash.config import settings
from amora.dash.css_styles import styles

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
