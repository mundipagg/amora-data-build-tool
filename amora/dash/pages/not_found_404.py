import dash
from dash import html

dash.register_page(__name__)


def layout():
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger", id="page-not-found"),
            html.Hr(),
            html.P("The pathname was not recognised..."),
        ]
    )
