import dash
import dash_bootstrap_components as dbc
from dash_extensions import EventListener
from dash_extensions.enrich import Dash, dcc

from amora.dash.authentication import add_auth0_login
from amora.dash.components import side_bar
from amora.dash.config import settings
from amora.models import list_models

dash_app = Dash(
    __name__, external_stylesheets=settings.external_stylesheets, use_pages=True
)

if settings.auth0_login_enabled:
    add_auth0_login(dash_app)

LISTENABLE_EVENTS = [
    {
        "event": "keydown",
        "props": ["key", "altKey", "ctrlKey", "shiftKey", "metaKey"],
    },
]
# App
dash_app.layout = dbc.Container(
    children=[
        dcc.Location(id="url"),
        EventListener(events=LISTENABLE_EVENTS, logging=True, id="event-listener"),
        side_bar.layout(),
        dbc.Row(
            dash.page_container,
            id="page-content",
        ),
    ],
)

# fixme: should be in a post init callback or shouldn't be needed at all
list(list_models())
