import dash
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from dash import html
from dash.development.base_component import Component
from dash_extensions import Lottie

from amora.config import settings
from amora.dash.components.animation import Lotties

dash.register_page(__name__, path="/", fa_icon="fa-house", location="sidebar")


def layout() -> Component:
    return dbc.Row(
        [
            dmc.Center(html.H1(f"Project: {settings.TARGET_PROJECT}")),
            Lottie(
                options=dict(
                    loop=False,
                    autoplay=True,
                    rendererSettings=dict(preserveAspectRatio="xMidYMid slice"),
                ),
                width="75%",
                url=Lotties.site_analytics_dashboard,
            ),
        ]
    )
