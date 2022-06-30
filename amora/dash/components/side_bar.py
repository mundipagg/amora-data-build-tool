from typing import NamedTuple

import dash
import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "24rem",
    "padding": "2rem 1rem",
}


class NavItem(NamedTuple):
    fa_icon: str
    href: str
    title: str


def nav() -> dbc.Nav:
    return dbc.Nav(
        [
            dbc.NavLink(
                [
                    html.I(className=f"fa-regular {page.get('fa_icon')}"),
                    " ",
                    page["name"],
                ],
                href=page["relative_path"],
                active="exact",
            )
            for page in dash.page_registry.values()
            if page.get("location") == "sidebar"
        ],
        vertical=True,
        pills=True,
    )


def component() -> Component:
    return html.Div(
        [
            html.H2("🌱 Amora", className="display-4"),
            html.Hr(),
            nav(),
        ],
        style=SIDEBAR_STYLE,
        id="side-bar",
    )
