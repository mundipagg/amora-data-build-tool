from typing import NamedTuple, Optional

import dash
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from authlib.oidc.core import UserInfo
from dash import html
from dash.development.base_component import Component
from dash_extensions.enrich import Input, Output, callback
from flask import session

from amora.dash.components import user_avatar


class NavItem(NamedTuple):
    fa_icon: str
    href: str
    title: str


def nav() -> dbc.Nav:
    return dbc.Nav(
        [
            dbc.NavLink(
                [
                    html.I(className=f"fa-solid {page.get('fa_icon')}"),
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


def layout() -> Component:
    return dbc.Offcanvas(
        [
            dmc.Center(html.H2("🌱", className="display-4")),
            dmc.Center(html.H2("Amora", className="display-4")),
            dmc.Space(h="lg"),
            nav(),
            dmc.Space(h=60),
            html.Hr(),
            dmc.Center(id="user-account"),
            dmc.Center(
                dbc.Alert(
                    ["Press ", dmc.Kbd("⌘"), " to toggle the menu"], color="light"
                )
            ),
        ],
        id="side-bar",
        is_open=True,
    )


@callback(Output("user-account", "children"), [Input("url", "pathname")])
def display_user(pathname):
    user: Optional[UserInfo] = session.get("user")
    if not user:
        return dbc.Button(
            children=[html.I(className=f"fa-solid fa-user"), " Login"],
            href="/login",
            external_link=True,
        )
    else:
        return user_avatar.layout(user["userinfo"])
