from typing import NamedTuple

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


MENU = [
    NavItem(fa_icon="fa-house", href="/", title="Home"),
    NavItem(fa_icon="fa-database", href="/models", title="Models"),
    NavItem(fa_icon="fa-shopping-cart", href="/feature-store", title="Feature Store"),
    NavItem(fa_icon="fa-vial", href="/tests", title="Tests"),
    NavItem(fa_icon="fa-chart-line", href="/dashboards", title="Dashboards"),
    NavItem(fa_icon="fa-circle-question", href="/questions", title="Data Questions"),
    NavItem(fa_icon="fa-magnifying-glass", href="/search", title="Search"),
    NavItem(fa_icon="fa-gear", href="/environment", title="Environment"),
]


def nav() -> dbc.Nav:
    return dbc.Nav(
        [
            dbc.NavLink(
                [html.I(className=f"fa-regular {item.fa_icon}"), f" {item.title}"],
                href=item.href,
                active="exact",
            )
            for item in MENU
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
