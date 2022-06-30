from typing import Iterable, Tuple

import dash
import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from amora.config import settings as base_settings
from amora.dash.config import settings as dash_settings
from amora.feature_store.config import settings as feature_store_settings

dash.register_page(__name__, fa_icon="fa-gear", location="sidebar")


def get_environment_data() -> Iterable[Tuple[str, str]]:
    def gen_data():
        for settings in (base_settings, feature_store_settings, dash_settings):
            for k, v in settings.dict().items():
                yield f"{settings.Config.env_prefix}{k}", str(v)

    return sorted(gen_data())


def layout() -> Component:
    return dbc.Table(
        [
            html.Thead(html.Tr([html.Th("Key"), html.Th("Value")])),
            html.Tbody(
                [
                    html.Tr([html.Td(key), html.Td(value)])
                    for key, value in get_environment_data()
                ]
            ),
        ],
        id="environment-table",
    )
