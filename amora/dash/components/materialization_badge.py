from datetime import datetime
from typing import Optional, Union

import dash_bootstrap_components as dbc
import humanize
from dash import html
from dash.development.base_component import Component


def component(updated_at: Union[datetime, None]) -> Optional[Component]:
    if updated_at:
        delta = datetime.now() - updated_at
        label = humanize.naturaltime(delta)
        badge = dbc.Badge(
            label, title=updated_at.isoformat(), color="light", className="me-1"
        )
    else:
        badge = dbc.Badge("Unknown", color="warning", className="me-1")

    return html.Div(
        [
            html.Small("Last materialization "),
            badge,
        ]
    )
