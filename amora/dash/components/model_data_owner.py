from dash import dcc, html
from dash.development.base_component import Component

from amora.models import Model


def layout(model: Model) -> Component:
    owner = model.owner()
    if owner:
        content = [
            "Owner: ",
            dcc.Link(model.owner(), href=f"/owners/{owner}"),
        ]
    else:
        content = ["Owner: Unowned"]

    return html.H6(children=content, id="model-data-owner")
