import urllib.parse

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
from dash import html
from dash.development.base_component import Component

from amora.models import list_models_with_owner, owners_to_models_dict

dash.register_page(
    __name__,
    name="Data Owners",
    path_template="/owners/<owner>",
    path="/owners",
)


def list_models_owned_by(owner: str) -> Component:
    models = list(list_models_with_owner(owner=owner))
    if not models:
        return html.Div(f"There are no models owned by `{owner}`")

    return dbc.ListGroup(
        children=[
            dbc.ListGroupItem(
                children=[
                    dcc.Link(
                        model.unique_name(), href=f"/models/{model.unique_name()}"
                    ),
                    html.P(model.__model_config__.description),
                ]
            )
            for model, _file_path in models
        ]
    )


def list_model_owners() -> Component:
    return dbc.ListGroup(
        children=[
            dbc.ListGroupItem(
                dbc.Row(
                    children=[
                        dbc.Col(dcc.Link(owner, href=f"/owners/{owner}"), width=11),
                        dbc.Col(
                            dbc.Badge(
                                len(owned_models),
                                title=f"Owns {len(owned_models)} models",
                                color="primary",
                                className="me-1",
                            ),
                            width=1,
                        ),
                    ],
                    justify="between",
                )
            )
            for owner, owned_models in owners_to_models_dict().items()
            if owner
        ]
    )


def layout(owner: str = None) -> Component:
    if owner:
        owner = urllib.parse.unquote(owner)
        content = [
            dbc.Row(html.H4(owner)),
            dbc.Row(html.P("List of owned Data Models:")),
            dbc.Row(list_models_owned_by(owner)),
        ]
    else:
        content = dbc.Row(list_model_owners())

    return dbc.Container(children=content)
