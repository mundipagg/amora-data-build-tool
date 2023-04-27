import urllib.parse

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
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


def model_owners_list() -> Component:
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
        ]
    )


def layout(owner: str = None) -> Component:
    if owner:
        owner = urllib.parse.unquote(owner)
        content = [
            html.H1("Data Owners"),
            html.H4(owner),
            html.P("List of owned Data Models:"),
            list_models_owned_by(owner),
        ]
    else:
        content = [
            html.H1("Data Owners"),
            html.P("List of Data Owners and the associated Data Models:"),
            model_owners_list(),
        ]

    return dbc.Container(children=[dbc.Row(row) for row in content])
