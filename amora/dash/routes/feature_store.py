"""
- Lista de feature views
    - data lineage
    - colunas
        - nome
        - tipo
    - nome
    - total de chaves na online store
    - summary da offline store
    - estimativa de custo da online store?
    - timestamp da última materialização

- cachear informações
"""
import dash_bootstrap_components as dbc
import pandas as pd
from dash import html
from dash.dash_table import DataTable
from dash.development.base_component import Component
from feast import FeatureService, FeatureView

from amora.feature_store.registry import FEATURE_REGISTRY
from amora.models import AmoraModel, list_models


def feature_details(
    fv: FeatureView, fs: FeatureService, model: AmoraModel
) -> Component:
    df = pd.DataFrame(columns=["column_name", "column_type", "fs_type"])
    return dbc.Card(
        dbc.CardBody(
            [
                html.H5(fv.name, className="feature-view-name"),
                html.Small(
                    f"Latest time up to which the feature view has been materialized: '{fv.most_recent_end_time}'",
                    className="card-text text-muted",
                ),
                DataTable(),
            ]
        )
    )


def content() -> Component:
    list(list_models())
    card_group = dbc.CardGroup(
        [
            feature_details(fv, fs, model)
            for (fv, fs, model) in FEATURE_REGISTRY.values()
        ]
    )
    return html.Div(
        [
            html.H1("Feature Store"),
            html.H2("Registered in this project are:"),
            card_group,
        ]
    )
