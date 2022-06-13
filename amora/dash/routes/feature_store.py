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
from typing import Iterable

import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component
from feast import Feature, FeatureService, FeatureView

from amora.dag import DependencyDAG
from amora.dash.components import dependency_dag, model_summary
from amora.feature_store.registry import FEATURE_REGISTRY
from amora.meta_queries import summarize
from amora.models import Model, list_models


def entities_list_items(entities: Iterable[str]):
    for entity in entities:
        yield dbc.ListGroupItem(entity, color="primary")


def features_list_items(features: Iterable[Feature]):
    for feature in features:
        yield dbc.ListGroupItem(feature.name)


def feature_details(fv: FeatureView, fs: FeatureService, model: Model) -> Component:
    summary = summarize(model)

    return dbc.Card(
        dbc.CardBody(
            [
                html.H5(fv.name, className="feature-view-name"),
                html.Small(
                    f"Latest time up to which the feature view has been materialized: '{fv.most_recent_end_time}'",
                    className="card-text text-muted",
                ),
                html.Div(
                    [
                        dbc.Row(model_summary.component(summary)),
                        dbc.Row(
                            [
                                dependency_dag.component(
                                    dag=DependencyDAG.from_model(model)
                                ),
                                dbc.Col("Bla"),
                            ]
                        ),
                    ]
                ),
            ]
        )
    )


def content() -> Component:
    list(list_models())

    card_group = dbc.CardGroup(
        id="feature-store-card-group",
        children=[
            feature_details(fv, fs, model)
            for (fv, fs, model) in FEATURE_REGISTRY.values()
        ],
    )
    return html.Div(
        id="feature-store-content",
        children=[
            html.H1("Feature Store"),
            html.H2("Registered in this project are:"),
            card_group,
        ],
    )
