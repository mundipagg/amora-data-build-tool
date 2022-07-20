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

import dash
import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component
from feast import Feature, FeatureView

from amora.dash.components import (
    materialization_badge,
    model_columns,
    model_datatable,
    model_labels,
    model_summary,
)
from amora.feature_store import fs as store
from amora.feature_store.registry import FEATURE_REGISTRY
from amora.models import Model, list_models

dash.register_page(__name__, fa_icon="fa-shopping-cart", location="sidebar")


def entities_list_items(entities: Iterable[str]):
    for entity in entities:
        yield dbc.ListGroupItem(entity, color="primary")


def features_list_items(features: Iterable[Feature]):
    for feature in features:
        yield dbc.ListGroupItem(feature.name)


def icon_for_model(model: Model) -> html.I:
    # fixme: What kind of contract should we expect from the model ?
    icon = getattr(model, "feature_view_fa_icon", None)
    icon = icon() if icon else "fa-square-question"
    return html.I(className=f"fa-regular {icon}")


def card_item(model: Model, fv: FeatureView) -> Component:
    return dbc.Card(
        [
            dbc.CardHeader(
                dbc.Row(
                    [
                        dbc.Col(icon_for_model(model)),
                        dbc.Col(
                            html.H4(fv.name, className="card-title"),
                        ),
                    ],
                    justify="between",
                )
            ),
            dbc.CardBody(
                [
                    model_labels.component(model),
                    materialization_badge.component(fv.last_updated_timestamp),
                    dbc.Accordion(
                        [
                            dbc.AccordionItem(
                                model_summary.component(model),
                                title="📈 Summary",
                            ),
                            dbc.AccordionItem(
                                model_columns.component(model),
                                title="📝 Docs",
                            ),
                            dbc.AccordionItem(
                                model_datatable.component(model),
                                title="🍰 Sample dataset",
                            ),
                        ],
                        start_collapsed=True,
                    ),
                ]
            ),
        ],
        className="w-50",
    )


def layout() -> Component:
    list(list_models())

    registry_fvs = {
        fv.name: fv for fv in store.registry.list_feature_views(store.project)
    }

    feature_views = dbc.CardGroup(
        [
            card_item(model=model, fv=registry_fvs[fv.name])
            for (fv, fs, model) in list(FEATURE_REGISTRY.values())
        ]
    )
    return html.Div(
        id="feature-store-content",
        children=[
            html.H1("Feature Store"),
            html.H2("Registered in this project are:"),
            html.Hr(),
            feature_views,
        ],
    )
