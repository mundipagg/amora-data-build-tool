import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from amora.dag import DependencyDAG
from amora.dash.components import dependency_dag, materialization_badge, model_summary
from amora.meta_queries import summarize
from amora.models import Model


def component(model: Model) -> Component:
    model_config = model.__model_config__
    return dbc.Card(
        [
            dependency_dag.component(DependencyDAG.from_model(model)),
            dbc.CardBody(
                [
                    html.H4(model.unique_name, className="card-title"),
                    materialization_badge.component(model_config.materialized),
                    html.P(
                        model_config.description,
                        className="card-text",
                    ),
                    html.Div([model_summary.component(summary=summarize(model))]),
                ]
            ),
        ],
    )
