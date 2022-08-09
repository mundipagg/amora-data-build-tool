import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from amora.dag import DependencyDAG
from amora.dash.components import (
    dependency_dag,
    materialization_type_badge,
    model_code,
    model_columns,
    model_datatable,
    model_summary,
)
from amora.models import Model


def component(model: Model) -> Component:
    model_config = model.__model_config__
    return dbc.Card(
        [
            dbc.CardHeader(
                html.H4(model.unique_name(), className="card-title"),
            ),
            dbc.CardBody(
                [
                    dependency_dag.component(DependencyDAG.from_model(model)),
                    materialization_type_badge.component(model_config.materialized),
                    html.P(
                        model_config.description,
                        className="card-text",
                    ),
                    dbc.Accordion(
                        [
                            dbc.AccordionItem(
                                model_summary.component(model),
                                title="📈 Summary",
                                item_id="summary",
                            ),
                            dbc.AccordionItem(
                                model_columns.component(model),
                                title="📝 Docs",
                                item_id="docs",
                            ),
                            dbc.AccordionItem(
                                model_datatable.component(model),
                                title="🍰 Sample dataset",
                                item_id="sample",
                            ),
                            dbc.AccordionItem(
                                model_code.python_component(model),
                                title="🐍 Python Code",
                                item_id="python_code",
                            ),
                            dbc.AccordionItem(
                                model_code.sql_component(model),
                                title="🏗 SQL Code",
                                item_id="sql_code",
                            ),
                        ],
                        start_collapsed=True,
                        id="model-details-accordion",
                    ),
                ]
            ),
        ],
    )
