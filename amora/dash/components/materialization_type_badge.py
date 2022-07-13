import dash_bootstrap_components as dbc

from amora.models import MaterializationTypes


def component(materialization_type: MaterializationTypes) -> dbc.Badge:
    return dbc.Badge(materialization_type.value, color="primary", className="me-1")
