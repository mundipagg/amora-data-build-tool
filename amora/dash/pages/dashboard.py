import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash.development.base_component import Component

from amora.dash.components import question_details
from amora.dash.components.filters import filter
from amora.dashboards import Dashboard
from examples.amora_project.dashboards import steps

dash.register_page(
    __name__,
    fa_icon="fa-chart-line",
    location="sidebar",
    path_template="/dashboards/<dashboard_id>",
)

# fixme: Mocked. Replace me with a call to list_dashboards()
DASHBOARDS = {steps.dashboard.id: steps.dashboard}


def render(dashboard: Dashboard) -> Component:
    questions = [
        dbc.Row(
            children=[
                dbc.Col(question_details.component(question_col))
                for question_col in row
            ]
        )
        for row in dashboard.questions
    ]
    filters = [filter.layout(f) for f in dashboard.filters]

    return html.Div([html.Div(filters), html.Div(questions)])


def dashboards_list():
    options = [
        {"label": dashboard.name, "value": dashboard.id}
        for dashboard in DASHBOARDS.values()
    ]
    return dcc.Dropdown(
        options=options,
        id="dashboard-select-dropdown",
        value=None,
        placeholder="Select a dashboard",
    )


def layout(dashboard_id: str = None) -> Component:
    dashboard = DASHBOARDS.get(dashboard_id)
    if not dashboard:
        return html.Div(
            [
                html.H1(f"Dashboard not found for id `{dashboard_id}`"),
                dashboards_list(),
            ],
            id="dashboard-content",
        )
    return html.Div([html.H1(dashboard.name), render(dashboard)])


@dash.callback(
    Output("dashboard-content", "children"),
    Input("dashboard-select-dropdown", "value"),
    prevent_initial_call=True,
)
def update_dashboard_details(value: str) -> Component:
    dashboard = DASHBOARDS.get(value)
    component = render(dashboard)
    return component
