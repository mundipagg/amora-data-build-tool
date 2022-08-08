import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash.development.base_component import Component
from dash_extensions import Lottie

from amora.dash.components import question_details
from amora.dash.components.animation import Lotties
from amora.dash.components.filters import filter
from amora.dashboards import DASHBOARDS, Dashboard, list_dashboards

dash.register_page(
    __name__,
    fa_icon="fa-chart-line",
    location="sidebar",
    path="/dashboards",
    path_template="/dashboards/<dashboard_id>",
)

list_dashboards()


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


def dashboards_dropdown() -> Component:
    options = [
        {"label": dashboard.name, "value": dashboard.uid}
        for dashboard in DASHBOARDS.values()
    ]
    return dcc.Dropdown(
        options=options,
        id="dashboard-select-dropdown",
        value=None,
        placeholder="Select a dashboard",
    )


def dashboards_selector() -> Component:
    return html.Div(
        [
            html.H1("🧑‍🔬 Select a dashboard and start exploring"),
            dashboards_dropdown(),
            Lottie(
                options=dict(
                    loop=True,
                    autoplay=True,
                    rendererSettings=dict(preserveAspectRatio="xMidYMid slice"),
                ),
                width="70%",
                url=Lotties.green_bi_dashboard,
            ),
        ],
        id="dashboard-content",
        style={"min-height": "600px"},
    )


def layout(dashboard_id: str = None) -> Component:
    if not dashboard_id:
        return dashboards_selector()

    dashboard = DASHBOARDS[dashboard_id]
    return html.Div([html.H1(dashboard.name), render(dashboard)])


@dash.callback(
    Output("dashboard-content", "children"),
    Input("dashboard-select-dropdown", "value"),
    prevent_initial_call=True,
)
def update_dashboard_details(value: str) -> Component:
    return render(dashboard=DASHBOARDS[value])
