from dash import dcc, html
from dash.development.base_component import Component

from amora.dashboards import AcceptedValuesFilter


def layout(filter: AcceptedValuesFilter) -> Component:
    return html.Div(
        [
            filter.title,
            dcc.Dropdown(
                id=filter.id,
                options=filter.values,
                value=filter.default or filter.values,
                multi=True,
            ),
        ]
    )
