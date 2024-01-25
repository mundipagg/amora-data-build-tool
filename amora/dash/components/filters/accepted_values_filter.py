from dash import dcc, html
from dash.development.base_component import Component

from amora.filters import AcceptedValuesFilter


def layout(filter: AcceptedValuesFilter) -> Component:
    return html.Div(
        [
            filter.title,
            dcc.Dropdown(
                id=filter.field,
                options=filter.selectable_values,
                value=filter.default,
                multi=True,
            ),
        ]
    )
