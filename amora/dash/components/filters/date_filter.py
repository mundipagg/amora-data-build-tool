from dash import dcc, html
from dash.development.base_component import Component

from amora.filters import DateFilter


def layout(filter: DateFilter) -> Component:
    return html.Div(
        [
            filter.title,
            dcc.DatePickerSingle(
                min_date_allowed=filter.min_selectable_date,
                max_date_allowed=filter.max_selectable_date,
                initial_visible_month=filter.default,
                date=filter.default,
            ),
        ]
    )
