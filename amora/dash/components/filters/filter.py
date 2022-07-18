from dash.development.base_component import Component

from amora.dash.components.filters import accepted_values_filter, date_filter
from amora.dashboards import AcceptedValuesFilter, DateFilter, Filter


def layout(filter: Filter) -> Component:
    if isinstance(filter, DateFilter):
        return date_filter.layout(filter)
    elif isinstance(filter, AcceptedValuesFilter):
        return accepted_values_filter.layout(filter)
