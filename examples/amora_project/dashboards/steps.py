from datetime import date

from amora.dashboards import Dashboard
from amora.filters import AcceptedValuesFilter, DateFilter
from examples.amora_project.models.step_count_by_source import (
    StepCountBySource,
    how_many_data_points_where_acquired,
    what_are_the_available_data_sources,
    what_are_the_values_observed_on_the_iphone,
    what_is_the_current_estimated_walked_distance,
    what_is_the_latest_data_point,
    what_is_the_total_step_count_to_date,
)

dashboard = Dashboard(
    uid="1",
    name="Health :: Step Analysis",
    questions=[
        [
            what_is_the_latest_data_point,
            what_is_the_current_estimated_walked_distance,
            how_many_data_points_where_acquired,
        ],
        [
            what_is_the_total_step_count_to_date,
            what_are_the_values_observed_on_the_iphone,
            what_are_the_available_data_sources,
        ],
    ],
    filters=[
        DateFilter(
            field="event_timestamp", default=date(2021, 1, 1), title="data de início"
        ),
        DateFilter(field="event_timestamp", default=date(2023, 1, 1), title="data fim"),
        AcceptedValuesFilter.from_column_values(
            column=StepCountBySource.source_name,
            title="Source device",
        ),
    ],
)
