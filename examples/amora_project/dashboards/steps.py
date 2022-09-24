from amora_models.step_count_by_source import (
    how_many_data_points_where_acquired,
    what_are_the_available_data_sources,
    what_are_the_values_observed_on_the_iphone,
    what_is_the_current_estimated_walked_distance,
    what_is_the_latest_data_point,
    what_is_the_total_step_count_to_date,
)

from amora.dashboards import AcceptedValuesFilter, Dashboard, DateFilter

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
            default="2021-01-01", title="data de início", id="start-date-filter"
        ),
        DateFilter(default="2023-01-01", title="data fim", id="end-date-filter"),
        AcceptedValuesFilter(
            default="iPhone",
            values=["Diogo's iPhone", "iPhone"],
            title="Source device",
            id="source-device-filter",
        ),
    ],
)
