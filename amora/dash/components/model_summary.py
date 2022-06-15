import pandas as pd
from dash import dash_table
from dash.development.base_component import Component


def component(summary: pd.DataFrame) -> Component:
    return dash_table.DataTable(
        id="model-summary",
        columns=[
            {"name": col, "id": col, "selectable": True}
            for col in sorted(summary.columns.values)
        ],
        data=summary.to_dict("records"),
        row_selectable="multi",
        hidden_columns=["is_fv_feature", "is_fv_entity", "is_fv_event_timestamp"],
        style_cell={"overflow": "hidden", "textOverflow": "ellipsis", "maxWidth": 0},
        style_data_conditional=[
            {
                "if": {
                    "filter_query": "{is_fv_entity} >= 1",
                    "column_id": "column_name",
                },
                "backgroundColor": "#ddccdd",
            },
            {
                "if": {
                    "filter_query": "{is_fv_feature} >= 1",
                    "column_id": "column_name",
                },
                "backgroundColor": "#ddddcc",
            },
            {
                "if": {
                    "filter_query": "{is_fv_event_timestamp} >= 1",
                    "column_id": "column_name",
                },
                "backgroundColor": "#ccdddd",
            },
        ],
        export_format="csv",
        export_headers="display",
    )
