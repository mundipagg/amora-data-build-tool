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
        # style_header={"backgroundColor": "rgb(30, 30, 30)", "color": "white"},
        # style_data={"backgroundColor": "rgb(50, 50, 50)", "color": "white"},
        style_cell={"overflow": "hidden", "textOverflow": "ellipsis", "maxWidth": 0},
        export_format="csv",
        export_headers="display",
    )
