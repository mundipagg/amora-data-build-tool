import pandas as pd
from dash import dash_table, html
from dash.development.base_component import Component

from amora.models import Model


def component(model: Model) -> Component:
    df = pd.DataFrame(
        [
            {
                "column": col.key,
                "description": col.comment if col.comment else "Undocumented",
            }
            for col in model.__table__.columns
        ]
    )
    return html.Div(
        dash_table.DataTable(
            data=df.to_dict("records"),
            columns=[
                {"name": "Column", "id": "column"},
                {"name": "Description", "id": "description"},
            ],
            style_cell={"textAlign": "left"},
            style_as_list_view=True,
        ),
        id=f"model-columns-{model.unique_name}",
    )
