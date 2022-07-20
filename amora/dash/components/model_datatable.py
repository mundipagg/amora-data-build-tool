import dash_table

from amora.models import Model
from amora.providers.bigquery import sample


def component(model: Model) -> dash_table.DataTable:
    df = sample(model, percentage=1)
    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": col, "id": col} for col in sorted(df.columns.values)],
        export_format="csv",
        sort_action="native",
    )
