import dash_bootstrap_components as dbc
import plotly.express as px
from dash import dash_table, dcc, html
from dash.development.base_component import Component

from amora.logger import logger
from amora.questions import Question
from amora.visualization import BarChart, BigNumber, LineChart, PieChart


def answer_visualization(question: Question) -> Component:
    try:
        visualization = question.render()
    except Exception:
        logger.exception("Unable to render question answer")
        return dbc.Alert("⚠️ Unable to render question answer", color="warning")
    view_config = visualization.config

    df = visualization.data
    if df.empty:
        return dbc.Alert("⚠️ No data", color="warning")

    if isinstance(view_config, BigNumber):
        big_number = view_config.value_func(df)
        return html.H4(big_number)

    if isinstance(view_config, LineChart):
        return dcc.Graph(
            figure=px.line(
                df,
                x=view_config.x_func(df),
                y=view_config.y_func(df),
            )
        )

    if isinstance(view_config, BarChart):
        return dcc.Graph(
            figure=px.bar(
                df,
                x=view_config.x_func(df),
                y=view_config.y_func(df),
            )
        )

    if isinstance(view_config, PieChart):
        return dcc.Graph(
            figure=px.pie(df, values=view_config.values, names=view_config.names)
        )

    return dash_table.DataTable(
        columns=[
            {"name": col, "id": col, "selectable": True} for col in df.columns.values
        ],
        data=df.to_dict("records"),
        row_selectable="multi",
        sort_action="native",
        style_cell={
            "overflow": "hidden",
            "textOverflow": "ellipsis",
            "maxWidth": 0,
        },
        export_format="csv",
        export_headers="display",
    )


def component(question: Question) -> Component:
    return dbc.Card(
        className="question-card",
        children=dbc.CardBody(
            [
                html.H5(question.name, className="card-title"),
                answer_visualization(question),
                dbc.Accordion(
                    [
                        dbc.AccordionItem(
                            html.Code(question.sql, lang="sql"), title="SQL"
                        )
                    ],
                    start_collapsed=True,
                ),
            ]
        ),
    )
