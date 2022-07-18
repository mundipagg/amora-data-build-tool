import dash_bootstrap_components as dbc
import plotly.express as px
from dash import dash_table, dcc, html
from dash.development.base_component import Component

from amora.questions import Question
from amora.visualization import BigNumber, LineChart, PieChart, Visualization


def answer_visualization(visualization: Visualization) -> Component:
    view_config = visualization.config
    if isinstance(view_config, BigNumber):
        big_number = view_config.value_func(visualization.data)
        return html.H4(big_number)
    elif isinstance(view_config, LineChart):
        return dcc.Graph(
            figure=px.line(
                visualization.data,
                x=view_config.x_func(visualization.data),
                y=view_config.y_func(visualization.data),
            )
        )
    elif isinstance(view_config, PieChart):
        return dcc.Graph(
            figure=px.pie(
                visualization.data, values=view_config.values, names=view_config.names
            )
        )
    else:
        return dash_table.DataTable(
            columns=[
                {"name": col, "id": col, "selectable": True}
                for col in visualization.data.columns.values
            ],
            data=visualization.data.to_dict("records"),
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
                answer_visualization(question.render()),
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
