import dash_bootstrap_components as dbc
from dash import dash_table, html
from dash.development.base_component import Component

from amora.questions import Question
from amora.visualization import Visualization, VisualizationKind


def answer_visualization(visualization: Visualization) -> Component:
    if visualization.config.kind == VisualizationKind.big_number:
        big_number = visualization.data.get_value()
        return html.P()
    elif visualization.config.kind == VisualizationKind.table:
        return dash_table.DataTable(
            columns=[
                {"name": col, "id": col, "selectable": True}
                for col in visualization.data.columns.values
            ],
            data=visualization.data.to_dict("records"),
            row_selectable="multi",
            sort_action="native",
            # style_header={"backgroundColor": "rgb(30, 30, 30)", "color": "white"},
            # style_data={"backgroundColor": "rgb(50, 50, 50)", "color": "white"},
            export_format="csv",
            export_headers="display",
        )


def component(question: Question) -> Component:
    return dbc.CardGroup(
        [
            dbc.Card(
                dbc.CardBody(
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
                )
            )
        ]
    )
