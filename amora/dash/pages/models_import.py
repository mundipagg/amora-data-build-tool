import dash
import dash_ace
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash.development.base_component import Component
from shed import shed

from amora.providers import bigquery

dash.register_page(
    __name__,
    name="Data Models - Import",
    path="/models-import",
)


def project_dropdown() -> dcc.Dropdown:
    options = [
        {"label": project.friendly_name, "value": project.project_id}
        for project in bigquery.get_client().list_projects()
    ]

    return dcc.Dropdown(
        options=options,
        id="project-dropdown",
        value=None,
        placeholder="Select a project",
    )


def dataset_dropdown(project_id: str) -> dcc.Dropdown:
    options = [
        {
            "label": dataset.dataset_id,
            "value": dataset.full_dataset_id,
        }
        for dataset in bigquery.get_client().list_datasets(project_id)
    ]

    return dcc.Dropdown(
        options=options,
        id="dataset-dropdown",
        value=None,
        placeholder="Select a dataset",
    )


def table_dropdown(full_dataset_id: str) -> dcc.Dropdown:
    """
    >>> table_dropdown("amora-data-build-tool:amora")

    Args:
        full_dataset_id: Bigquery's dataset identifier. E.g: `amora-data-build-tool.amora`
    """

    # Datasets can be represented as `project_id.dataset_id` or `project_id:dataset_id`
    # on gcp api responses, but only the first format is accepted on requests
    dataset = full_dataset_id.replace(":", ".")
    options = [
        {
            "label": table.table_id,
            "value": str(table.reference),
        }
        for table in bigquery.get_client().list_tables(dataset)
    ]

    return dcc.Dropdown(
        options=options,
        id="table-dropdown",
        value=None,
        placeholder="Select a table",
    )


def model_code(table_reference: str) -> Component:
    model_source_code = bigquery.import_model(table_reference)
    formatted_source_code = shed(model_source_code)

    return dbc.Row(
        dbc.Row(
            [
                dash_ace.DashAceEditor(
                    id="input",
                    value=formatted_source_code,
                    theme="github",
                    mode="python",
                    tabSize=2,
                    enableBasicAutocompletion=False,
                    enableLiveAutocompletion=False,
                    readOnly=True,
                ),
                dbc.Row(dbc.Button("Import")),
            ]
        )
    )


def layout() -> Component:
    return dbc.Container(
        children=[
            html.H1("Data Model import"),
            dbc.Row(
                children=[
                    dbc.Col(project_dropdown(), id="project-selector"),
                    dbc.Col(dcc.Loading(html.Span(id="dataset-selector"))),
                    dbc.Col(dcc.Loading(html.Span(id="table-selector"))),
                ]
            ),
            dbc.Row(id="amora-model-code"),
        ]
    )


@dash.callback(
    Output("dataset-selector", "children"),
    Input("project-dropdown", "value"),
    prevent_initial_call=True,
)
def update_dataset_selector(value: str) -> Component:
    return dataset_dropdown(project_id=value)


@dash.callback(
    Output("table-selector", "children"),
    Input("dataset-dropdown", "value"),
    prevent_initial_call=True,
)
def update_table_selector(value: str) -> Component:
    if value is None:
        raise dash.exceptions.PreventUpdate

    return table_dropdown(full_dataset_id=value)


@dash.callback(
    Output("amora-model-code", "children"),
    Input("table-dropdown", "value"),
    prevent_initial_call=True,
)
def render_model_code(value: str) -> Component:
    if value is None:
        raise dash.exceptions.PreventUpdate

    return html.Div(model_code(table_reference=value))
