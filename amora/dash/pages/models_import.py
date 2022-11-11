import dash
import dash_ace
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash.development.base_component import Component
from jinja2 import Environment, PackageLoader, select_autoescape
from shed import shed

from amora.providers.bigquery import (
    BIGQUERY_TYPES_TO_PYTHON_TYPES,
    BIGQUERY_TYPES_TO_SQLALCHEMY_TYPES,
    get_client,
    get_schema,
)

dash.register_page(
    __name__,
    name="Data Models - Import",
    path="/models-import",
)


def project_dropdown() -> dcc.Dropdown:
    client = get_client()
    options = [
        {"label": project.friendly_name, "value": project.project_id}
        for project in client.list_projects()
    ]

    return dcc.Dropdown(
        options=options,
        id="project-dropdown",
        value=None,
        placeholder="Select a project",
    )


def dataset_dropdown(project_id: str) -> dcc.Dropdown:
    client = get_client()
    options = [
        {
            "label": dataset.dataset_id,
            "value": dataset.full_dataset_id,
        }
        for dataset in client.list_datasets(project_id)
    ]

    return dcc.Dropdown(
        options=options,
        id="dataset-dropdown",
        value=None,
        placeholder="Select a dataset",
    )


def table_dropdown(full_dataset_id: str) -> dcc.Dropdown:
    client = get_client()
    options = [
        {
            "label": table.table_id,
            "value": str(table.reference),
        }
        for table in client.list_tables(dataset=full_dataset_id.replace(":", "."))
    ]

    return dcc.Dropdown(
        options=options,
        id="table-dropdown",
        value=None,
        placeholder="Select a table",
    )


def model_code(table_reference: str) -> Component:
    env = Environment(
        loader=PackageLoader("amora"),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("new-model.py.jinja2")

    project, dataset, table = table_reference.split(".")
    model_name = "".join(part.title() for part in table.split("_"))

    sorted_schema = sorted(get_schema(table_reference), key=lambda field: field.name)
    model_source_code = template.render(
        BIGQUERY_TYPES_TO_PYTHON_TYPES=BIGQUERY_TYPES_TO_PYTHON_TYPES,
        BIGQUERY_TYPES_TO_SQLALCHEMY_TYPES=BIGQUERY_TYPES_TO_SQLALCHEMY_TYPES,
        dataset=dataset,
        dataset_id=f"{project}.{dataset}",
        model_name=model_name,
        project=project,
        schema=sorted_schema,
        table=table,
    )
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
