import dash_ace
from dash import html
from dash.development.base_component import Component

from amora.models import Model, model_path_for_model


def python_component(model: Model) -> Component:
    source_code = model_path_for_model(model).read_text()
    return html.Div(
        [
            dash_ace.DashAceEditor(
                id="input",
                value=source_code,
                theme="github",
                mode="python",
                tabSize=2,
                enableBasicAutocompletion=True,
                enableLiveAutocompletion=True,
                autocompleter="/autocompleter?prefix=",
                placeholder="Python code ...",
            )
        ]
    )


def sql_component(model: Model) -> Component:
    source_code = model.target_path(
        model_file_path=model_path_for_model(model)
    ).read_text()
    return html.Div(
        [
            dash_ace.DashAceEditor(
                id="input",
                value=source_code,
                theme="github",
                mode="SQL",
                tabSize=2,
                enableBasicAutocompletion=True,
                enableLiveAutocompletion=True,
                autocompleter="/autocompleter?prefix=",
                placeholder="SQL code ...",
            )
        ]
    )
