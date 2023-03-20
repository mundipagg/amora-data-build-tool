from pydantic import BaseModel

from amora.models import list_models
from amora.providers.bigquery import schema_for_model


def prompt_context() -> str:
    """
    Generates the a prompt context for the project models
    """

    def schema():
        for model, path in list_models():
            schema = schema_for_model(model)
            columns = ",".join(f"{field.name} {field.field_type}" for field in schema)
            yield f"# {model.unique_name()}({columns})"

    def columns_documentation():
        for (model, _model_path) in list_models():
            yield f"# Table {model.unique_name()}: {model.__model_config__.description}"
            for column in model.__table__.columns:
                if column.doc is not None:
                    yield f"# Column {column.key}: {column.doc}"

    schema_context = "\n".join(schema())
    columns_documentation_context = "\n".join(sorted(set(columns_documentation())))

    return f"{schema_context}\n{columns_documentation_context}"


class SQLPromptAnswer(BaseModel):
    sql: str

    completion_tokens: int
    prompt_tokens: int
    total_tokens: int

    request_params: dict
    response_ms: int

    class Config:
        arbitrary_types_allowed = True
