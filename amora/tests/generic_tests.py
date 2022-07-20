"""
Tests that can be reused by multiple projects
"""
from amora.models import MaterializationTypes, list_models
from amora.providers.bigquery import (
    get_schema,
    schema_for_model,
    schema_for_model_source,
)


def test_materialized_schema_equal_local_schema():
    """
    Validates that the current materialization has the same schema
    as the locally defined model
    """
    for model, model_path in list_models():
        if MaterializationTypes.ephemeral == model.__model_config__.materialized:
            continue

        materialized_schema = get_schema(model.unique_name)
        model_schema = schema_for_model(model)

        assert set(materialized_schema) == set(model_schema), (
            f"Diff found between the materialized model `{model.unique_name}` "
            f"schema and the local definition `{model_path}`"
        )


def test_models_schema_equal_its_source_schema():
    """
    Validates that the model schema matches its `source` classmethod result schema
    """
    for model, _model_path in list_models():
        source = model.source()

        if source is None:
            continue

        model_schema = schema_for_model(model)
        source_schema = schema_for_model_source(model)

        assert set(source_schema) == set(model_schema), (
            f"Diff found between the schema of `source` classmethod "
            f"and the model schema definition on `{model.unique_name}`"
        )
