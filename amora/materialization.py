from concurrent import futures
from typing import Optional

from google.cloud.bigquery import Client, QueryJobConfig, Table

from amora.compilation import compile_statement
from amora.config import settings
from amora.dag import MaterializationDAG
from amora.models import MaterializationTypes, Model


def materialize_model(model: Model) -> Optional[Table]:
    config = model.__model_config__
    materialization = config.materialized
    model_name = model.unique_name()
    sql = compile_statement(model.source())

    if materialization == MaterializationTypes.ephemeral:
        return None

    client = Client()

    if materialization == MaterializationTypes.view:
        view = Table(model_name)
        view.description = config.description
        view.labels = config.labels_dict
        view.view_query = sql

        client.delete_table(model_name, not_found_ok=True)

        created_model = client.create_table(view)

        return (
            f"✅  Created `{model_name}` as `{created_model.full_table_id}`\n"
            f"    Rows: {created_model.num_rows}\n"
            f"    Bytes: {created_model.num_bytes}\n"
        )

    if materialization == MaterializationTypes.table:
        client.delete_table(model_name, not_found_ok=True)
        query_job = client.query(
            sql,
            job_config=QueryJobConfig(
                destination=model_name,
                write_disposition="WRITE_TRUNCATE",
            ),
        )

        query_job.result()

        table = client.get_table(model_name)
        table.description = config.description
        table.labels = config.labels_dict

        if config.cluster_by:
            table.clustering_fields = config.cluster_by

        created_model = client.update_table(
            table, ["description", "labels", "clustering_fields"]
        )

        return (
            f"✅  Created `{model_name}` as `{created_model.full_table_id}`\n"
            f"    Rows: {created_model.num_rows}\n"
            f"    Bytes: {created_model.num_bytes}\n"
        )

    raise ValueError(
        f"Invalid model materialization configuration. "
        f"Valid types are: `{', '.join((m.name for m in MaterializationTypes))}`. "
        f"Got: `{materialization}`"
    )


def materialize_dag(
    dag: "MaterializationDAG",  # type: ignore
) -> None:
    with futures.ProcessPoolExecutor(
        max_workers=settings.MATERIALIZE_NUM_THREADS
    ) as executor:
        for models in dag.topological_generations():

            results = executor.map(
                materialize_model,
                models,
            )

            for result in results:
                print(result)
