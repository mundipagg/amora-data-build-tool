from concurrent import futures
from typing import Callable, Optional, Union

import networkx as nx
from google.cloud.bigquery import Client, QueryJobConfig, Table

from amora.config import settings
from amora.dag import DependencyDAG, Task
from amora.models import MaterializationTypes, ModelConfig
from amora.utils import list_target_files


def materialize_model(
    sql: str, model_name: str, config: ModelConfig
) -> Optional[Table]:
    materialization = config.materialized

    if materialization == MaterializationTypes.ephemeral:
        return f"    Skipping materialization of ephemeral model {model_name}"

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
    dag: "DependencyDAG",  # type: ignore
    tasks: dict[str, Task],
    logger: Optional[Callable] = None,
) -> None:
    with futures.ProcessPoolExecutor(
        max_workers=settings.MATERIALIZE_NUM_THREADS
    ) as executor:
        for models in nx.topological_generations(dag):
            sqls = [tasks[model].sql_stmt for model in models if model in tasks]
            model_names = [model for model in models if model in tasks]
            configs = [
                tasks[model].model.__model_config__
                for model in models
                if model in tasks
            ]

            results = executor.map(
                materialize_model,
                sqls,
                model_names,
                configs,
            )

            for result in results:
                if logger:
                    logger(result)


def create_materialization_tasks(models: Union[list, None]) -> dict[str, Task]:
    tasks: dict[str, Task] = {}

    for target_file_path in list_target_files():
        if models and target_file_path.stem not in models:
            continue

        task = Task.for_target(target_file_path)
        tasks[task.model.unique_name()] = task
    return tasks
