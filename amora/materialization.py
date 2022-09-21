import threading
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import Callable, Optional

from google.cloud.bigquery import Client, QueryJobConfig, Table

from amora.config import settings
from amora.models import (
    MaterializationTypes,
    Model,
    amora_model_for_name,
    amora_model_for_target_path,
)


@dataclass
class Task:
    sql_stmt: str
    model: Model
    target_file_path: Path

    @classmethod
    def for_target(cls, target_file_path: Path) -> "Task":
        return cls(
            sql_stmt=target_file_path.read_text(),
            model=amora_model_for_target_path(target_file_path),
            target_file_path=target_file_path,
        )

    def __repr__(self):
        return f"{self.model.__name__} -> {self.sql_stmt}"


def materialize_model(sql: str, model: Model) -> Optional[Table]:
    config = model.__model_config__
    materialization = config.materialized

    if materialization == MaterializationTypes.ephemeral:
        return None

    client = Client()

    if materialization == MaterializationTypes.view:
        table_name = model.unique_name()

        view = Table(table_name)
        view.description = config.description
        view.labels = config.labels_dict
        view.view_query = sql

        client.delete_table(table_name, not_found_ok=True)

        return client.create_table(view)

    if materialization == MaterializationTypes.table:
        table_name = model.unique_name()
        client.delete_table(table_name, not_found_ok=True)
        query_job = client.query(
            sql,
            job_config=QueryJobConfig(
                destination=table_name,
                write_disposition="WRITE_TRUNCATE",
            ),
        )

        query_job.result()

        table = client.get_table(table_name)
        table.description = config.description
        table.labels = config.labels_dict

        if config.cluster_by:
            table.clustering_fields = config.cluster_by

        return client.update_table(
            table, ["description", "labels", "clustering_fields"]
        )

    raise ValueError(
        f"Invalid model materialization configuration. "
        f"Valid types are: `{', '.join((m.name for m in MaterializationTypes))}`. "
        f"Got: `{materialization}`"
    )


def node_predecessors_are_materialized(
    dag: "DependencyDAG",  # type: ignore
    node: str,
    materialized: set,
) -> bool:
    predecessors = set(dag.predecessors(node))
    return predecessors == predecessors.intersection(materialized)


def materialize_worker(
    queue: Queue[Task],
    dag: "DependencyDAG",  # type: ignore
    materialized: set,
    logger: Optional[Callable] = None,
) -> None:
    while True:
        try:
            task = queue.get(timeout=5)
            model_unique_name = task.model.unique_name()

            if node_predecessors_are_materialized(dag, model_unique_name, materialized):

                table = materialize_model(sql=task.sql_stmt, model=task.model)

                materialized.add(model_unique_name)

                if table and logger:
                    logger(
                        f" ✅  Created `{model_unique_name}` as `{table.full_table_id}`\n"
                        + "    Rows: {table.num_rows}\n"
                        + "    Bytes: {table.num_bytes}\n",
                    )

            else:
                queue.put(task)

        except Empty:
            break


def materialize_dag(
    dag: "DependencyDAG",  # type: ignore
    model_to_task: dict[str, Task],
    logger: Optional[Callable] = None,
) -> None:
    materialized: set = set()

    queue: Queue = Queue()

    for model in dag:
        try:
            task = model_to_task[model]
            queue.put_nowait(task)
        except KeyError:
            materialized.add(model)
            if logger:
                logger(f"⚠️  Skipping `{amora_model_for_name(model).unique_name()}`")

    thread_list = list()
    for _ in range(settings.MATERIALIZE_NUM_THREADS):
        thread = threading.Thread(
            target=materialize_worker,
            kwargs=dict(queue=queue, dag=dag, materialized=materialized, logger=logger),
        )
        thread_list.append(thread)
        thread.start()

    for thread in thread_list:
        thread.join()
