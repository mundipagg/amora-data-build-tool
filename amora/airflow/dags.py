from datetime import timedelta
from typing import List, Optional

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator

from amora.airflow.config import settings
from amora.airflow.operators import amora_command_operator


def default(
    dag_id: str = "amora-default",
    root_node: Optional[BaseOperator] = None,
    feature_store_materialize_start_date: str = "{{ ds }}",
    with_dash: bool = False,
    with_feature_store: bool = False,
    **dag_kwargs,
) -> DAG:
    """
    Default DAG for an Amora Project data pipeline.

    Args:
        dag_id: Airflow DAG ID
        root_node: Root node of the DAG
        feature_store_materialize_start_date: Start date for feature store materialization
        dag_kwargs: Additional kwargs for the DAG
        with_dash: Whether to add extra tasks for `amora[dash]`
        with_feature_store: Whether to add extra tasks for `amora[feature-store]`


    Returns:
        DAG
    """
    if not root_node:
        root_node = EmptyOperator(task_id="dag_root")

    dag = DAG(
        dag_id=dag_id,
        catchup=False,
        default_args={
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(seconds=10),
        },
        **dag_kwargs,
    )

    with dag:

        def amora_command(command: List[str], task_id: Optional[str] = None):
            return amora_command_operator(
                amora_command=command,
                dag=dag,
                k8s_namespace=settings.K8S_NAMESPACE,
                task_id=task_id,
            )

        amora_command_test = amora_command(["test"])

        (
            root_node
            >> amora_command(["compile"])
            >> amora_command(["materialize"])
            >> amora_command(["models", "list"])
            >> amora_command_test
        )

        if with_dash:
            amora_command_test >> amora_command(["dash", "inspect"])

        if with_feature_store:
            (
                amora_command_test
                >> amora_command(["feature-store", "plan"])
                >> amora_command(["feature-store", "apply"])
                >> amora_command(
                    [
                        "feature-store",
                        "materialize",
                        feature_store_materialize_start_date,
                        "{{ next_ds }}",
                    ],
                    task_id="feature_store__materialize",
                )
            )

    return dag
