from datetime import timedelta
from typing import List, Optional

import pkg_resources
from airflow import DAG
from airflow.operators import BaseOperator, DummyOperator

from amora.airflow.operators import amora_command_operator
from amora.config import settings


def _task_id_for_command(command: List[str]) -> str:
    """
    >>> _task_id_for_command(["materialize"])
    'materialize'

    >>> _task_id_for_command(["models", "list"])
    'models__list'

    >>> _task_id_for_command(["feature-store", "materialize-incremental"])
    'feature_store__materialize_incremental'
    """
    return " ".join(command).replace("-", "_").replace(" ", "__")


def create(
    dag_id: str = "amora",
    root_node: BaseOperator = DummyOperator("dag_root"),
    feature_store_materialize_start_date: str = "{{ ds }}",
    **dag_kwargs,
) -> DAG:
    dag = DAG(
        dag_id=dag_id,
        catchup=False,
        default_args={
            "owner": "airflow",
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(seconds=10),
        },
        **dag_kwargs,
    )

    with dag:

        def amora_command(command: List[str], task_id: Optional[str] = None):
            return amora_command_operator(
                dag=dag,
                task_id=task_id or _task_id_for_command(command),
                amora_command=command,
                k8s_namespace=settings.AIRFLOW_K8S_NAMESPACE,
                env_vars=config["amora_models_envs"],
            )

        amora_command_test = amora_command(["test"])

        (
            root_node
            >> amora_command(["materialize"])
            >> amora_command(["models", "list"])
            >> amora_command_test
        )

        if pkg_resources.has_distribution("amora[dash]"):
            amora_command_test >> amora_command(["dash", "inspect"])

        if pkg_resources.has_distribution("amora[feature-store]"):
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
