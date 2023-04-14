from typing import Dict, List, Optional

from airflow.kubernetes.secret import Secret
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client.models.v1_env_var import V1EnvVar

from amora.airflow.config import settings


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


def amora_command_operator(
    dag: DAG,
    amora_command: List[str],
    k8s_namespace: str,
    task_id: Optional[str] = None,
    additional_labels: Optional[Dict[str, str]] = None,
) -> KubernetesPodOperator:
    """
    Create a KubernetesPodOperator to run an Amora command in a Kubernetes cluster.

    Args:
        dag (DAG): The DAG object to which the task belongs.
        task_id (str): The task_id of the task.
        amora_command (List[str]): The Amora command to run in the pod.
        k8s_namespace (str): The Kubernetes namespace to run the pod in.
        additional_labels (Dict[str, str]): Additional labels to add to the pod.

    Returns:
        KubernetesPodOperator: A KubernetesPodOperator object for running the specified Amora command.

    """
    if task_id is None:
        task_id = _task_id_for_command(amora_command)
    if additional_labels is None:
        additional_labels = {}

    service_account_secret = Secret(  # nosec
        deploy_type="volume",
        deploy_target="/tmp",
        secret=settings.K8S_OPERATOR_SERVICE_ACCOUNT_SECRET_NAME,
        key=settings.K8S_OPERATOR_SERVICE_ACCOUNT_SECRET_KEY,
    )

    redis_connection_secret = Secret(  # nosec
        deploy_type="env",
        deploy_target="AMORA_FEATURE_STORE_ONLINE_STORE_CONFIG",
        secret=settings.K8S_OPERATOR_REDIS_CONNECTION_SECRET_NAME,
        key=settings.K8S_OPERATOR_REDIS_CONNECTION_SECRET_KEY,
    )

    return KubernetesPodOperator(
        cmds=["amora"] + amora_command,
        image=settings.K8S_OPERATOR_DOCKER_IMAGE,
        labels={
            "operator": KubernetesPodOperator.__name__,
            "task_id": task_id,
            "dag_id": dag.dag_id,
            **additional_labels,
        },
        namespace=k8s_namespace,
        secrets=[service_account_secret, redis_connection_secret],
        env_vars=[V1EnvVar(name=k, value=v) for k, v in settings.k8s_env_vars()],
        is_delete_operator_pod=settings.K8S_OPERATOR_IS_DELETE_OPERATOR_POD,
        dag=dag,
        name=task_id,
        task_id=task_id,
        image_pull_policy=settings.K8S_OPERATOR_DOCKER_IMAGE_PULL_POLICY,
        pod_runtime_info_envs=[],
    )
