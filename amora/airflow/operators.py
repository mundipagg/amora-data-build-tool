from typing import Dict, List

from airflow.kubernetes.secret import Secret
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client.models.v1_env_var import V1EnvVar
from amora.config import settings


def amora_command_operator(
    dag: DAG,
    task_id: str,
    amora_command: List[str],
    k8s_namespace: str,
    env_vars: Dict[str, str],
    additional_labels: Dict[str, str] = {},
) -> KubernetesPodOperator:
    """
    Create a KubernetesPodOperator to run an Amora command in a Kubernetes cluster.

    Args:
        dag (DAG): The DAG object to which the task belongs.
        task_id (str): The task_id of the task.
        docker_image (str): The Docker image to use for the pod.
        amora_command (List[str]): The Amora command to run in the pod.
        k8s_namespace (str): The Kubernetes namespace to run the pod in.
        env_vars (Dict[str, str]): A dictionary of environment variable names and values to set in the pod.
        additional_labels (Dict[str, str]): Additional labels to add to the pod.

    Returns:
        KubernetesPodOperator: A KubernetesPodOperator object for running the specified Amora command.

    """
    service_account_secret = Secret(  # nosec
        deploy_type="volume",
        # Path where we mount the secret as volume
        deploy_target="/tmp",
        # Name of Kubernetes Secret
        secret=settings.AIRFLOW_OPERATOR_SERVICE_ACCOUNT_SECRET_NAME,
        # Key in the form of service account file name
        key="service-account",
    )

    redis_connection_secret = Secret(  # nosec
        deploy_type="env",
        deploy_target="AMORA_FEATURE_STORE_ONLINE_STORE_CONFIG",
        secret=settings.AIRFLOW_K8S_OPERATOR_SERVICE_ACCOUNT_SECRET_NAME,
        key="redis-connection-string",
    )

    return KubernetesPodOperator(
        name=task_id,
        task_id=task_id,
        image=settings.AIRFLOW_K8S_OPERATOR_DOCKER_IMAGE,
        cmds=["amora"] + amora_command,
        labels={
            "operator": KubernetesPodOperator.__name__,
            "task_id": task_id,
            "dag_id": dag.dag_id,
            **additional_labels,
        },
        secrets=[service_account_secret, redis_connection_secret],
        env_vars=[V1EnvVar(name=name, value=value) for name, value in env_vars.items()],
        is_delete_operator_pod=settings.AIRFLOW_K8S_OPERATOR_IS_DELETE_OPERATOR_POD,
        namespace=k8s_namespace,
        dag=dag,
        image_pull_policy=settings.AIRFLOW_K8S_OPERATOR_DOCKER_IMAGE_PULL_POLICY,
        pod_runtime_info_envs=[],
    )
