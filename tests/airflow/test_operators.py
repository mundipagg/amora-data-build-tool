from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from amora.airflow.operators import amora_command_operator


def test_amora_command_operator():
    dag = DAG(dag_id="amora", start_date=datetime.today())

    amora_command_operator(
        dag=dag,
        amora_command=["materialize"],
        k8s_namespace="default",
    )

    assert len(dag.tasks) == 1
    k8s_operator = dag.tasks[0]
    assert isinstance(k8s_operator, KubernetesPodOperator)

    assert k8s_operator.task_id == "materialize"
    assert k8s_operator.name == "materialize"
    assert k8s_operator.namespace == "default"
    assert k8s_operator.image == "gcr.io/amora-data-build-tool/amora:latest"
    assert k8s_operator.image_pull_policy == "Always"
    assert k8s_operator.cmds == ["amora", "materialize"]


def test_amora_command_operator_adds_additional_labels():
    dag = DAG(dag_id="amora", start_date=datetime.today())

    amora_command_operator(
        dag=dag,
        amora_command=["materialize"],
        k8s_namespace="default",
        task_id="amora_materialize",
        additional_labels={"foo": "bar"},
    )

    assert len(dag.tasks) == 1

    k8s_operator = dag.tasks[0]
    assert k8s_operator.labels == {
        "dag_id": "amora",
        "task_id": "amora_materialize",
        "operator": "KubernetesPodOperator",
        "foo": "bar",
    }
