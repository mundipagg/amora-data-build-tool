from amora.dag import DependencyDAG
from amora.utils import clean_compiled_files

from tests.models.health import Health
from tests.models.heart_rate import HeartRate


def setup_function(module):
    clean_compiled_files()


def test_DependencyDAG_root():
    dag = DependencyDAG()
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    assert dag.root() == "a"


def test_DependencyDAG_root_with_empty_dag():
    dag = DependencyDAG()
    assert dag.root() is None


def test_DependencyDAG_to_cytoscape_elements():
    dag = DependencyDAG()
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    assert dag.to_cytoscape_elements() == [
        {"group": "nodes", "data": {"id": "a", "label": "a"}},
        {"group": "nodes", "data": {"id": "b", "label": "b"}},
        {"group": "nodes", "data": {"id": "c", "label": "c"}},
        {"group": "edges", "data": {"source": "a", "target": "b"}},
        {"group": "edges", "data": {"source": "b", "target": "c"}},
    ]


def test_DependencyDAG_from_target():
    dag = DependencyDAG.from_target()
    assert list(dag.nodes) == []

    target_path = HeartRate.target_path()
    target_path.write_text("SELECT 1")

    dag = DependencyDAG.from_target()
    assert list(dag.nodes) == [
        HeartRate.unique_name(),
        Health.unique_name(),
    ]


def test_DependencyDAG_from_model():
    dag = DependencyDAG.from_model(HeartRate)
    assert list(dag.nodes) == [
        HeartRate.unique_name(),
        Health.unique_name(),
    ]
    assert list(dag.edges) == [
        (
            Health.unique_name(),
            HeartRate.unique_name(),
        )
    ]
