from amora.dag import DependencyDAG
from amora.utils import clean_compiled_files

from tests.models.heart_rate import HeartRate


def setup_function(module):
    clean_compiled_files()


def test_DependencyDAG_root():
    dag = DependencyDAG()
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    assert dag.root() == "a"


def test_DependencyDAG_to_cytoscape_elements():
    dag = DependencyDAG()
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    assert dag.to_cytoscape_elements() == [
        {"data": {"id": "a", "label": "a"}},
        {"data": {"id": "b", "label": "b"}},
        {"data": {"id": "c", "label": "c"}},
        {"data": {"source": "a", "target": "b"}},
        {"data": {"source": "b", "target": "c"}},
    ]


def test_DependencyDAG_from_target():
    dag = DependencyDAG.from_target()
    assert list(dag.nodes) == []

    target_path = HeartRate.target_path(model_file_path=HeartRate.model_file_path())
    target_path.write_text("SELECT 1")

    dag = DependencyDAG.from_target()
    assert list(dag.nodes) == [
        "amora-data-build-tool.amora.heart_rate",
        "amora-data-build-tool.amora.health",
    ]
