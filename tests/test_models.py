import inspect

from amora.compilation import compile_statement

from tests.models.health import Health
from tests.models.steps import Steps


def test_target_path():
    path = Health.target_path(model_file_path=inspect.getfile(Health))
    assert path.as_posix().endswith("target/health.sql")


def test_model_without_dependencies():
    assert Health.dependencies() == []


def test_model_with_dependencies():
    assert Steps.dependencies() == [Health]


def test_model_without_source():
    assert Health.source() is None


def test_model_with_compilable_source():
    assert Steps.source() is not None
    assert isinstance(compile_statement(Steps.source()), str)
