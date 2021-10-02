import unittest
from pathlib import Path

from amora.compilation import Compilable
from amora.config import settings
from tests.models.health import Health
from tests.models.steps import Steps


class ModelTests(unittest.TestCase):
    def setUp(self) -> None:
        tests_path = Path(__file__).parent

        settings.dbt_models_path = tests_path.joinpath("models").as_posix()
        settings.target_path = tests_path.joinpath("target").as_posix()

    def test_target_path(self):
        path = Health.target_path()
        self.assertTrue(path.as_posix().endswith("target/health.sql"))

    def test_model_without_dependencies(self):
        self.assertEqual(Health.dependencies(), [])

    def test_model_with_dependencies(self):
        self.assertEqual(Steps.dependencies(), [Health])

    def test_model_without_source(self):
        self.assertIsNone(Health.source())

    def test_model_with_compilable_source(self):
        self.assertIsInstance(Steps.source(), Compilable)
