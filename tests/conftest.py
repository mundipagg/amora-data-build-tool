from pathlib import Path

from amora.config import settings
from amora.utils import clean_compiled_files


def pytest_sessionstart(session):
    tests_path = Path(__file__).parent

    settings.MODELS_PATH = tests_path.joinpath("models")
    settings.TARGET_PATH = tests_path.joinpath("target")


def pytest_sessionfinish(session, exitstatus):
    clean_compiled_files()
