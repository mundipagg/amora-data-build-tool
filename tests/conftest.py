from pathlib import Path

from amora.config import settings
from amora.compilation import clean_compiled_files


def pytest_sessionstart(session):
    tests_path = Path(__file__).parent

    settings.MODELS_PATH = tests_path.joinpath("models").as_posix()
    settings.TARGET_PATH = tests_path.joinpath("target").as_posix()

    print(settings)


def pytest_sessionfinish(session, exitstatus):
    clean_compiled_files()
