from pathlib import Path

from selenium.webdriver.chrome.options import Options

from amora.compilation import remove_compiled_files
from amora.config import settings


def pytest_sessionstart(session):
    tests_path = Path(__file__).parent

    settings.MODELS_PATH = tests_path.joinpath("models")
    settings.TARGET_PATH = tests_path.joinpath("target")


def pytest_sessionfinish(session, exitstatus):
    remove_compiled_files()


def pytest_setup_options():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    return options
