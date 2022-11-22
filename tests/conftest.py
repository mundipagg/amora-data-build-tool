from pathlib import Path

from selenium.webdriver.chrome.options import Options

from amora.config import settings
from amora.utils import clean_compiled_files


def pytest_sessionstart(session):
    tests_path = Path(__file__).parent

    settings.MODELS_PATH = tests_path.joinpath("models")
    settings.TARGET_PATH = tests_path.joinpath("target")


def pytest_sessionfinish(session, exitstatus):
    clean_compiled_files()


def pytest_setup_options():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    return options
