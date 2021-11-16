from typing import List

import pytest
from _pytest.fixtures import SubRequest


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(
    session, config, items: List[pytest.Item]
) -> None:
    return


def setup_test_environement():
    print(
        """
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A amora adoça mais na boca de quem namora. 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Mantido com <3 por `RexData@Stone`
"""
    )


def teardown_test_environment():
    print("todo: Print table with total bytes billed, query time, etc")


@pytest.fixture(autouse=True, scope="session")
def amora_test_environment(request: SubRequest) -> None:
    """
    Ensure that everything that Amora needs is loaded and has its testing environment setup.

    """
    setup_test_environement()
    request.addfinalizer(teardown_test_environment)
