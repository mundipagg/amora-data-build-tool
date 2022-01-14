from typing import List

import pytest
from _pytest.fixtures import SubRequest
from amora.tests.assertions import _REGISTRY


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
    for test, results in _REGISTRY.items():
        total_bytes = sum(r.total_bytes for r in results)
        total_cost = sum(r.estimated_cost for r in results)
        print(
            f"🤑 {test} :: Total bytes: {total_bytes} :: Estimated cost: ${total_cost}"
        )


@pytest.fixture(autouse=True, scope="session")
def amora_test_environment(request: SubRequest) -> None:
    """
    Ensure that everything that Amora needs is loaded and has its testing environment setup.

    """
    setup_test_environement()
    request.addfinalizer(teardown_test_environment)
