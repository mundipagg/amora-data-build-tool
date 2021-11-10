from typing import List

import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(session, config, items: List[pytest.Item]) -> None:
    print(session, config, items)
    return


@pytest.fixture(autouse=True, scope="session")
def amora_test_environment(request) -> None:
    print(request)
    return


@pytest.fixture(scope="function")
def models(*args, **kwargs):
    return {"foo": "bar"}
