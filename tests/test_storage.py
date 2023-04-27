from unittest.mock import patch

import pandas as pd
import pytest

from amora import storage
from amora.config import StorageCacheProviders, settings


@pytest.fixture(scope="module")
def cache():
    return storage.Cache()


@pytest.fixture(scope="module")
def cache_key():
    return storage.CacheKey(
        func_module="test_storage",
        func_name="test_Cache",
        func_checksum="checksum",
        suffix="",
    )


@pytest.fixture(scope="module")
def cache_key_with_suffix():
    return storage.CacheKey(
        func_module="test_storage",
        func_name="test_Cache",
        func_checksum="checksum",
        suffix="a_suffix",
    )


def test_cache_key_to_str(cache_key, cache_key_with_suffix):
    assert str(cache_key) == "test_storage.test_Cache.checksum"
    assert str(cache_key_with_suffix) == "test_storage.test_Cache.checksum.a_suffix"


def test_cache_key_repr(cache_key, cache_key_with_suffix):
    assert repr(cache_key) == "test_storage.test_Cache.checksum"
    assert repr(cache_key_with_suffix) == "test_storage.test_Cache.checksum.a_suffix"


def test_Cache_with_local_storage(cache, cache_key):
    df = pd.DataFrame([{"amora": 4, "storage": 2}])
    cache[cache_key] = df

    assert cache[cache_key].equals(df)


def test_Cache_with_gcs_storage(cache, cache_key):
    with patch.multiple(settings, STORAGE_CACHE_PROVIDER=StorageCacheProviders.gcs):
        df = pd.DataFrame([{"amora": 4, "storage": 2}])
        cache[cache_key] = df

        assert cache[cache_key].equals(df)


def test_cache_decorator():
    settings.STORAGE_CACHE_ENABLED = True

    @storage.cache()
    def cacheable_func():
        return pd.DataFrame([{"amora": 4, "storage": 2}])

    uncached_call_result = cacheable_func()
    cached_call_result = cacheable_func()

    assert uncached_call_result.equals(cached_call_result)


def test_cache_decorator_with_suffix_function():
    settings.STORAGE_CACHE_ENABLED = True

    @storage.cache(suffix=lambda arg1, arg2: f"{arg1}_{arg2}")
    def cacheable_func(arg1, arg2):
        return pd.DataFrame([{"amora": arg1, "storage": arg2}])

    uncached_call_result = cacheable_func(4, 2)
    cached_call_result = cacheable_func(4, 2)

    assert uncached_call_result.equals(cached_call_result)


def test_cache_decorator_can_be_disabled():
    settings.STORAGE_CACHE_ENABLED = False

    with patch("amora.storage.CACHE") as CACHE:

        @storage.cache()
        def cacheable_func():
            return pd.DataFrame([{"amora": 4, "storage": 2}])

        cacheable_func()
        cacheable_func()

        assert not CACHE.called
