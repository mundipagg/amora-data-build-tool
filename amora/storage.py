import hashlib
import inspect
from collections import UserDict
from functools import wraps
from typing import Callable, NamedTuple, Union

import pandas as pd

from amora import logger
from amora.config import StorageCacheProviders, settings
from amora.models import MetaData, create_engine

local_engine = create_engine(
    f"sqlite:///{settings.LOCAL_ENGINE_SQLITE_FILE_PATH}",
    echo=settings.LOCAL_ENGINE_ECHO,
)
local_metadata = MetaData(schema=None)


class CacheKey(NamedTuple):
    """
    E.g:

    ```python
    CacheKey(
        func_module="amora.questions",
        func_name="answer_df",
        func_checksum="e6ccc38abffd7081822da108971e9d9c",
        suffix="how_many_data_points_where_acquired",
    )

    "amora.questions.answer_df.e6ccc38abffd7081822da108971e9d9c.how_many_data_points_where_acquired"
    ```

    """

    func_module: str
    func_name: str
    func_checksum: str
    suffix: str

    def __repr__(self):
        return str(self)

    def __str__(self):
        return ".".join(value for value in self if value)


class Cache(UserDict):
    """
    A `pandas.DataFrame` cache provider that uses the local File System or GCS
    """

    file_suffix: str = ".parquet"

    @property
    def type_(self) -> StorageCacheProviders:
        return settings.STORAGE_CACHE_PROVIDER

    def filepath_for_key(self, key: CacheKey) -> str:
        if self.type_ is StorageCacheProviders.gcs:
            blob_name = f"{key}{self.file_suffix}"
            gs_url = f"gs://{settings.STORAGE_GCS_BUCKET_NAME}/{blob_name}"
            return gs_url
        elif self.type_ is StorageCacheProviders.local:
            blob_name = f"{key}{self.file_suffix}"
            return settings.STORAGE_LOCAL_CACHE_PATH.joinpath(blob_name).as_posix()
        else:
            raise NotImplementedError

    @logger.log_execution()
    def __setitem__(self, key: CacheKey, value: pd.DataFrame):
        value.to_parquet(
            self.filepath_for_key(key), engine=settings.STORAGE_PARQUET_ENGINE
        )

    @logger.log_execution()
    def __getitem__(self, item: CacheKey) -> pd.DataFrame:
        try:
            return pd.read_parquet(self.filepath_for_key(item))
        except FileNotFoundError as e:
            raise KeyError from e


CACHE = Cache()
Cacheable = Callable[..., pd.DataFrame]


def cache(suffix: Union[Callable[..., str], None] = None):
    """
    Caches a `amora.storage.Cacheable` into the provider selected
    at `setting.STORAGE_CACHE_PROVIDER`.

    To disable the cache, set the env var `AMORA_STORAGE_CACHE_ENABLED` to false.

    ```python
    from datetime import datetime
    from time import sleep

    import pandas as pd

    from amora.storage import cache


    @cache()
    def a_slow_to_build_dataframe():
        sleep(3)
        return pd.DataFrame([{"a": 4, "b": 2}])


    t0 = datetime.now()
    print(a_slow_to_build_dataframe())
    print("Uncached execution:", datetime.now() - t0)

    t0 = datetime.now()
    print(a_slow_to_build_dataframe())
    print("Cached execution:", datetime.now() - t0)
    ```

    ```
       a  b
    0  4  2
    Uncached execution: 0:00:03.011570

       a  b
    0  4  2
    Cached execution: 0:00:00.005567
    ```

    If the cached function expects arguments, a `suffix` function should be provided
    to the `cache` decorator. The function will be called with the same arguments a the
    cached function, and a `str` should be returned.

    ```python
    @cache(suffix=lambda arg1, arg2: f"{arg1}_{arg2}")
    def cacheable_func(arg1, arg2):
        return pd.DataFrame([{"amora": arg1, "storage": arg2}])


    cacheable_func(4, 2)
    # CacheKey -> foo_module.cacheable_func.e6ccc38abffd7081822da108971e9d9c.4_2

    cacheable_func(5, 3)
    # CacheKey -> foo_module.cacheable_func.e6ccc38abffd7081822da108971e9d9c.5_3
    ```

    The key function can also be used to implement a cache TTL:

    ```python
    from datetime import date


    @cache(suffix=lambda: date.today().isoformat())
    def im_cached_for_one_day():
        sleep(3)
        return pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    ```
    """

    def wrapper(fn: Cacheable):
        func_checksum = hashlib.md5(inspect.getsource(fn).encode("utf-8")).hexdigest()

        @wraps(fn)
        def decorator(*args, **kwargs):
            if not settings.STORAGE_CACHE_ENABLED:
                return fn(*args, **kwargs)

            cache_key = CacheKey(
                func_module=fn.__module__,
                func_name=fn.__name__,
                func_checksum=func_checksum,
                suffix=suffix(*args, **kwargs) if suffix else "",
            )

            try:
                return CACHE[cache_key]
            except KeyError:
                result = fn(*args, **kwargs)
                CACHE[cache_key] = result
                return result

        return decorator

    return wrapper
