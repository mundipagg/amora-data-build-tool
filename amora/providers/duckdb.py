import time
from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Row

from amora.config import settings

_engine = None

# Read more: https://duckdb.org/docs/guides/import/s3_import
# Read more: GCS<->S3 Interoperability https://cloud.google.com/storage/docs/interoperability
_ENGINE_START_SQL_COMMANDS = [
    "INSTALL httpfs;",
    "LOAD httpfs;",
    "SET s3_endpoint='storage.googleapis.com';",
    "SET s3_region='auto';",
    f"SET s3_access_key_id='{settings.GCP_HMAC_ACCESS_KEY}';",
    f"SET s3_secret_access_key='{settings.GCP_HMAC_SECRET_KEY}';",
]


@dataclass
class RunResult:
    query: str
    rows: Iterable[Row]
    execution_time_in_ms: float
    schema: Optional[dict] = None

    @property
    def estimated_cost(self):
        raise NotImplementedError


def get_engine() -> Engine:
    global _engine

    if _engine is not None:
        return _engine

    _engine = create_engine(settings.DUCKDB_ENGINE_URL)
    for sql_cmd in _ENGINE_START_SQL_COMMANDS:
        _engine.execute(sql_cmd)

    return _engine


def run(statement: str) -> RunResult:
    started_at = time.perf_counter()
    result = get_engine().execute(statement)
    finished_at = time.perf_counter()

    rows = result.fetchall()

    return RunResult(
        rows=rows,
        execution_time_in_ms=(finished_at - started_at) * 1000,
        query=statement,
    )


def register_dataframe(df: pd.DataFrame, name: str) -> None:
    get_engine().execute("register", (name, df))


# register_dataframe(df, name) -> Type[AmoraModel]
