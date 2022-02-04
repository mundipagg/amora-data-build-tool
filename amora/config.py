import logging
import os
from enum import Enum
from uuid import uuid4
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Tuple

from pydantic import BaseSettings, AnyUrl

ROOT_PATH = Path(__file__).parent.parent
AMORA_MODULE_PATH = ROOT_PATH.joinpath("amora")

_Width = float
_Height = float


class FeatureStoreProviders(str, Enum):
    local = "local"
    gcp = "gcp"


class FeatureStoreOnlineStoreTypes(str, Enum):
    redis = "redis"
    sqllite = "sqllite"
    datastore = "datastore"


class FeatureStoreOfflineStoreTypes(str, Enum):
    bigquery = "bigquery"
    file = "file"


class Settings(BaseSettings):
    TARGET_PROJECT: str
    TARGET_SCHEMA: str
    TARGET_PATH: Path
    MODELS_PATH: Path

    CLI_CONSOLE_MAX_WIDTH: int = 160
    CLI_MATERIALIZATION_DAG_FIGURE_SIZE: Tuple[_Width, _Height] = (32, 32)

    FEATURE_STORE_REGISTRY: Path = NamedTemporaryFile(
        suffix="amora-feature-store-registry", delete=False
    )

    # https://cloud.google.com/bigquery/pricing#analysis_pricing_models
    GCP_BIGQUERY_ON_DEMAND_COST_PER_TERABYTE_IN_USD: float = 5.0
    # https://cloud.google.com/bigquery/pricing#storage
    GCP_BIGQUERY_ACTIVE_STORAGE_COST_PER_GIGABYTE_IN_USD: float = 0.020

    LOCAL_ENGINE_ECHO: bool = False
    LOCAL_ENGINE_SQLITE_FILE_PATH: Path = Path(
        NamedTemporaryFile(suffix="amora-sqlite.db", delete=False).name
    )
    LOGGER_LOG_LEVEL: int = logging.DEBUG

    MONEY_DECIMAL_PLACES: int = 4

    TEST_RUN_ID: str = os.getenv("PYTEST_XDIST_TESTRUNUID") or f"amora-{uuid4().hex}"

    class Config:
        env_prefix = "AMORA_"


settings = Settings()
