import logging
import os
from enum import Enum
from uuid import uuid4
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Tuple, Dict, Optional

from pydantic import BaseSettings, AnyUrl

ROOT_PATH = Path(__file__).parent.parent
AMORA_MODULE_PATH = ROOT_PATH.joinpath("amora")

_Width = float
_Height = float


class Settings(BaseSettings):
    TARGET_PROJECT: str
    TARGET_SCHEMA: str
    TARGET_PATH: Path
    MODELS_PATH: Path

    CLI_CONSOLE_MAX_WIDTH: int = 160
    CLI_MATERIALIZATION_DAG_FIGURE_SIZE: Tuple[_Width, _Height] = (32, 32)

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


class FeatureStoreProviders(str, Enum):
    local = "local"
    gcp = "gcp"


class FeatureStoreOnlineStoreTypes(str, Enum):
    redis = "redis"
    sqlite = "sqlite"
    datastore = "datastore"


class FeatureStoreOfflineStoreTypes(str, Enum):
    bigquery = "bigquery"
    file = "file"


class FeatureStoreSettings(BaseSettings):
    REGISTRY: str = NamedTemporaryFile(
        suffix="amora-feature-store-registry", delete=False
    ).name
    REPO_PATH: str = NamedTemporaryFile(suffix="repo-path", delete=False).name
    PROVIDER: str = FeatureStoreProviders.local.value
    OFFLINE_STORE_TYPE: str = FeatureStoreOfflineStoreTypes.file.value
    OFFLINE_STORE_CONFIG: Dict[str, str] = {}

    ONLINE_STORE_TYPE: str = FeatureStoreOnlineStoreTypes.sqlite.value
    ONLINE_STORE_CONFIG: Dict[str, str] = {
        "path": Path(ROOT_PATH).joinpath("amora-online-feature-store.db").name
    }
    DEFAULT_FEATURE_TTL_IN_SECONDS: int = 3600

    class Config:
        env_prefix = "AMORA_FEATURE_STORE_"


feature_store = FeatureStoreSettings()
settings = Settings()
