from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict

from pydantic import BaseSettings

from amora.config import ROOT_PATH


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


settings = FeatureStoreSettings()