import logging
import os
from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile, mkdtemp
from typing import Optional, Tuple
from uuid import uuid4

from pydantic import BaseSettings, root_validator, validator

ROOT_PATH = Path(__file__).parent.parent
AMORA_MODULE_PATH = ROOT_PATH.joinpath("amora")

_Width = float
_Height = float


class StorageCacheProviders(str, Enum):
    local = "local"
    gcs = "gcs"


class Settings(BaseSettings):
    TARGET_PROJECT: str
    TARGET_SCHEMA: str

    PROJECT_PATH: Path
    DASHBOARDS_PATH: Optional[Path]
    MODELS_PATH: Optional[Path]
    TARGET_PATH: Optional[Path]

    CLI_CONSOLE_MAX_WIDTH: int = 160
    CLI_MATERIALIZATION_DAG_FIGURE_SIZE: Tuple[_Width, _Height] = (32, 32)

    # https://cloud.google.com/bigquery/pricing#analysis_pricing_models
    GCP_BIGQUERY_ON_DEMAND_COST_PER_TERABYTE_IN_USD: float = 5.0
    # https://cloud.google.com/bigquery/pricing#storage
    GCP_BIGQUERY_ACTIVE_STORAGE_COST_PER_GIGABYTE_IN_USD: float = 0.020

    GCP_BIGQUERY_DEFAULT_LIMIT_SIZE: int = 1000

    LOCAL_ENGINE_ECHO: bool = False
    LOCAL_ENGINE_SQLITE_FILE_PATH: Path = Path(
        NamedTemporaryFile(suffix="amora-sqlite.db", delete=False).name
    )
    STORAGE_CACHE_ENABLED: bool = False
    STORAGE_CACHE_PROVIDER: StorageCacheProviders = StorageCacheProviders.local
    STORAGE_GCS_BUCKET_NAME: str = "amora-storage"
    STORAGE_LOCAL_CACHE_PATH: Path = Path(mkdtemp())
    STORAGE_PARQUET_ENGINE: str = "pyarrow"

    LOGGER_LOG_LEVEL: int = logging.DEBUG

    MONEY_DECIMAL_PLACES: int = 4

    TEST_RUN_ID: str = os.getenv("PYTEST_XDIST_TESTRUNUID") or f"amora-{uuid4().hex}"

    class Config:
        env_prefix = "AMORA_"

    @root_validator
    def compute_MODELS_PATH(cls, values: dict) -> dict:
        if values["MODELS_PATH"] is not None:
            return values

        values["MODELS_PATH"] = values["PROJECT_PATH"].joinpath("models")
        return values

    @root_validator
    def compute_DASHBOARDS_PATH(cls, values: dict) -> dict:
        if values["DASHBOARDS_PATH"] is not None:
            return values

        values["DASHBOARDS_PATH"] = values["PROJECT_PATH"].joinpath("dashboards")
        return values

    @root_validator
    def compute_TARGET_PATH(cls, values: dict) -> dict:
        if values["TARGET_PATH"] is not None:
            return values

        target_path: Path = values["PROJECT_PATH"].joinpath(".target")
        if not target_path.exists():
            target_path.mkdir(exist_ok=False)

        values["TARGET_PATH"] = target_path

        return values

    @validator("PROJECT_PATH")
    def project_path_is_a_valid_path(cls, v: Path) -> Path:
        if not v.is_dir():
            raise ValueError(f"{v.as_posix()} must be a valid directory")
        return v

    @property
    def models_path(self) -> Path:
        assert isinstance(self.MODELS_PATH, Path)
        return self.MODELS_PATH

    @property
    def target_path(self) -> Path:
        assert isinstance(self.TARGET_PATH, Path)
        return self.TARGET_PATH

    @property
    def dashboards_path(self) -> Path:
        assert isinstance(self.DASHBOARDS_PATH, Path)
        return self.DASHBOARDS_PATH


settings = Settings()
