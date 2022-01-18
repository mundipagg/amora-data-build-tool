import os
from uuid import uuid4
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Tuple

from pydantic import BaseSettings

ROOT_PATH = Path(__file__).parent.parent
AMORA_MODULE_PATH = ROOT_PATH.joinpath("amora")

_Width = float
_Height = float


class Settings(BaseSettings):
    TARGET_PROJECT: str
    TARGET_SCHEMA: str
    TARGET_PATH: Path
    MODELS_PATH: Path

    SQLITE_FILE_PATH: Path = Path(
        NamedTemporaryFile(suffix="amora-sqlite.db", delete=False).name
    )

    CLI_CONSOLE_MAX_WIDTH: int = 160
    CLI_MATERIALIZATION_DAG_FIGURE_SIZE: Tuple[_Width, _Height] = (32, 32)

    GCP_BIGQUERY_ON_DEMAND_COST_PER_TERABYTE_IN_USD: float = 5.0
    GCP_BIGQUERY_ACTIVE_STORAGE_COST_PER_GIGABYTE_IN_USD: float = 0.020

    MONEY_DECIMAL_PLACES: int = 4

    TEST_RUN_ID: str = (
        os.getenv("PYTEST_XDIST_TESTRUNUID") or f"amora-{uuid4().hex}"
    )

    class Config:
        env_prefix = "AMORA_"


settings = Settings()
