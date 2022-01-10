from pathlib import Path
from typing import Tuple

from pydantic import BaseSettings

ROOT_PATH = Path(__file__).parent.parent
AMORA_MODULE_PATH = ROOT_PATH.joinpath("amora")

_Width = float
_Height = float


class Settings(BaseSettings):
    TARGET_PROJECT: str
    TARGET_SCHEMA: str
    TARGET_PATH: Path = AMORA_MODULE_PATH.joinpath("target")
    MODELS_PATH: Path = ROOT_PATH.joinpath("examples/amora_project/models")
    SEEDS_PATH: Path = ROOT_PATH.joinpath("examples/amora_project/seeds")
    GCP_POLLING_INTERVAL_IN_SECONDS: float = 3.0

    CLI_CONSOLE_MAX_WIDTH: int = 160
    CLI_MATERIALIZATION_DAG_FIGURE_SIZE: Tuple[_Width, _Height] = (32, 32)

    class Config:
        env_prefix = "AMORA_"


settings = Settings()
