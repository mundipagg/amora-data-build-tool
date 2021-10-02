from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):
    target_schema: str = "stg-tau-rex.diogo"
    target_path: str = Path(__file__).parent.joinpath("target").as_posix()
    dbt_models_path: str = (
        Path(__file__).parent.parent.joinpath("dbt/models").as_posix()
    )


settings = Settings()
