from typing import List

import dash_bootstrap_components as dbc
from pydantic import BaseSettings, validator


class DashSettings(BaseSettings):
    HTTP_HOST: str = "127.0.0.1"
    HTTP_PORT: str = "8050"
    DBC_THEME: str = "MATERIA"
    DEBUG: bool = False

    class Config:
        env_prefix = "AMORA_DASH_"

    @property
    def dbc_theme_stylesheet(self) -> str:
        return getattr(dbc.themes, self.DBC_THEME)

    @property
    def external_stylesheets(self) -> List[str]:
        return [self.dbc_theme_stylesheet, dbc.icons.FONT_AWESOME]

    @validator("DBC_THEME")
    def is_valid_dash_dbc_theme(cls, v):
        assert hasattr(dbc.themes, v)
        return v


settings = DashSettings()
