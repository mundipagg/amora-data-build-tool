from typing import Optional

from pydantic import BaseSettings
from aiologger.loggers.json import JsonLogger


class Settings(BaseSettings):
    DISCORD_APPLICATION_ID: Optional[str]
    DISCORD_PUBLIC_KEY: Optional[str]
    DISCORD_PERMISSIONS_INTEGER: int = 377960384576
    DISCORD_TOKEN: Optional[str]
    DISCORD_SERVER: Optional[str]
    DISCORD_SERVER_ID: Optional[int]

    class Config:
        env_prefix = 'AMORA_MESSAGING_'


settings = Settings()

logger = JsonLogger.with_default_handlers(name='amora-discord-bot')