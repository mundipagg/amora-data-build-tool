from amora.config import settings
from amora.models import create_engine

local_storage = create_engine(
    f"sqlite:///{settings.SQLITE_FILE_PATH}", echo=True
)
