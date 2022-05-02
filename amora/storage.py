from amora.config import settings
from amora.models import MetaData, create_engine

local_engine = create_engine(
    f"sqlite:///{settings.LOCAL_ENGINE_SQLITE_FILE_PATH}",
    echo=settings.LOCAL_ENGINE_ECHO,
)
local_metadata = MetaData(schema=None)
