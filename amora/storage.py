from amora.config import settings
from amora.models import create_engine, MetaData

local_engine = create_engine(
    f"sqlite:///{settings.SQLITE_FILE_PATH}", echo=True
)
local_metadata = MetaData(schema=None)
