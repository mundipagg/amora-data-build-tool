from datetime import datetime

from sqlmodel import Field

from amora.models import AmoraModel


class Health(AmoraModel, table=True):
    id: int = Field(primary_key=True)
    type: str
    sourceName: str
    sourceVersion: str
    unit: str
    creationDate: datetime
    startDate: datetime
    endDate: datetime
    value: float
    device: str
