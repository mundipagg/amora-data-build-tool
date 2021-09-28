from datetime import datetime

from sqlmodel import SQLModel, MetaData, Field


class Health(SQLModel, table=True):
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

    metadata = MetaData(schema="stg-tau-rex.diogo", quote_schema=True)


def source() -> None:
    return None


output = Health
