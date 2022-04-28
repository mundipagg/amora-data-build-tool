from datetime import datetime

from sqlalchemy import TIMESTAMP
from sqlmodel import Field

from amora.models import AmoraModel, Column, MaterializationTypes, ModelConfig


class Health(AmoraModel, table=True):
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        description="Health data exported by the Apple Health App",
    )

    id: int = Field(primary_key=True, description="Identificador único da medida")
    type: str = Field(description="Tipo da métrica coletada")
    sourceName: str = Field(description="Origem dos dados")
    sourceVersion: str = Field(description="Versão da origem de dados")
    unit: str = Field(description="Unidade de medida")
    value: float = Field(description="Valor observado")
    device: str = Field(description="Dispositivo de origem dos dados")
    creationDate: datetime = Field(
        description="Data de inserção dos dados", sa_column=Column(TIMESTAMP)
    )
    startDate: datetime = Field(
        description="Data do início da medida", sa_column=Column(TIMESTAMP)
    )
    endDate: datetime = Field(
        description="Data do fim da medida", sa_column=Column(TIMESTAMP)
    )
