from datetime import datetime

from sqlalchemy import TIMESTAMP, Float, Integer, String

from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig


class Health(AmoraModel):
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        description="Health data exported by the Apple Health App",
    )

    id: int = Field(Integer, primary_key=True, doc="Identificador único da medida")
    type: str = Field(String, doc="Tipo da métrica coletada")
    sourceName: str = Field(String, doc="Origem dos dados")
    sourceVersion: str = Field(String, doc="Versão da origem de dados")
    unit: str = Field(String, doc="Unidade de medida")
    value: float = Field(Float, doc="Valor observado")
    device: str = Field(String, doc="Dispositivo de origem dos dados")
    creationDate: datetime = Field(TIMESTAMP, doc="Data de inserção dos dados")
    startDate: datetime = Field(TIMESTAMP, doc="Data do início da medida")
    endDate: datetime = Field(TIMESTAMP, doc="Data do fim da medida")
