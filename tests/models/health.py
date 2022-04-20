from datetime import datetime

from sqlmodel import Field

from amora.models import AmoraModel


class Health(AmoraModel, table=True):
    creationDate: datetime = Field(description="Data de inserção dos dados")
    device: str = Field(description="Dispositivo de origem dos dados")
    endDate: datetime = Field(description="Data do fim da medida")
    id: int = Field(primary_key=True, description="Identificador único da medida")
    sourceName: str = Field(description="Origem dos dados")
    sourceVersion: str = Field(description="Versão da origem de dados")
    startDate: datetime = Field(description="Data do início da medida")
    type: str = Field(description="Tipo da métrica coletada")
    unit: str = Field(description="Unidade de medida")
    value: float = Field(description="Valor observado")
