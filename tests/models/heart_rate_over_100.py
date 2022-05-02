from datetime import datetime

from sqlalchemy import TIMESTAMP

from amora.models import AmoraModel, Column, Field, MaterializationTypes, ModelConfig

from tests.models.heart_rate import HeartRate


class HeartRateOver100(AmoraModel, table=True):
    __tablename__ = "heart_rate_over_100"
    __depends_on__ = [HeartRate]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.view,
        labels={"freshness": "daily"},
    )

    unit: str = Field(description="Unidade de medida")
    value: float = Field(description="Valor observado")
    creationDate: datetime = Field(
        description="Data de inserção dos dados", sa_column=Column(TIMESTAMP)
    )
    id: int = Field(primary_key=True, description="Identificador único da medida")
