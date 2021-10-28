# Amora Data Build Tool (ADBT)

 **Amora Data Build Tool** enables analysts and engineers to transform data on the data warehouse (BigQuery) 
by writing *Amora Models* that describe the data schema using Python's [PEP484 - Type Hints](https://www.python.org/dev/peps/pep-0484/) 
and select statements with [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy). Amora is able to transform Python 
code into SQL data transformation jobs that run inside the warehouse.

## When should I use? 

to-do 

## Installation

`pip install amora`

### How can I set up the right permissions in BigQuery?

1. Do this
2. Do that

Required roles:
   
- BigQuery Data Editor (`roles/bigquery.dataEditor`)
- BigQuery User (`roles/bigquery.user`)

## How to use?

Amora is packed with a **compiler** and a **runner**. The **compiler** (CLI `amora compile`) compiles Amora's python 
models into SQL statements which can be executed against the configured data warehouse using `amora materialize`.

![Amora](./docs/static/capture.gif)

The animation above displays an user inside an Amora Project using Amora's CLI `amora compile` to compile model 
files into SQL statements and creating the corresponding views/tables into the Data Warehouse (BigQuery) using `amora materialize`.

----

An Amora project is a directory with `.py` files, each containing a model definition, a subclass of `amora.models.AmoraModel`.

### Models

Models express both a data schema and a transformation statement:


```python
from datetime import datetime

from amora.compilation import Compilable
from amora.models import (
    AmoraModel,
    ModelConfig,
    PartitionConfig,
    MaterializationTypes,
)
from dbt.models.health import Health
from sqlmodel import Field, select


class HeartRate(AmoraModel, table=True):
    # model configuration
    __tablename__ = "heart_rate"
    __depends_on__ = [Health]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        partition_by=PartitionConfig(
            field="creationDate", data_type="TIMESTAMP", granularity="day"
        ),
        cluster_by="sourceName",
        tags=["daily"],
    )

    # data schema
    creationDate: datetime
    device: str
    endDate: datetime
    id: int = Field(primary_key=True)
    sourceName: str
    startDate: datetime
    unit: str
    value: float

    # transformation statement
    @classmethod
    def source(cls) -> Compilable:
        return select(
            [
                Health.creationDate,
                Health.device,
                Health.endDate,
                Health.id,
                Health.sourceName,
                Health.startDate,
                Health.unit,
                Health.value,
            ]
        ).where(Health.type == "HeartRate")

```

`HeartRate` is an Amora Model that is materialized as a [partitioned table](https://cloud.google.com/bigquery/docs/partitioned-tables) 
named `heart_rate`, with data [clustered](https://cloud.google.com/bigquery/docs/clustered-tables) by the `sourceName` column. 
The source of its data is a filter in `Health` model. Let's go through each part of the `HeartRate` model:

- `__tablename__: str`: 
- `__depends_on__: List[AmoraModel]`: 
- `__model_config__: amora.models.ModelConfig`:
- `source(cls) -> Compilable`: 
- 

### Sources

Tables/views that already exist on the Data Warehouse and shouldn't be managed by Amora. 

```python
# models/health.py
from amora.models import AmoraModel, Field
from datetime import datetime


class Health(AmoraModel):
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

```
Source models are models without a `source` implementation, no dependencies and no materialization configuration.
