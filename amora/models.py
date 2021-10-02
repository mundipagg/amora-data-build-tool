import inspect
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Iterable, Union

from dbt.adapters.bigquery.impl import PartitionConfig
from sqlalchemy import Table, MetaData
from sqlmodel import SQLModel
from sqlalchemy.inspection import inspect


from amora.compilation import Compilable
from amora.config import settings

PartitionConfig = PartitionConfig


@dataclass
class ModelConfig:
    materialized: str = "ephemeral"
    partition_by: Optional[PartitionConfig] = None
    cluster_by: Optional[str] = None
    tags: Optional[List[str]] = None


def list_files(path: Union[str, Path], suffix: str) -> Iterable[Path]:
    for root, _dir, files in os.walk(path):
        for file in files:
            if not file.endswith(suffix):
                continue

            yield Path(root, file)


def list_model_files() -> Iterable[Path]:
    return list_files(settings.dbt_models_path, suffix=".py")


def list_target_files() -> Iterable[Path]:
    return list_files(settings.target_path, suffix=".sql")


# todo: Como defino "Objeto que tenha 2 atributos: `source` e `output`" ?
def is_py_model(obj) -> bool:
    return hasattr(obj, "source") and hasattr(obj, "output")


class AmoraModel(SQLModel):
    __depends_on__: List["AmoraModel"] = []
    __model_config__ = ModelConfig(materialized="view")
    metadata = MetaData(schema=settings.target_schema)

    @classmethod
    def dependencies(cls) -> Iterable["AmoraModel"]:
        source = cls.source()
        if source is None:
            return []

        # todo: Remover necessidade de __depends_on__ inspecionando a query e chegando ao modelo de origem
        # tables: List[Table] = source.froms

        return cls.__depends_on__

    @classmethod
    def source(cls) -> Optional[Compilable]:
        """
        Called when `amora compile` is executed, Amora will build this model
        in your data warehouse by wrapping it in a `create view as` or `create table as` statement.

        Return `None` for defining models for tables/views that already exist on the data warehouse
        and shouldn't be managed by Amora.

        Returning a `Compilable`, which is a sqlalchemy select statement
        :return:
        """
        return None

    @classmethod
    def target_path(cls) -> Path:
        # {settings.dbt_models_path}/a_model/a_model.py -> a_model/a_model.py
        strip_path = settings.dbt_models_path
        relative_model_path = str(cls.model_file_path()).split(strip_path)[1][1:]
        # a_model/a_model.py -> ~/project/amora/target/a_model/a_model.sql
        target_file_path = Path(settings.target_path).joinpath(
            relative_model_path.replace(".py", ".sql")
        )

        return target_file_path

    @classmethod
    def model_file_path(cls) -> Path:
        return Path(inspect.getfile(cls))
