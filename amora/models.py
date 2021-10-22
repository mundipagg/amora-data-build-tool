from inspect import getfile
import os
from dataclasses import dataclass
from enum import Enum, auto
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
from typing import List, Optional, Iterable, Union, Type

from dbt.adapters.bigquery.impl import PartitionConfig
from sqlalchemy import Table, MetaData
from sqlmodel import SQLModel
from sqlmodel.main import default_registry
from sqlmodel.sql.expression import Select, SelectOfScalar
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.decl_api import DeclarativeMeta


from amora.config import settings

Compilable = Union[Select, SelectOfScalar]
PartitionConfig = PartitionConfig


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class MaterializationTypes(AutoName):
    ephemeral = auto()
    view = auto()
    table = auto()


@dataclass
class ModelConfig:
    materialized: MaterializationTypes = MaterializationTypes.view
    partition_by: Optional[PartitionConfig] = None
    cluster_by: Optional[Union[str, List[str]]] = None
    tags: Optional[List[str]] = None


def list_files(path: Union[str, Path], suffix: str) -> Iterable[Path]:
    for root, _dir, files in os.walk(path):
        for file in files:
            if not file.endswith(suffix):
                continue

            yield Path(root, file)


def list_model_files() -> Iterable[Path]:
    return list_files(settings.DBT_MODELS_PATH, suffix=".py")


def list_target_files() -> Iterable[Path]:
    return list_files(settings.TARGET_PATH, suffix=".sql")


# todo: Como defino "Objeto que tenha 2 atributos: `source` e `output`" ?
def is_py_model(obj) -> bool:
    return hasattr(obj, "source") and hasattr(obj, "output")


# registry = registry()

metadata = MetaData(schema=settings.TARGET_SCHEMA)


class AmoraModel(SQLModel):
    __depends_on__: List["AmoraModel"] = []
    __model_config__ = ModelConfig(materialized="view")
    __table_args__ = {"extend_existing": True}
    metadata = metadata

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
    def target_path(cls, model_file_path: Union[str, Path]) -> Path:
        # {settings.dbt_models_path}/a_model/a_model.py -> a_model/a_model.py
        strip_path = settings.DBT_MODELS_PATH
        relative_model_path = str(model_file_path).split(strip_path)[1][1:]
        # a_model/a_model.py -> ~/project/amora/target/a_model/a_model.sql
        target_file_path = Path(settings.TARGET_PATH).joinpath(
            relative_model_path.replace(".py", ".sql")
        )

        return target_file_path

    @classmethod
    def model_file_path(cls) -> Path:
        return Path(getfile(cls))

    @classmethod
    def is_source_model(cls, path: Path) -> bool:
        raise path == cls.target_path()

    @classmethod
    def init_from_path(cls, model_path: Path) -> Type["AmoraModel"]:
        spec = spec_from_file_location(model_path.stem, model_path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if issubclass(attr, cls):
                return attr

        return module
