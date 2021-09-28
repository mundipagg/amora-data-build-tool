import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Iterable, Union

from dbt.adapters.bigquery.impl import PartitionConfig

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
