import hashlib
import json
from pathlib import Path
from typing import Dict, Optional

from _hashlib import HASH
from pydantic import BaseModel

from amora.config import settings
from amora.dag import DependencyDAG
from amora.models import list_models

BUF_SIZE = 65536


class ModelMetadata(BaseModel):
    stat: float
    size: float
    hash: str
    path: str
    deps: list


class Manifest(BaseModel):
    models: dict[str, ModelMetadata]

    @classmethod
    def from_project(cls) -> "Manifest":
        dag = DependencyDAG.from_project()

        models_manifest: Dict[str, ModelMetadata] = {}

        for model, model_file_path in list_models():
            file_stats = model_file_path.stat()
            model_unique_name = model.unique_name()

            models_manifest[model_unique_name] = ModelMetadata(
                stat=file_stats.st_mtime,
                size=file_stats.st_size,
                hash=hash_file(model_file_path).hexdigest(),
                path=str(model_file_path),
                deps=[
                    dep for dep in dag.get_all_dependencies(source=model_unique_name)
                ],
            )

        return Manifest(models=models_manifest)

    @classmethod
    def load(cls) -> Optional["Manifest"]:
        try:
            with open(settings.manifest_path) as f:
                return Manifest(**(json.load(f)))
        except FileNotFoundError:
            return None

    def save(self) -> None:
        with open(settings.manifest_path, "w+") as f:
            json.dump(self.dict(), f)


def hash_file(file_path: Path) -> HASH:
    hash = hashlib.md5()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            hash.update(data)
    return hash
