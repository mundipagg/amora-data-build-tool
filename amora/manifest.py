import hashlib
import json
from os.path import exists
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

from _hashlib import HASH
from pydantic import BaseModel

from amora.config import settings
from amora.dag import DependencyDAG
from amora.models import Model, amora_model_from_name_list, list_models

BUF_SIZE = 65536


class ModelMetadata(BaseModel):
    stat: float
    size: float
    hash: str
    path: str
    deps: list


class Manifest(BaseModel):
    models: Dict[str, ModelMetadata]

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
            json.dump(self.dict(), f, indent=2)

    def get_models_to_compile(
        self: "Manifest", previous_manifest: "Manifest"
    ) -> Set[Tuple[Model, Path]]:
        models_to_compile = set()
        deps_names_to_compile: Set = set()

        for model, model_file_path in list_models():
            model_unique_name = model.unique_name()

            model_current_manifest = self.models[model_unique_name]
            model_previous_manifest = previous_manifest.models.get(
                model_unique_name
            )  # model could not exist in previous

            compile_model = not model_previous_manifest or (
                model_current_manifest.size != model_previous_manifest.size
                or model_current_manifest.deps != model_previous_manifest.deps
                or (
                    model_current_manifest.stat > model_previous_manifest.stat
                    and (
                        not exists(model.target_path())
                        or model_current_manifest.hash != model_previous_manifest.hash
                    )
                )
            )

            if compile_model:
                models_to_compile.add((model, model_file_path))
                deps_names_to_compile = deps_names_to_compile.union(
                    model_current_manifest.deps
                )

        deps_to_compile = amora_model_from_name_list(deps_names_to_compile)
        models_to_compile = models_to_compile.union(deps_to_compile)

        return models_to_compile


def hash_file(file_path: Path) -> HASH:
    hash = hashlib.md5()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            hash.update(data)
    return hash
