import hashlib
import json
from collections import defaultdict
from pathlib import Path

from _hashlib import HASH

from amora.config import settings
from amora.dag import DependencyDAG
from amora.models import list_models

BUF_SIZE = 65536


def hash_file(file_path: Path) -> HASH:
    hash = hashlib.md5()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            hash.update(data)
    return hash


def generate_manifest() -> dict:
    manifest: dict = defaultdict(models={})

    dag = DependencyDAG.from_project()

    for model, model_file_path in list_models():
        file_stats = model_file_path.stat()
        model_unique_name = model.unique_name()
        manifest["models"][model_unique_name] = {
            "stat": file_stats.st_mtime,
            "size": file_stats.st_size,
            "hash": hash_file(model_file_path).hexdigest(),
            "path": str(model_file_path),
            "deps": [dep for dep in dag.get_all_dependencies(source=model_unique_name)],
        }

    return manifest


def save_manifest(manifest: dict):
    with open(settings.manifest_path, "w+") as f:
        json.dump(manifest, f)


def load_manifest() -> dict:
    try:
        with open(settings.manifest_path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
