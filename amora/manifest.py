import hashlib
import json
from collections import defaultdict

from amora.config import settings
from amora.dag import DependencyDAG
from amora.models import list_models

BUF_SIZE = 65536


def hash_file(file_path):  # should be _Hash, but fails due to import error
    hash = hashlib.md5()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            hash.update(data)
    return hash


def generate_manifest() -> dict:
    manifest: dict = defaultdict(models={}, all_sources={})

    all_sources = []

    for model, model_file_path in list_models():
        file_stats = model_file_path.stat()
        manifest["models"][model.unique_name()] = {
            "stat": file_stats.st_mtime,
            "size": file_stats.st_size,
            "hash": hash_file(model_file_path).hexdigest(),
            "path": str(model_file_path),
            "deps": [dep for dep in DependencyDAG.from_model(model)],
        }

        all_sources.append(str(model_file_path))

    manifest["all_sources"] = all_sources

    return manifest


def save_manifest(manifest: dict):
    # TODO -> save content from manifest before save, in cases where the save fails
    with open(settings.manifest_path, "w+") as f:
        json.dump(manifest, f)


def load_manifest() -> dict:
    try:
        with open(settings.manifest_path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}