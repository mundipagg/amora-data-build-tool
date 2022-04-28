import os
from pathlib import Path
from typing import Iterable, Union

from amora.config import settings


def list_files(path: Union[str, Path], suffix: str) -> Iterable[Path]:
    yield from Path(path).rglob(f"*{suffix}")


def model_path_for_target_path(path: Path) -> Path:
    return Path(
        str(path)
        .replace(settings.TARGET_PATH.as_posix(), settings.MODELS_PATH.as_posix())
        .replace(".sql", ".py"),
    )


def target_path_for_model_path(path: Path) -> Path:
    return Path(
        str(path)
        .replace(settings.MODELS_PATH.as_posix(), settings.TARGET_PATH.as_posix())
        .replace(".py", ".sql")
    )


def clean_compiled_files() -> None:
    for sql_file in list_target_files():
        os.remove(sql_file)


def list_target_files() -> Iterable[Path]:
    return list_files(settings.TARGET_PATH, suffix=".sql")
