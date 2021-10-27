from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from amora.compilation import amora_model_for_path
from amora.models import AmoraModel


def test_amora_model_for_path_with_invalid_file_path_type():
    with NamedTemporaryFile(suffix=".sql") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=fr".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_file_path():
    invalid_model_path = Path("not-a-real-file-name")
    with pytest.raises(ValueError, match=fr".*{invalid_model_path}.*"):
        amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_python_file_path():
    with NamedTemporaryFile(suffix=".py") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=fr".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_bytecode_python_file_path():
    with NamedTemporaryFile(suffix=".pyc") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=fr".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_valid_python_file_path():
    model_path = Path("models/steps.py")
    model = amora_model_for_path(path=model_path)
    assert issubclass(model, AmoraModel)
