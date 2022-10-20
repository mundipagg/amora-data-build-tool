from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from sqlalchemy import Integer, String, func, select
from sqlalchemy_bigquery.base import BQArray

from amora.compilation import compile_statement
from amora.models import AmoraModel, amora_model_for_path
from amora.providers.bigquery import fixed_unnest

from tests.models.array_repeated_fields import ArrayRepeatedFields


def test_amora_model_for_path_with_invalid_file_path_type():
    with NamedTemporaryFile(suffix=".sql") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=rf".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_file_path():
    invalid_model_path = Path("not-a-real-file-name")
    with pytest.raises(ValueError, match=rf".*{invalid_model_path}.*"):
        amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_python_file_path():
    with NamedTemporaryFile(suffix=".py") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=rf".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_invalid_bytecode_python_file_path():
    with NamedTemporaryFile(suffix=".pyc") as fp:
        invalid_model_path = Path(fp.name)
        with pytest.raises(ValueError, match=rf".*{invalid_model_path}.*"):
            amora_model_for_path(path=invalid_model_path)


def test_amora_model_for_path_with_valid_python_file_path():
    model = amora_model_for_path(path=Path(__file__).parent.joinpath("models/steps.py"))
    assert issubclass(model, AmoraModel)


def test_AmoraBigQueryCompiler_array__getitem__with_integer_item():
    version = func.split("123.45", ".", type_=BQArray(String))[2]
    statement = select(func.cast(version, Integer))
    query_str = compile_statement(statement)

    assert (
        query_str
        == "SELECT CAST((split('123.45', '.'))[OFFSET(2)] AS INT64) AS `anon_1`"
    )


def test_AmoraBigQueryCompiler_array__getitem__with_function_item():
    version = func.split("123.45", ".", type_=BQArray(String))[func.safe_ordinal(2)]
    statement = select(func.cast(version, Integer))
    query_str = compile_statement(statement)

    assert (
        query_str
        == "SELECT CAST((split('123.45', '.'))[safe_ordinal(2)] AS INT64) AS `anon_1`"
    )


def test_AmoraBigQueryCompiler_visit_function_with_offset():
    offset_alias = "off_alias"
    stmt = fixed_unnest(ArrayRepeatedFields.int_arr).table_valued(
        with_offset=offset_alias
    )
    compiled = compile_statement(stmt)
    assert compiled.endswith(f"OFFSET AS {offset_alias}")


def test_AmoraBigQueryCompiler_visit_function_without_offset():
    stmt = fixed_unnest(ArrayRepeatedFields.int_arr).table_valued()
    compiled = compile_statement(stmt)

    assert compiled == "unnest(`array_repeated_fields`.`int_arr`)"
