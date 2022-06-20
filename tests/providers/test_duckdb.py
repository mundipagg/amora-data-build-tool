import datetime
from pathlib import Path

import pandas as pd
import pytest

from amora.providers.duckdb import register_dataframe, run


@pytest.fixture
def alltypes_rows():
    return [
        (
            4,
            True,
            0,
            0,
            0,
            0,
            0.0,
            0.0,
            b"03/01/09",
            b"0",
            datetime.datetime(2009, 3, 1, 0, 0),
        ),
        (
            5,
            False,
            1,
            1,
            1,
            10,
            1.100000023841858,
            10.1,
            b"03/01/09",
            b"1",
            datetime.datetime(2009, 3, 1, 0, 1),
        ),
        (
            6,
            True,
            0,
            0,
            0,
            0,
            0.0,
            0.0,
            b"04/01/09",
            b"0",
            datetime.datetime(2009, 4, 1, 0, 0),
        ),
        (
            7,
            False,
            1,
            1,
            1,
            10,
            1.100000023841858,
            10.1,
            b"04/01/09",
            b"1",
            datetime.datetime(2009, 4, 1, 0, 1),
        ),
        (
            2,
            True,
            0,
            0,
            0,
            0,
            0.0,
            0.0,
            b"02/01/09",
            b"0",
            datetime.datetime(2009, 2, 1, 0, 0),
        ),
        (
            3,
            False,
            1,
            1,
            1,
            10,
            1.100000023841858,
            10.1,
            b"02/01/09",
            b"1",
            datetime.datetime(2009, 2, 1, 0, 1),
        ),
        (
            0,
            True,
            0,
            0,
            0,
            0,
            0.0,
            0.0,
            b"01/01/09",
            b"0",
            datetime.datetime(2009, 1, 1, 0, 0),
        ),
        (
            1,
            False,
            1,
            1,
            1,
            10,
            1.100000023841858,
            10.1,
            b"01/01/09",
            b"1",
            datetime.datetime(2009, 1, 1, 0, 1),
        ),
    ]


def test_register_dataframe():
    register_dataframe(
        name="a_dataframe",
        df=pd.DataFrame(columns=["x", "y"], data=[{"x": n, "y": n} for n in range(3)]),
    )

    # def register_
    # class ADataframe(AmoraModel, table=True):
    #     x: int
    #     y: int
    #
    #     @classmethod
    #     def source(cls):
    #         return pd.DataFrame(columns=["x", "y"], data=[{"x": n, "y": n} for n in range(3)])

    # register_dataframe(ADataframe)

    assert run("SELECT * FROM a_dataframe").rows == [
        (0, 0),
        (1, 1),
        (2, 2),
    ]


def test_read_parquet_from_file(alltypes_rows):
    parquet_file_path = Path(__file__).parent.parent.joinpath(
        "assets/alltypes_plain.parquet"
    )
    result = run(f"SELECT * FROM read_parquet('{parquet_file_path.as_posix()}')")
    assert result.rows == alltypes_rows


def test_read_parquet_from_gcs(alltypes_rows):
    result = run(
        "SELECT * FROM read_parquet('s3://amora-duckdb/alltypes_plain.parquet')"
    )
    assert result.rows == alltypes_rows


def test_read_parquet_from_http(alltypes_rows):
    # fixme: replace me with a mundipagg/amora-data-build-tool url
    parquet_file_url = "https://raw.githubusercontent.com/apache/parquet-testing/master/data/alltypes_plain.parquet"
    result = run(f"SELECT * FROM '{parquet_file_url}'")

    assert result.rows == alltypes_rows
