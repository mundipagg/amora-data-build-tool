from abc import ABC
from typing import Any, Protocol, Union

import pandas as pd
from pandas import Series
from pydantic import BaseModel


class SeriesSelectorFunc(Protocol):
    def __call__(self, df: pd.DataFrame) -> Series[Any]:
        pass


class ValueSelectorFunc(Protocol):
    def __call__(self, df: pd.DataFrame) -> str:
        pass


class VisualizationConfig(ABC):
    pass


class PieChart(VisualizationConfig, BaseModel):
    values: str
    names: str


class _2DChart(VisualizationConfig, BaseModel):
    x_func: SeriesSelectorFunc = lambda data: data["x"]
    y_func: SeriesSelectorFunc = lambda data: data["y"]


class BarChart(VisualizationConfig, BaseModel):
    x_func: SeriesSelectorFunc = lambda data: data["x"]
    y_func: SeriesSelectorFunc = lambda data: data["y"]


class LineChart(_2DChart):
    pass


class BigNumber(VisualizationConfig, BaseModel):
    value_func: ValueSelectorFunc = lambda data: data["total"][0]


class Table(VisualizationConfig, BaseModel):
    title: Union[str, None] = None


class Visualization:
    """
    The Amora visual representation of a `pandas.DataFrame`
    """

    def __init__(self, data: pd.DataFrame, config: VisualizationConfig):
        self.data = data
        self.config = config

    def __str__(self):
        return self.data.to_markdown()
