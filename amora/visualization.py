from abc import ABC
from typing import Optional, Protocol, Union, runtime_checkable

import pandas as pd
from pandas import Series
from pydantic import BaseModel


@runtime_checkable
class SeriesSelectorFunc(Protocol):
    def __call__(self, data: pd.DataFrame) -> Series:
        ...


@runtime_checkable
class ValueSelectorFunc(Protocol):
    def __call__(self, data: pd.DataFrame) -> str:
        ...


class VisualizationConfig(ABC):
    pass


class PieChart(VisualizationConfig, BaseModel):
    values: str
    names: str


class _2DChart(VisualizationConfig, BaseModel):
    x_func: SeriesSelectorFunc = lambda data: data["x"]
    y_func: SeriesSelectorFunc = lambda data: data["y"]

    class Config:
        arbitrary_types_allowed = True


class BarChart(VisualizationConfig, BaseModel):
    x_func: SeriesSelectorFunc = lambda data: data["x"]
    y_func: SeriesSelectorFunc = lambda data: data["y"]

    class Config:
        arbitrary_types_allowed = True


class LineChart(_2DChart):
    pass


class Heatmap(VisualizationConfig, BaseModel):
    x_func: SeriesSelectorFunc = lambda data: data["x"]
    y_func: SeriesSelectorFunc = lambda data: data["y"]
    z_func: Optional[SeriesSelectorFunc] = None
    labels: dict = {}

    class Config:
        arbitrary_types_allowed = True


class BigNumber(VisualizationConfig, BaseModel):
    value_func: ValueSelectorFunc = lambda data: data["total"][0]

    class Config:
        arbitrary_types_allowed = True


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
