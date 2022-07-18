from abc import ABC
from typing import Callable, Union

import pandas as pd
from pydantic import BaseModel


class ViewConfig(ABC):
    pass


class PieChart(ViewConfig, BaseModel):
    values: str
    names: str


class BarChart(ViewConfig, BaseModel):
    x_func: Callable[[pd.DataFrame], str] = lambda data: data["x"]
    y_func: Callable[[pd.DataFrame], str] = lambda data: data["y"]


class BigNumber(ViewConfig, BaseModel):
    value_func: Callable[[pd.DataFrame], str] = lambda data: data["total"][0]


class Table(ViewConfig, BaseModel):
    title: Union[str, None] = None


class Visualization:
    """
    The Amora visual representation of a `pandas.DataFrame`
    """

    def __init__(self, data: pd.DataFrame, config: ViewConfig):
        self.data = data
        self.config = config

    def __str__(self):
        return self.data.to_markdown()
