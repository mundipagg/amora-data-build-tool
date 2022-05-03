from enum import Enum
from typing import NamedTuple

import pandas as pd


class ViewKind(str, Enum):
    big_number = "big_number"
    bar_chart = "bar_chart"
    column_chart = "column_chart"
    line_chart = "line_chart"
    table = "table"
    pie_chart = "pie_chart"


class ViewConfig(NamedTuple):
    title: str
    kind: ViewKind = ViewKind.table


class View:
    """
    The Amora visual representation of a `pandas.DataFrame`
    """

    def __init__(self, data: pd.DataFrame, config: ViewConfig):
        self.data = data
        self.config = config

    def __str__(self):
        return self.data.to_markdown()
