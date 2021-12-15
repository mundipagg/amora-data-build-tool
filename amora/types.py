from typing import Union

from sqlalchemy.sql import Select, Selectable

Compilable = Union[Select, Selectable]
