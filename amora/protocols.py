from typing import Protocol, Union, runtime_checkable

from sqlalchemy.sql import Select, Selectable

Compilable = Union[Select, Selectable]


@runtime_checkable
class CompilableProtocol(Protocol):
    def source(self) -> Compilable:
        ...
