from typing import Protocol, runtime_checkable

from amora.types import Compilable


@runtime_checkable
class CompilableProtocol(Protocol):
    def source(self) -> Compilable:
        ...
