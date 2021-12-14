from typing import runtime_checkable, Protocol

from amora.types import Compilable


@runtime_checkable
class CompilableProtocol(Protocol):
    def source(self) -> Compilable:
        ...
