from collections import defaultdict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .contentio import _IOContext

__all__: list[str] = [
    "INPUT_MAP",
    "OUTPUT_MAP",
    "DATATYPE_REGISTRY",
    "IOCONTEXT_MAP",
]


class InputMap(dict[str, Callable]):
    ...


class OutputMap(dict[str, Callable]):
    ...


class DatatypeRegistry(dict[str, Any]):
    ...


class IOContextMap(defaultdict):
    ...


INPUT_MAP = InputMap()
OUTPUT_MAP = OutputMap()
DATATYPE_REGISTRY = DatatypeRegistry()
IOCONTEXT_MAP = IOContextMap(list)
