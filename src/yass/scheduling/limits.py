from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from rich.pretty import pprint

from yass.scheduling import BaseLimit

if TYPE_CHECKING:
    from yass.storages import BaseBufferableStorage


__all__ = [
    "CountLimit",
    "MemoryLimit",
    "TimeLimit",
    "UnableBufferize",
    "get_allowed_limits",
    "limit",
    "setup_limit",
]


_LIMIT_MAP = dict()

_LIMIT_TYPE = Literal["unable", "memory", "count", "time"]


def limit(limit_type: str) -> Callable[[type[BaseLimit]], type[BaseLimit]]:
    def registred_limit(cls: type[BaseLimit]) -> type[BaseLimit]:
        _LIMIT_MAP[limit_type] = cls
        return cls

    return registred_limit


def setup_limit(
    limit_type: str, capacity: Any, storage: BaseBufferableStorage
) -> BaseLimit:
    limit_instance: BaseLimit = _LIMIT_MAP[limit_type](storage, capacity)
    return limit_instance


def get_allowed_limits() -> list[tuple[str, BaseLimit]]:
    return list((k, v) for k, v in _LIMIT_MAP.items())


@limit("time")
class TimeLimit(BaseLimit):
    def __init__(self, storage: BaseBufferableStorage, capacity: int):
        self.storage: BaseBufferableStorage = storage
        self.capacity = timedelta(seconds=capacity)

    def is_overflowed(self):
        current_timestamp = datetime.now()
        return self.capacity < (current_timestamp - self.storage.last_commit)


@limit("memory")
class MemoryLimit(BaseLimit):
    def __init__(self, storage: BaseBufferableStorage, capacity: int | float):
        self.storage: BaseBufferableStorage = storage
        self.capacity: int | float = capacity

    def is_overflowed(self) -> bool:
        pprint(
            f"Текущий размер занятой буферами памяти составляет {self.storage.mem_alloc} MB при ограничении в {self.capacity} MB."
        )
        return self.capacity < self.storage.mem_alloc


@limit("count")
class CountLimit(BaseLimit):
    def __init__(self, storage: BaseBufferableStorage, capacity: int):
        self.storage = storage
        self.capacity = capacity
        # TODO: нужно реализовать служебный метод для подсчета количества элементов в сторадже

    def is_overflowed(self) -> bool:
        return self.capacity < self.storage.total_objects


@limit("unable")
class UnableBufferize(BaseLimit):
    def is_overflowed(self) -> bool:
        return True
