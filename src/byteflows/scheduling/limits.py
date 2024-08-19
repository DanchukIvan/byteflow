from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from rich.pretty import pprint

from byteflows.scheduling import BaseLimit

if TYPE_CHECKING:
    from byteflows.storages import BaseBufferableStorage


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
    """
    A decorator for registering limit classes in the corresponding factory.

    Args:
        limit_type (str): name of the limit. It is highly desirable that it reflects the real meaning of the registered limit class.

    Returns:
        Callable: returns the registration function. Based on the results of the entire chain of calls,
                the specified function will return the registered class without changes.
    """

    def registred_limit(cls: type[BaseLimit]) -> type[BaseLimit]:
        _LIMIT_MAP[limit_type] = cls
        return cls

    return registred_limit


def setup_limit(
    limit_type: str, capacity: Any, storage: BaseBufferableStorage
) -> BaseLimit:
    """
    Factory function that returns an instance of the limits class.

    Args:
        limit_type (str): limit type.
        capacity (Any): volume limit.
        storage (BaseBufferableStorage): the storage to which the limit will be associated.

    Returns:
        BaseLimit: instance of the limit class of the selected type.
    """
    limit_instance: BaseLimit = _LIMIT_MAP[limit_type](storage, capacity)
    return limit_instance


def get_allowed_limits() -> list[tuple[str, BaseLimit]]:
    """
    The function returns all currently available limit classes as a container.

    Returns:
        list (tuple[str, BaseLimit]): list of tuples with pairs limit name - limit class object.
    """
    return list((k, v) for k, v in _LIMIT_MAP.items())


@limit("time")
class TimeLimit(BaseLimit):
    """
    A limit class that controls how long data remains in the buffer.

    Attributes:
        storage (BaseBufferableStorage): a storage facility whose status is monitored.
        capacity (int): time in seconds.
    """

    def __init__(self, storage: BaseBufferableStorage, capacity: int):
        """
        Args:
            storage (BaseBufferableStorage): a storage facility whose status is monitored.
            capacity (int): time in seconds.
        """
        self.storage: BaseBufferableStorage = storage
        self.capacity = timedelta(seconds=capacity)

    def is_overflowed(self) -> bool:
        current_timestamp: datetime = datetime.now()
        return self.capacity < (current_timestamp - self.storage.last_commit)


@limit("memory")
class MemoryLimit(BaseLimit):
    """
    A limit class that controls the amount of memory occupied by buffers.

    Attributes:
        storage (BaseBufferableStorage): a storage facility whose status is monitored.
        capacity (int): time in seconds.
    """

    def __init__(self, storage: BaseBufferableStorage, capacity: int | float):
        """
        Args:
            storage (BaseBufferableStorage): a storage facility whose status is monitored.
            capacity (int): time in seconds.
        """
        self.storage: BaseBufferableStorage = storage
        self.capacity: int | float = capacity

    def is_overflowed(self) -> bool:
        pprint(
            f"Текущий размер занятой буферами памяти составляет {self.storage.mem_alloc} MB при ограничении в {self.capacity} MB."
        )
        return self.capacity < self.storage.mem_alloc


@limit("count")
class CountLimit(BaseLimit):
    """
    A limit class that controls the amount of objects occupied by buffers.

    Attributes:
        storage (BaseBufferableStorage): a storage facility whose status is monitored.
        capacity (int): time in seconds.
    """

    def __init__(self, storage: BaseBufferableStorage, capacity: int):
        """
        Args:
            storage (BaseBufferableStorage): a storage facility whose status is monitored.
            capacity (int): the limit on the number of objects in the buffer.
        """
        self.storage: BaseBufferableStorage = storage
        self.capacity: int = capacity
        # TODO: нужно реализовать служебный метод для подсчета количества элементов в сторадже

    def is_overflowed(self) -> bool:
        return self.capacity < self.storage.total_objects


@limit("unable")
class UnableBufferize(BaseLimit):
    """
    Special plug. Essentially disables data buffering, forcing the storage class
    to continuously write data to the backend. Its use can negatively impact
    performance as it significantly increases the amount of I/O and more often
    blocks memory buffers until the data is uploaded to the backend.
    """

    def is_overflowed(self) -> bool:
        return True
