"""
The module contains only the base class of the storage manager, from which all other implementations should inherit.
"""

from __future__ import annotations

from abc import abstractmethod
from asyncio import Lock, create_task
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
)
from contextlib import asynccontextmanager
from datetime import datetime
from itertools import chain
from threading import Lock as ThreadLock
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self, TypeAlias
from weakref import WeakValueDictionary

from _collections_abc import dict_items
from rich.pretty import pprint as rpp

from yass.contentio import serialize
from yass.core import YassCore, make_empty_instance
from yass.scheduling import UnableBufferize, setup_limit
from yass.utils import scale_bytes

__all__ = [
    "AnyDataobj",
    "BaseBufferableStorage",
    "BufferDispatcher",
    "ContentQueue",
    "Mb",
    "engine_factory",
    "supported_engine_factories",
    "AnyDataobj",
]

if TYPE_CHECKING:
    from yass.contentio import IOContext
    from yass.resources.base import BaseResourceRequest
    from yass.scheduling import BaseLimit


class _SupportAsync(Awaitable, Protocol): ...


_EMPTY_ASYNC_ENGINE = make_empty_instance(_SupportAsync)
_ENGINE_FACTORIES: dict[tuple[type, ...], Callable[..., _SupportAsync]] = (
    dict()
)

Mb = int | float
AnyDataobj: TypeAlias = Any


def engine_factory(*_cls: type):
    def wrapped_func(func: Callable):
        _ENGINE_FACTORIES[_cls] = func
        return func

    return wrapped_func


def _get_engine_factory(key: type):
    for t in filter(lambda x: key in x, _ENGINE_FACTORIES.keys()):
        return _ENGINE_FACTORIES[t]
    msg = "Фабрика движков для данного класса не зарегистрирована."
    raise KeyError(msg) from None


def supported_engine_factories():
    return _ENGINE_FACTORIES


class ContentQueue:
    def __init__(
        self, storage: BaseBufferableStorage, in_format: str, out_format: str
    ):
        self.queue: dict[str, AnyDataobj] = dict()
        self.storage: BaseBufferableStorage = storage
        self.in_format: str = in_format
        self.out_format: str = out_format
        self.internal_lock = Lock()

    @asynccontextmanager
    async def block_state(self) -> AsyncGenerator[Self, Any]:
        try:
            await self.internal_lock.acquire()
            yield self
        except Exception as exc:
            raise exc
        finally:
            if self.internal_lock.locked():
                self.internal_lock.release()

    async def parse_content(self, content: Iterable) -> None:
        for path, dataobj in content:
            self.queue[path] = dataobj
        rpp(f"Количество объектов в буфере {len(self.queue)}")
        await self.storage._recalc_counters()
        async with self.storage._timemark_lock:
            self.storage.last_commit = datetime.now()
            rpp(f"Последний коммит совершен в {self.storage.last_commit}")
        if self.storage.check_limit():
            create_task(self.storage.merge_to_backend(self))

    def get_content(self, path: str) -> AnyDataobj:
        return self.queue.get(path, False)

    def get_all_content(self) -> chain[AnyDataobj]:
        return chain(self.queue.values())

    @property
    def size(self) -> int:
        return len(self.queue)

    @property
    def memory_size(self) -> int | float:
        bytes_mem: int = sum(
            len(serialize(data, self.out_format))
            for data in self.get_all_content()
        )
        return scale_bytes(bytes_mem, "mb")

    def reset(self) -> None:
        self.queue.clear()

    def __contains__(self, item: AnyDataobj) -> bool:
        return item in self.queue

    def __iter__(self) -> Generator[tuple[str, Any], Any, None]:
        yield from self.queue.items()


class BufferDispatcher:
    def __init__(self):
        self.queue_sequence: dict[BaseResourceRequest, ContentQueue] = dict()
        self._lock: ThreadLock = ThreadLock()
        self._cache: WeakValueDictionary[BaseResourceRequest, ContentQueue] = (
            WeakValueDictionary()
        )

    def make_channel(
        self, storage: BaseBufferableStorage, id: BaseResourceRequest
    ) -> ContentQueue:
        if id not in self._cache:
            io_ctx: IOContext = id.io_context
            queue = ContentQueue(storage, io_ctx.in_format, io_ctx.out_format)
            with self._lock:
                self._cache[id] = queue
                self.queue_sequence[id] = queue
        else:
            queue: ContentQueue = self._cache[id]
        print(
            f"Созданные в памяти буферы: {self.queue_sequence.keys(), self.queue_sequence.values()}"
        )
        return queue

    def get_content(self, path: str) -> AnyDataobj | None:
        for dataobj in filter(
            lambda x: x.get_content(path), self.queue_sequence.values()
        ):
            return dataobj

    def get_buffers(self) -> list[ContentQueue]:
        return list(self.queue_sequence.values())

    def get_items(self) -> dict_items[BaseResourceRequest, ContentQueue]:
        return self.queue_sequence.items()

    def __iter__(self) -> Generator[ContentQueue, Any, None]:
        yield from self.get_buffers()


class BaseBufferableStorage(YassCore):
    """
    The base class for all other classes implementing data saving operations and interaction with the storage backend.
    """

    def __init__(
        self,
        engine: Any = _EMPTY_ASYNC_ENGINE,
        *,
        handshake_timeout: int = 10,
        bufferize: bool = False,
        limit_type: Literal["none", "memory", "count", "time"] = "none",
        limit_capacity: int | float = 10,
    ):
        self.engine: Any = engine
        self.connect_timeout: int = handshake_timeout
        self.last_commit: datetime = datetime.now()
        if bufferize and limit_type != "none":
            self.limit: BaseLimit = setup_limit(
                limit_type, limit_capacity, self
            )
        else:
            self.limit: BaseLimit = UnableBufferize()
        self.mem_buffer: BufferDispatcher = BufferDispatcher()
        self.mem_alloc: Mb = 0
        self.total_objects: int = 0
        self._queue_lock: Lock = Lock()
        self._timemark_lock: Lock = Lock()
        self.active_session: bool = False

    @abstractmethod
    async def launch_session(self): ...

    @property
    @abstractmethod
    def registred_types(self) -> Iterable[str]: ...

    @abstractmethod
    async def merge_to_backend(self, buf: ContentQueue) -> None:
        """
        The method activates the loading of data to the backend storage from the intermediate buffer
        if it is available in the implementation.
        """
        async with self._queue_lock:
            ...

    def configure(
        self,
        *,
        engine_proto: str | None = None,
        engine_params: dict | None = None,
        handshake_timeout: int | None = None,
        bufferize: bool | None = None,
        limit_type: Literal["none", "memory", "count", "time"] | None = None,
        limit_capacity: int | float | None = None,
    ) -> Self:
        new_params = {
            key: value
            for key, value in locals().items()
            if key != "self" and value is not None
        }
        engine_maker = _get_engine_factory(self.__class__)
        self.engine = engine_maker(engine_proto, engine_kwargs=engine_params)
        if bufferize and (limit_type and limit_type != "none"):
            self.limit = setup_limit(limit_type, limit_capacity, self)
        default_params = vars(self)
        default_params.update(new_params)
        for param, value in filter(
            lambda x: hasattr(self, x[0]), default_params.items()
        ):
            setattr(self, param, value)
        return self

    async def _recalc_counters(self) -> None:
        """
        The method recalculates the memory occupied in the buffer.
        """
        async with self._queue_lock:
            self.mem_alloc = sum(
                s.memory_size for s in self.mem_buffer.get_buffers()
            )
            self.total_objects = sum(
                s.size for s in self.mem_buffer.get_buffers()
            )

    async def get_content(self, path: str) -> Any:
        """
        The method returns serialized content from the buffer.

        Args:
            path (str): the path to read the content.

        Returns:
            Any | Future | None: content in byte (serialized) representation or None if no such path is found.
        """
        async with self._queue_lock:
            return self.mem_buffer.get_content(path)

    async def get_all_content(self) -> list[AnyDataobj]:
        """
        The method returns all the content associated with this IO context from the buffer. Existing content is deleted from the buffer.

        Args:
            ctx (_IOContext): IOContext object.

        Returns:
            ContentSet (tuple): a set of content in a deserialized form.
        """
        async with self._queue_lock:
            return list(
                chain(
                    *[
                        buf.get_all_content()
                        for buf in self.mem_buffer.get_buffers()
                    ]
                )
            )

    def create_buffer(self, anchor: BaseResourceRequest) -> ContentQueue:
        return self.mem_buffer.make_channel(self, anchor)

    async def write(self, queue_id: Any, content: Iterable) -> None:
        """
        Starts the content processing cycle in competitive mode and stores it in an intermediate buffer.

        Args:
            content (list[bytes]): any iterable object containing a series of serialized (not transformed) data.
        """
        buffer: ContentQueue = self.create_buffer(queue_id)
        await buffer.parse_content(content)

    def check_limit(self) -> bool:
        return self.limit.is_overflowed()


if __name__ == "__main__":
    ...
