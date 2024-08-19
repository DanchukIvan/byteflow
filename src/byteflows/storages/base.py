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
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self
from weakref import WeakValueDictionary

from rich.pretty import pprint as rpp

from byteflows.contentio import serialize
from byteflows.core import ByteflowCore, SfnUndefined, Undefined
from byteflows.scheduling import UnableBufferize, setup_limit
from byteflows.utils import scale_bytes

__all__ = [
    "AnyDataobj",
    "BaseBufferableStorage",
    "BufferDispatcher",
    "ContentQueue",
    "Mb",
    "engine_factory",
    "supported_engine_factories",
]

if TYPE_CHECKING:
    from byteflows.contentio import IOContext
    from byteflows.resources.base import BaseResourceRequest
    from byteflows.scheduling import BaseLimit


class _SupportAsync(Awaitable, Protocol): ...


_ENGINE_FACTORIES: dict[tuple[type, ...], Callable[..., _SupportAsync]] = (
    dict()
)

Mb = int | float
AnyDataobj = Any
"""
Alias for Any. Indicates that the object accepts any valid data object.
"""


def engine_factory(*_cls: type):
    """
    The function registers factories for backend engines. The backend can be file storage,
    object storage, databases, buses and message brokers. Each backend has its own factory class,
    which can either generalize a group of backends or correspond to only one backend.

    Args:
        _cls (type): a sequence of classes to which the engine factory is assigned.
    """

    def wrapped_func(func: Callable):
        _ENGINE_FACTORIES[_cls] = func
        return func

    return wrapped_func


def _get_engine_factory(key: type):
    """
    The function returns the engine factory for the given backend class.

    Args:
        key (type): the class to which the engine factory needs to be provided.

    Raises:
        KeyError: thrown if no factory is registered for this backend class.
    """
    for t in filter(lambda x: key in x, _ENGINE_FACTORIES.keys()):
        return _ENGINE_FACTORIES[t]
    msg = "Фабрика движков для данного класса не зарегистрирована."
    raise KeyError(msg) from None


def supported_engine_factories():
    """
    The function returns the currently available engine factories.
    """
    return _ENGINE_FACTORIES


class ContentQueue:
    """
    This class buffers content in memory, namely by placing it in a dictionary.
    Its main purpose is to avoid repeated I/O, which can significantly affect performance.
    The class also provides additional information about the stored content
    (size of memory allocated by objects, their number).
    The class also allows you to check whether a data object is in a queue
    and to loop through the path-data object pairs stored in the queue.
    In-memory buffers are used by data collectors to temporarily store downloaded content.

    Args:
        storage (BaseBufferableStorage): backend for which the queue is created.
        in_format (str): input data format.
        out_format (str): data upload format.
    """

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
        """
        The method locks the state of the buffer while working with it. The lock that is called is asynchronous.
        """
        try:
            await self.internal_lock.acquire()
            yield self
        except Exception as exc:
            raise exc
        finally:
            if self.internal_lock.locked():
                self.internal_lock.release()

    async def parse_content(self, content: Iterable) -> None:
        """
        The method parses the container with content on the path and the directly loaded
        content and distributes it in a queue. Based on the results of content distribution,
        it updates the time of the last data fixation in the backend and the statistics
        of downloaded data.

        Args:
            content (Iterable): a container with a path string where the content should be stored in the future, and data.
        """
        for path, dataobj in content:
            self.queue[path] = dataobj
        rpp(f"Количество объектов в буфере {len(self.queue)}")
        await self.storage._recalc_counters()
        async with self.storage._timemark_lock:
            self.storage.last_commit = datetime.now()
            rpp(f"Последний коммит совершен в {self.storage.last_commit}")
        if self.storage.check_limit():
            create_task(self.storage.merge_to_backend(self))

    def get_content(self, path: str) -> AnyDataobj | None:
        """
        Returns the content that is stored at the given path.

        Args:
            path (str): the path string where the content is stored.

        Returns:
            AnyDataobj (optional): any data object that is captured in a buffer. If the object is not stored
                                in the given path (for example, the queue was cleared after uploading to the
                                backend), then None is returned.
        """
        return self.queue.get(path, None)

    def get_all_content(self) -> chain[AnyDataobj]:
        """
        The method wraps the content queue in a generator that produces values in the order in which the content was committed to the queue.

        Returns:
            chain[AnyDataobj]: all data objects (without paths) currently present in the buffer, wrapped in a generator.
        """
        return chain(self.queue.values())

    @property
    def size(self) -> int:
        """
        The property returns the length of the queue or, in other words, the number of objects in it.

        Returns:
            int: number of objects in the queue.
        """
        return len(self.queue)

    @property
    def memory_size(self) -> int | float:
        """
        The property returns the amount of allocated memory in megabytes.

        Returns:
            int | float: memory occupied by data in megabytes.
        """
        bytes_mem: int = sum(
            len(serialize(data, self.out_format))
            for data in self.get_all_content()
        )
        return scale_bytes(bytes_mem, "mb")

    def reset(self) -> None:
        """
        The method clears the queue of all objects. As a rule,
        it is used at the end of the process of uploading data to the backend.
        """
        self.queue.clear()

    def __contains__(self, item: AnyDataobj) -> bool:
        return item in self.queue

    def __iter__(self) -> Generator[tuple[str, Any], Any, None]:
        """
        When iterating, the method returns a pair of path and data object.

        Yields:
            Generator[tuple[str, Any], Any, None]: generator that returns the target path and data.
        """
        yield from self.queue.items()


class BufferDispatcher:
    def __init__(self):
        """
        This class accumulates information about the in-memory buffers used by the backend, and also provides a method for creating such buffers.
        """
        self.queue_sequence: dict[BaseResourceRequest, ContentQueue] = dict()
        self._lock: ThreadLock = ThreadLock()
        self._cache: WeakValueDictionary[BaseResourceRequest, ContentQueue] = (
            WeakValueDictionary()
        )

    def make_channel(
        self, storage: BaseBufferableStorage, id: BaseResourceRequest
    ) -> ContentQueue:
        """
        The method creates a buffer in memory and binds it to the specified request.

        Args:
            storage (BaseBufferableStorage): an instance of the backend class to which the buffer is bound.
            id (BaseResourceRequest): an instance of the resource request class. Used to identify the format of input and output data
                                    and assigning a buffer to a specific request.

        Returns:
            ContentQueue: in-memory buffer instance.
        """
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
        """
        Returns the content that is stored at the given path.
        The search is carried out among all buffers registered in the dispatcher.

        Args:
            path (str): the path string where the content is stored.

        Returns:
            AnyDataobj (optional): any data object that is captured in a buffers. If the object is not stored
                                in the given path (for example, the queue was cleared after uploading to the
                                backend), then None is returned.
        """
        for dataobj in filter(
            lambda x: x.get_content(path), self.queue_sequence.values()
        ):
            return dataobj

    def get_buffers(self) -> list[ContentQueue]:
        """
        The method returns an array of registered buffers.

        Returns:
            list[ContentQueue]: an array of committed buffers.
        """
        return list(self.queue_sequence.values())

    # TODO: переименовать метод, сделать возврат кортежа кортежей
    def get_items(
        self,
    ) -> tuple[tuple[BaseResourceRequest, ContentQueue], ...]:
        """
        The method returns a dictionary view of the current set of registered buffers.

        Returns:
            dict_items[BaseResourceRequest, ContentQueue]: I view in the form of “request - buffer” pairs.
        """
        return tuple(self.queue_sequence.items())

    def __iter__(self) -> Generator[ContentQueue, Any, None]:
        """
        The method wraps an array of buffers into a generator for iteration.
        """
        yield from self.get_buffers()


class BaseBufferableStorage(ByteflowCore):
    """
    The base class for all other classes implementing data saving operations and interaction with the storage backend.
    Storage classes are used as a portal to the backend itself. Anything can act as a backend - a message broker, network
    storage, any database, if the type of downloaded data allows storage in such a backend.
    The purpose of storage classes is to optimize I/O by buffering data, providing load control on the end storage system,
    and recording statistics about how resources and data are used.
    """

    def __init__(
        self,
        engine: Undefined | Any = SfnUndefined,
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
    async def launch_session(self) -> None:
        """
        The method is used to establish a session with the destination store.
        Implementation details depend on the type of backend used.

        """

    @property
    @abstractmethod
    def registred_types(self) -> Iterable[str]:
        """
        The property provides a list of all available implementations of engines of a certain backend class.

        Returns:
            Iterable (str): list of available engines.
        """

    @abstractmethod
    async def merge_to_backend(self, buf: ContentQueue) -> None:
        """
        The method transfers data directly to the backend (for storage or further distribution depending on the engine used)
        and clears in-memory buffers if the data is buffered.
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
        """
        The method allows you to reconfigure all or individual backend parameters after creating an instance of the class.
        Accepts the same parameters as the class itself upon initialization.
        """
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
        A utility method that is used to update information about the amount of
        memory occupied by data and the number of objects in buffers.
        """
        async with self._queue_lock:
            self.mem_alloc = sum(
                s.memory_size for s in self.mem_buffer.get_buffers()
            )
            self.total_objects = sum(
                s.size for s in self.mem_buffer.get_buffers()
            )

    async def get_content(self, path: str) -> AnyDataobj | None:
        """
        Returns the content that is stored at the given path.
        The search is carried out among all buffers registered in the buffer dispatcher.

        Args:
            path (str): the path string where the content is stored.

        Returns:
            AnyDataobj (optional): any data object that is captured in a buffers. If the object is not stored
                                in the given path (for example, the queue was cleared after uploading to the
                                backend), then None is returned.
        """
        async with self._queue_lock:
            return self.mem_buffer.get_content(path)

    async def get_all_content(self) -> list[AnyDataobj]:
        """
        The method returns all the content (without the paths where it should be stored) that
        is stored in buffers. During content retrieval, access to the buffer queue is blocked.
        Buffers are not cleared in this case.
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
        """
        The method registers a new in-memory buffer in the manager.

        Args:
            anchor (BaseResourceRequest): the object with which the buffer will be associated.

        Returns:
            ContentQueue: an instance of the in-memory buffer class.
        """
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
        """
        The method checks compliance with the limits set for data storage.
        More detailed information about limits can be found in the limits.py file.

        Returns:
            bool: returns True if data storage limits have reached or exceeded the control parameter limit.
        """
        return self.limit.is_overflowed()


if __name__ == "__main__":
    ...
