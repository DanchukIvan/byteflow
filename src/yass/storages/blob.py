"""
The module contains implementations of various storage managers.
At the moment, only one implementation is presented - Storage Manager - which is used
in data extraction tasks that do not require immediate storage in the storage backend.
"""

from __future__ import annotations

from asyncio import wait_for
from collections.abc import Callable, Iterable
from copy import copy
from pathlib import Path
from typing import Any, Literal, ParamSpec, TypeAlias, TypeVar, cast

from fsspec import available_protocols, get_filesystem_class
from fsspec.asyn import AsyncFileSystem
from rich.pretty import pprint as rpp

from yass.contentio import deserialize, serialize
from yass.core import make_empty_instance, reg_type
from yass.storages import BaseBufferableStorage, engine_factory
from yass.storages.base import ContentQueue

__all__ = [
    "FsBlobStorage",
    "check_path",
    "create_fsspec_engine",
    "download",
    "mk_path",
    "read",
    "upload",
]

_P = ParamSpec("_P")
_V = TypeVar("_V", bound=AsyncFileSystem)
_StorageFabric: TypeAlias = Callable[_P, _V]
_FSSpecEngine: TypeAlias = AsyncFileSystem
_EMPTY_FSSPEC: AsyncFileSystem = make_empty_instance(AsyncFileSystem)


def upload(engine: _FSSpecEngine, content: bytes, path: str) -> None:
    """
    The function of uploading content to the repository.
    Although the function is synchronous, it organizes the execution of the asynchronous storage engine in a separate thread.

    Args:
        engine (_FSSpecEngine): asynchronous storage engine.
        content (bytes): serialized content.
        path (str): the path to save the content.
    """
    engine.pipe_file(path, content)


def download(engine: _FSSpecEngine, path: str) -> bytes:
    """
    The function of downloading content to the repository.
    Although the function is synchronous, it organizes the execution of the asynchronous storage engine in a separate thread.

    Args:
        engine (_FSSpecEngine): asynchronous storage engine.
        path (str): the path to save the content.

    Returns:
        bytes: content in byte (serialized) representation.
    """
    content: bytes = cast(bytes, engine.cat(path))
    return content


def read(engine: _FSSpecEngine, path: str) -> Any:
    """
    A function for reading content from storage.
    Although the function is synchronous, it organizes the execution of the asynchronous storage engine in a separate thread.

    Args:
        engine (_FSSpecEngine): asynchronous storage engine.
        path (str): the path to download the content

    Returns:
        Any: an instance of the data object. The type of the returned object depends on the deserialization
        function registered for the corresponding data format.
    """
    content: bytes = download(engine, path)
    extension: str = Path(path).suffix.lstrip(".")
    dataobj = deserialize(content, extension)
    return dataobj


def mk_path(engine: _FSSpecEngine, path: str) -> None:
    """
    A function for creating a folder or file in the storage.
    Although the function is synchronous, it organizes the execution of the asynchronous storage engine in a separate thread.

    Args:
        engine (_FSSpecEngine): asynchronous storage engine.
        path (str): the path to use to create a folder or file.
    """
    try:
        engine.mkdir(Path(path).parts[0])
    except FileExistsError:
        engine.touch(path)  # type: ignore


def check_path(
    engine: _FSSpecEngine, path: str, *, autocreate: bool = True
) -> bool:
    """
    A function to check the existence of a path to a file or folder.
    Although the function is synchronous, it organizes the execution of the asynchronous storage engine in a separate thread.

    Args:
        engine (_FSSpecEngine): asynchronous storage engine.
        path (str): the path to be checked
        autocreate (bool, optional): a flag that determines whether to create an object according to the path automatically.
        Defaults to True.

    Returns:
        bool: an indicator of the existence of a path.
    """
    status: bool = engine.exists(path)
    if (res := not status) and autocreate:
        mk_path(engine, path)
        return res
    else:
        return status


@reg_type("blob")
class FsBlobStorage(BaseBufferableStorage):
    """
    Implementation of a backend class for working with network and other file storages, based on the fsspec library.
    As a consequence, in addition to explicitly defined arguments and attributes, the class also accepts argument
    types and values ​​that are valid for a particular FS implementation (s3fs, local fs, etc.). For the class to work
    correctly, you must use exclusively asynchronous FS implementations.

    Args:
        engine (_FSSpecEngine): the engine used to access the repository. In this case, the engine is understood as an initialized instance of the AsyncFileSystem class. Defaults to _EMPTY_FSSPEC.
        handshake_timeout (int): timeout for establishing a connection with the backend. Defaults to 10.
        bufferize (bool): data buffering indicator. If False, all data will be constantly merged into the backend without buffering. Defaults to True.
        limit_type (Literal["none", "memory", "count", "time"]): type of data storage limit. Defaults to "none".
        limit_capacity (int | float): limit value of the limiting parameter. For memory limit means the volume in megabytes. Defaults to 10.

    """

    def __init__(
        self,
        engine: _FSSpecEngine = _EMPTY_FSSPEC,
        *,
        handshake_timeout: int = 10,
        bufferize: bool = True,
        limit_type: Literal["none", "memory", "count", "time"] = "none",
        limit_capacity: int | float = 10,
    ):
        super().__init__(
            engine,
            handshake_timeout=handshake_timeout,
            bufferize=bufferize,
            limit_type=limit_type,
            limit_capacity=limit_capacity,
        )

    async def launch_session(self) -> None:
        async with self._queue_lock:
            if not self.active_session:
                try:
                    coro = self.engine.set_session()
                    await wait_for(coro, self.connect_timeout)
                    self.active_session = True
                except TimeoutError as err:
                    msg = f"Не удалось установить соединение в {self.__class__.__name__} в течение заданного таймаута."
                    raise RuntimeError(msg) from err
                print("Сессия с хранилищем успешно установлена.")

    @property
    def registred_types(self) -> Iterable[str]:
        return available_protocols()

    async def check_path(self, path: str, *, autocreate: bool = False) -> bool:
        status: bool = await self.engine._exists(path)
        if (res := not status) and autocreate:
            try:
                await self.engine._mkdir(Path(path).parts[0])
            except FileExistsError:
                await self.engine._touch(path)  # type: ignore
            return res
        else:
            return status

    async def merge_to_backend(self, buf: ContentQueue) -> None:
        """
        The method activates the loading of data to the backend storage from the intermediate buffer
        if it is available in the implementation.
        """
        print(f"Start merging to backend")
        rpp(
            f"Внутренняя блокировка буфера уже захвачена? {buf.internal_lock.locked()}"
        )
        async with buf.block_state() as buf:
            rpp(
                "Захвачена внутренняя блокировка буфера для выгрузки контента."
            )
            old_content: dict[str, Any] = copy(buf.queue)
            buf.reset()
            await self._recalc_counters()
            content_format: str = buf.out_format
            rpp(f"Очищаю содержимое буфера {buf} после копирования.")
            rpp(f"Перехожу к загрузке контента в хранилище.")
            for path, data in old_content.items():
                await self.check_path(path, autocreate=True)
                await self.engine._pipe_file(
                    path, serialize(data, content_format)
                )
            rpp(f"Завершил загрузку контекта в хранилище.")

        rpp(f"Процесс выгрузки данных в хранилище завершен.")


@engine_factory(FsBlobStorage)
def create_fsspec_engine(
    proto: str, *, engine_kwargs: dict[str, Any]
) -> _FSSpecEngine:
    """
    The factory of data storage engines. Uses fsspec under the hood.
    Returns an error if the engine does not support asynchronous execution of operations.

    Args:
        proto (str): a string with the name of the storage engine protocol. For a list of supported protocols, see ...
        engine_kwargs (dict[str, Any]): a dictionary with storage engine parameters. For mandatory and optional parameters,
        see the corresponding implementation of the fsspec protocol, also via the function...

    Returns:
        _FSSpecEngine: asynchronous implementation of the storage engine.
    """
    _storage: _StorageFabric = get_filesystem_class(proto)
    if not issubclass(_storage, _FSSpecEngine):
        msg = "Допускается использование только асинхронных движков."
        raise RuntimeError(msg) from None
    engine = _storage(**engine_kwargs)
    return engine


if __name__ == "__main__":
    ...
