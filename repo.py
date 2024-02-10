import contextlib
import copy
from abc import abstractmethod
from asyncio import Task, create_task, gather, wait_for
from collections.abc import AsyncGenerator, Callable, Iterable
from dataclasses import KW_ONLY, dataclass, field
from functools import partial
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
    ParamSpec,
    Self,
    TypeAlias,
    TypeVar,
    overload,
)

from fsspec import get_filesystem_class
from fsspec.asyn import AsyncFileSystem
from more_itertools import first

from base import YassCore
from exceptions import EndOfResource

P = ParamSpec("P")
V = TypeVar("V", bound=AsyncFileSystem)
StorageFabric: TypeAlias = Callable[P, V]


inited_engine_map: dict[str, AsyncFileSystem] = {}


# TODO: фабрика движков хранилища fsspec и собственных движков
def create_storage_engine(
    proto: str, *, engine_kwargs: dict[str, Any]
) -> AsyncFileSystem:
    _storage: StorageFabric = get_filesystem_class(proto)
    if not issubclass(_storage, AsyncFileSystem):
        raise RuntimeError from None
    engine = _storage(**engine_kwargs)
    return engine


from contentio import DataProxyProto, deserialize, io_context_map, serialize

if TYPE_CHECKING:
    from contentio import _IOContext


# TODO: базовый класс Репо. Он наследует от YassCore
class Repo(YassCore):
    @abstractmethod
    def put(self, content: Iterable[bytes]) -> None:
        ...

    @abstractmethod
    def merge_to_backend(self):
        ...


@dataclass
class ContextBindingMixin:
    engine_set: dict = field(default_factory=dict)

    @abstractmethod
    async def set_session(self):
        ...

    @classmethod
    async def start_session(cls, engine: AsyncFileSystem) -> None | NoReturn:
        coro: Task = create_task(engine.set_session())
        await coro
        if coro.exception() is None:
            cls.active_session = True
        else:
            raise coro.exception()

    def get_engine(self, proto: str, *, engine_kwargs) -> AsyncFileSystem:
        return create_storage_engine(proto, engine_kwargs=engine_kwargs)

    @contextlib.asynccontextmanager
    async def start_stream(self, target: object) -> AsyncGenerator[Self, Any]:
        try:
            ctx_set: list[_IOContext] = io_context_map[id(target)]
            self.ctx_set: list[_IOContext] = ctx_set
            for ctx in self.ctx_set:
                self.engine_set[id(ctx)] = self.get_engine(
                    ctx.storage_proto, engine_kwargs=ctx.storage_kwargs
                )
                await gather(
                    *[
                        self.start_session(engine)
                        for engine in self.engine_set.values()
                    ]
                )
            yield self
        except KeyError:
            raise RuntimeError(
                "Контекст ввода-вывода для данного объекта не определен"
            )


T = TypeVar("T", bound=object, covariant=True)


async def upload(engine: AsyncFileSystem, content: bytes, path: str) -> None:
    await engine._pipe_file(path, content)


async def download(engine: AsyncFileSystem, path: str):
    content = await engine._cat(path)
    return content


async def read(engine: AsyncFileSystem, path: str) -> DataProxyProto:
    content = await download(engine, path)
    extension: str = Path(path).suffix.lstrip(".")
    dataobj = datatype_registry[extension](content)
    return dataobj


async def mk_path(engine: AsyncFileSystem, path: str):
    try:
        await engine._mkdir(Path(path).parts[0])
    except FileExistsError:
        await engine._touch(path)


async def check_path(
    engine: AsyncFileSystem, path: str, *, autocreate: bool = True
):
    status: bool = await engine._exists(path)
    if (res := not status) and autocreate:
        await mk_path(engine, path)
        return res
    else:
        return status


from asyncio import Future
from collections import defaultdict
from dataclasses import dataclass, field
from functools import partial

if TYPE_CHECKING:
    from contentio import DataProcPipeline, DataProxyProto

from threading import Lock

SHARED_LOCK = Lock()

from contentio import _IOContext, datatype_registry
from helpers import scale_bytes


class EORTrigger:
    def __init__(self):
        self._default: int | float = 0

    @overload
    def __get__(self, instance: None, owner: type) -> Self:
        ...

    @overload
    def __get__(self, instance: object, owner: None) -> float:
        ...

    @overload
    def __get__(self, instance: object, owner: None) -> int:
        ...

    def __get__(
        self, instance: object | None, owner: type | None = None
    ) -> Self | float | int:
        if instance is None:
            return self
        return scale_bytes(self._default, "mb")

    @overload
    def __set__(self, instance: object, value: int) -> int | None:
        ...

    @overload
    def __set__(self, instance: object, value: float) -> float | None:
        ...

    def __set__(
        self, instance: object, value: int | float
    ) -> int | float | None:
        if value == 0:
            raise EndOfResource
        else:
            self._default = self._default + value

    def reset_counter(self):
        self._default = 0


WrappedContent = DataProxyProto[Any] | Future[Any] | bytes
ContentSet = tuple[tuple[str, WrappedContent], ...]
empty_func = lambda x: x


@dataclass
class ContentManager:
    _: KW_ONLY
    max_alloc_mem: int | float = field(default=10)
    mem_alloc: int | float = field(default=0)
    queques: dict[int, dict[str, DataProxyProto | Future]] = field(
        default_factory=partial(defaultdict, dict)
    )
    ctx_set: list[_IOContext] = field(default_factory=list)
    target_id: int = field(default=0)

    async def save(self, content: bytes):
        for io_ctx in self.ctx_set:
            queue = self.queques[id(io_ctx)]
            path = io_ctx.out_path
            in_fmt = io_ctx.in_format
            data: Future[Any] | DataProxyProto = deserialize(content, in_fmt)
            pipeline: DataProcPipeline | None = io_ctx.pipeline
            if pipeline:
                checked_data = pipeline.eor_checker(data)
                future = pipeline.run_transform(checked_data)
                processed_data = await wait_for(future, pipeline.timeout)
            else:
                processed_data = data
            print(f"Memory buffer size is {self.mem_alloc}")
            queue[path] = processed_data
        self._recalc_mem_alloc()

    def _recalc_mem_alloc(self):
        mem_alloc: int | float = 0
        for ctx in self.ctx_set:
            mem_alloc += sum(
                len(serialize(data, ctx.out_format))
                for data in self.queques[id(ctx)].values()
            )
        self.mem_alloc = scale_bytes(mem_alloc, "mb")

    def get_content(self, path: str) -> DataProxyProto | Future | None:
        for queue in self.queques.values():
            if path in queue.values():
                return queue[path]

    async def get_all_content(self, ctx: _IOContext) -> ContentSet:
        ctx_id: int = id(ctx)
        # TODO: тут нужен асинхронный лок для общей очереди, вообще нужен лок при обращении к разделяемым ресурсам и переменным.
        # TODO: лок у каждого экземпляра будет свой.
        target_queue: dict[str, DataProxyProto | Future] = copy.deepcopy(
            self.queques[ctx_id]
        )
        self.queques[ctx_id].clear()
        self._recalc_mem_alloc()
        print(f"Memory buffer size is {self.mem_alloc}")
        res: ContentSet = tuple(
            (path, dataobj)
            for path, dataobj in target_queue.items()
            if not isinstance(dataobj, Exception)
        )
        return res

    @property
    def input_set(self) -> list[str]:
        return [ctx.in_format for ctx in self.ctx_set]

    @property
    def output_set(self) -> list[str]:
        return [ctx.out_format for ctx in self.ctx_set]

    @property
    def binded_targets(self) -> list[_IOContext]:
        return self.ctx_set


@dataclass
class RepoManager(
    Repo, ContextBindingMixin, ContentManager, subcls_key="repo_manager"
):
    async def put(self, content: list[bytes]) -> None:
        print("Saving content")
        await gather(*[self.save(cbytes) for cbytes in content])
        if self.mem_alloc > self.max_alloc_mem:
            create_task(self.merge_to_backend())

    async def merge_to_backend(self) -> None:
        print(f"Start merging to backend")
        for ctx in self.ctx_set:
            for path, buffer in await self.get_all_content(ctx):
                self.engine = self.engine_set[id(ctx)]
                if not await check_path(self.engine, path):
                    await mk_path(self.engine, path)
                await upload(
                    self.engine, serialize(buffer, ctx.out_format), path
                )  # type:ignore


if __name__ == "__main__":
    ...
