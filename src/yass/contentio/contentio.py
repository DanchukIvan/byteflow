from asyncio import to_thread
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field, is_dataclass, replace
from functools import reduce
from inspect import BoundArguments, Signature, signature
from io import BytesIO
from itertools import chain
from pprint import pprint
from typing import Any, Generic, Protocol, Self, TypeAlias, TypeVar, cast

from fsspec.asyn import AsyncFileSystem

from ._helpers import *
from .common import *

__all__ = [
    "DataProxy",
    "DataProxyProto",
    "IOBoundPipeline",
    "PathSegment",
    "PathTemplate",
    "Proxied",
    "WrapperNewMixin",
    "allowed_datatypes",
    "create_datatype",
    "create_io_context",
    "datatype_info",
    "deserialize",
    "reg_input",
    "reg_output",
    "serialize",
]


def reg_input(extension: str, func: Callable, extra_args: dict[str, Any] = {}):
    if _check_input_sig(func):
        func = _update_sign(func, extra_args) if extra_args else func
    else:
        raise RuntimeError(
            "Первым аргументом функции ввода должен быть объект типа bytes"
        ) from None
    INPUT_MAP[extension] = func


def reg_output(
    extension, func: Callable, extra_args: dict[str, Any] = {}
) -> None:
    if _check_output_sig(func):
        func = _update_sign(func, extra_args) if extra_args else func
    else:
        raise RuntimeError(
            "Вторым аргументом функции вывода должен быть объект типа BytesIO или совместимый с ним байтовый контейнер"
        ) from None
    OUTPUT_MAP[extension] = func


def deserialize(
    content: bytes, format: str, extra_args: dict[str, Any] = {}
) -> "Proxied":
    func: Callable[..., Proxied] = INPUT_MAP[format]
    dataobj: Proxied = func(content, **extra_args)
    return dataobj


def serialize(
    dataobj: object, format: str, extra_args: dict[str, Any] = {}
) -> bytes:
    byte_buf = BytesIO()
    func = OUTPUT_MAP[format]
    func(dataobj, byte_buf, **extra_args)
    return byte_buf.getvalue()


Proxied = TypeVar("Proxied", covariant=True)


class DataProxyProto(Protocol[Proxied]):
    data_format: str

    def to_bytes(self) -> bytes: ...

    def convert(self, format: str) -> Self: ...

    def get_repr(self) -> Proxied: ...

    def __getattr__(self) -> Any: ...


class WrapperNewMixin(Generic[Proxied]):
    def __new__(cls: type, wrapped: Proxied) -> Proxied:
        new_cls: cls = cast(cls, type(wrapped))
        return super().__new__(new_cls)


class DataProxy(DataProxyProto, WrapperNewMixin[Proxied]):
    data_format: str

    def __new__(cls, content: bytes) -> Proxied:
        try:
            wrapped: Proxied = INPUT_MAP[cls.data_format](content)
        except KeyError as exc:
            raise RuntimeError(
                "Не зарегистрировано ни одной функции ввода-вывода для данного типа данных"
            ) from exc
        return super().__new__(cls, wrapped)

    def __init__(self, wrapped: Proxied):
        self.data: Proxied = wrapped
        self.dataformat = self.data_format

    @property
    def extension(self):
        return self.data_format

    def to_bytes(self) -> bytes:
        bytes_repr: bytes = serialize(self, self.dataformat)
        return bytes_repr

    def convert(self, format: str) -> None:
        self.dataformat = format

    def get_repr(self) -> Proxied:
        return self.data

    def __getattr__(self, name: str):
        return getattr(self.data, name)

    def __str__(self) -> str:
        return self.data_format


# TODO: нам не нужны разные прокси-типы
datatype_registry: dict[str, type[DataProxyProto]] = {}


def create_datatype(
    *,
    format_name: str,
    input_func: Callable,
    extra_args_in: dict = {},
    output_func: Callable,
    extra_args_out: dict = {},
) -> None:
    if not format_name in INPUT_MAP or not format_name in OUTPUT_MAP:
        reg_input(format_name, input_func, extra_args_in)
        reg_output(format_name, output_func, extra_args_out)
    else:
        msg = "Данный тип данных уже зарегистрирован"
        raise RuntimeError(msg)


_DataTypeInfo: TypeAlias = dict[str, dict[str, Any]]


def allowed_datatypes(*, display: bool = False) -> list[_DataTypeInfo]:
    ndrows: list[_DataTypeInfo] = [datatype_info(k) for k in INPUT_MAP]
    if display:
        for row in ndrows:
            pprint(row, depth=2, sort_dicts=False)
    return ndrows


def datatype_info(datatype: str) -> _DataTypeInfo:
    if datatype in INPUT_MAP:
        info: _DataTypeInfo = {
            datatype: {
                "output func": OUTPUT_MAP.get(datatype),
                "input func": INPUT_MAP.get(datatype),
                "data container": signature(
                    INPUT_MAP.get(datatype)  # type: ignore
                ).return_annotation,
            }
        }
        return info
    else:
        msg = f"Тип данных {datatype} не зарегистрирован"
        raise KeyError(msg)


@dataclass
class PathSegment:
    concatenator: str
    segment_order: int = field(compare=True)
    segment_parts: list[str | Callable] = field(default_factory=list)

    def add_part(self, *part: str) -> None:
        self.segment_parts.extend(part)

    def change_concat(self, concatenator: str) -> Self:
        kwds: dict[str, Any] = locals()
        return replace(kwds.pop("self"), **kwds)

    def __str__(self) -> str:
        str_represent: list[str] = list(
            map(lambda x: f"{x()}" if callable(x) else x, self.segment_parts)
        )
        return self.concatenator.join(str_represent)


class PathTemplate:
    def __init__(
        self, segments: list[PathSegment] = [], is_local: bool = False
    ) -> None:
        self.segments: list[PathSegment] = segments
        self.is_local: bool = is_local

    def add_segment(
        self,
        concatenator: str,
        segment_order: int,
        segment_parts: list[str | Callable[..., Any]],
    ) -> None:
        self.segments.append(
            PathSegment(concatenator, segment_order, segment_parts)
        )

    def render_path(self, ext: str = "") -> str:
        self.segments.sort(key=lambda x: x.segment_order)
        nonull_segments = map(
            lambda x: str(x),
            filter(lambda x: x.__str__() != "", self.segments),
        )
        nonull_segments = cast(Iterable[str], nonull_segments)
        if self.is_local or (platform == "linux" and not self.is_local):
            sep = os.sep
        elif platform == "win32" and not self.is_local:
            sep = cast(str, os.altsep)
        return (
            sep.join(nonull_segments) + f".{ext}"
            if ext
            else sep.join(nonull_segments)
        )


@dataclass
class IOBoundPipeline:
    io_context: IOContext
    functions: list[Callable] = field(default_factory=list)
    timeout: int = field(default=10)
    on_error: Callable = lambda x: x
    data_filter: Callable = lambda x: True

    def step(self, order: int, *, extra_kwargs: dict[str, Any] = {}):
        def wrapper(func):
            self._check_sig(func)
            updated_func: Callable = _update_sign(
                func, extra_kwargs=extra_kwargs
            )
            self.functions.insert(order - 1, updated_func)
            return func

        return wrapper

    def eorsignal(self, predicate: Callable) -> Callable:
        self._check_sig(predicate)
        self.eor_checker = predicate
        return predicate

    def run_transform(self, dataobj: Any) -> Coroutine:
        coro: Coroutine = to_thread(
            reduce, lambda arg, func: func(arg), self.functions, dataobj
        )
        return coro

    def accept_handler(self, func: Callable) -> bool:
        try:
            self._check_sig(func)
            return True
        except ValueError as exc:
            msg = exc.args
            print(
                f"Функция-обработчик должна принимать объект класса {msg} и возвращать его."
            )
            return False

    # TODO: нужно проверять, что тип возврата функции обработки есть в аргументах функции десериализации
    def _check_sig(self, func: Callable) -> None:
        ctx: _IOContext = [
            ctx
            for ctx in chain(*IOCONTEXT_MAP.values())
            if ctx.ctx_id == self.binding_id
        ][0]
        return_annot: type = signature(func).return_annotation
        func_args_annot: list[type] = [
            param.annotation for param in signature(func).parameters.values()
        ]
        input_func = INPUT_MAP[ctx.in_format]
        return_input_annot = _resolve_annotation(
            signature(input_func).return_annotation, str(input_func.__module__)
        )
        valid_annot = [
            arg_annot
            for arg_annot in func_args_annot
            if issubclass(arg_annot, return_input_annot)
            and issubclass(return_annot, return_input_annot)
        ]
        if valid_annot:
            return
        else:
            msg: str = f"Функция-обработчик должна принимать любой объект из перечисленных: {return_input_annot}. А также возвращать его или совестимый с функцией ввода тип."
            raise ValueError(msg)

    def change_order(self, old_idx: int, new_idx: int) -> None:
        func: Callable = self.functions.pop(old_idx)
        self.functions.insert(new_idx, func)

    def get_pipeline(self) -> tuple[tuple[int, str], ...]:
        return tuple(
            (order, func.__name__) for order, func in enumerate(self.functions)
        )

    def show_pipline(self):
        ctx: _IOContext = [
            ctx
            for ctx in chain(*IOCONTEXT_MAP.values())
            if ctx.ctx_id == self.binding_id
        ][0]
        pipe: list[str] = [
            f"{order}: {func.__name__}"
            for order, func in enumerate(self.functions)
        ]
        return "->".join(pipe) + f" for {ctx.in_format}."


pipeline_map: dict[str | int, DataProcPipeline] = {}


@dataclass
class _IOContext:
    target: object = field()
    in_format: str = field(kw_only=True)
    out_format: str = field(kw_only=True)
    storage_proto: str = field(kw_only=True)
    storage_kwargs: dict = field(kw_only=True)
    path_temp: PathTemplate = field(init=False, kw_only=True)
    pipeline: DataProcPipeline | None = field(default=None, kw_only=True)

    def __post_init__(self):
        self._check_io()

    @property
    def ctx_id(self):
        return id(self.target)

    @property
    def wrap_pipeline(self) -> DataProcPipeline:
        if not self.pipeline:
            self.pipeline = DataProcPipeline(self.ctx_id)
        return self.pipeline

    @property
    def out_path(self):
        return self.path_temp.with_ext(self.out_format)

    @property
    def in_path(self):
        return self.path_temp.with_ext(self.in_format)

    def _check_io(self):
        try:
            in_obj = datatype_registry[self.in_format]
            out_obj = datatype_registry[self.out_format]
        except KeyError as exc:
            raise ValueError(
                f"Не зарегистрирован тип данных {exc.args}."
            ) from exc
        return True

    def update_ctx(
        self,
        *,
        target: object | None = None,
        in_format: str | None = None,
        out_format: str | None = None,
        engine: AsyncFileSystem | None = None,
        path_temp: PathTemplate | None = None,
    ) -> Self:
        kwds: dict[str, Any] = {k: v for k, v in locals() if v}
        kwds.pop("self")
        if is_dataclass(self):
            return replace(self, **kwds)
        else:
            sig: Signature = signature(type(self))
            bind: BoundArguments = sig.bind_partial(**kwds)
            self.__dict__.update(bind.kwargs)
            return self


def create_io_context(
    *,
    target: object,
    in_format: str,
    out_format: str,
    storage_proto: str,
    storage_kwargs: dict,
) -> _IOContext:
    kwargs: dict[str, Any] = {
        k: v for k, v in locals().items() if v is not None
    }
    ctx = _IOContext(**kwargs)
    IOCONTEXT_MAP[id(target)].append(ctx)
    return ctx


if __name__ == "__main__":
    ...
    # import polars
    # overloadeds = get_overloads(polars.read_json) or [polars.read_json]
    # ann = signature(polars.read_csv).return_annotation
    # ann2 = list(signature(polars.DataFrame.write_csv).parameters.values())[1].annotation
    # print(_resolve_annotation(ann, str(polars.read_csv.__module__)))
    # print(_resolve_annotation(ann2, str(polars.DataFrame.write_csv.__module__)))
