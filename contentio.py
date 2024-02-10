import ast
from asyncio import to_thread
from collections import defaultdict
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, field, is_dataclass, replace
from functools import partial, reduce
from importlib import import_module
from inspect import BoundArguments, Parameter, Signature, signature
from io import BytesIO
from itertools import chain
from pathlib import Path
from pprint import pprint
from typing import (
    Any,
    Generic,
    Protocol,
    Self,
    TypeAlias,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_overloads,
    get_type_hints,
)

from fsspec.asyn import AsyncFileSystem

input_map: dict[str, Callable] = {}
output_map: dict[str, Callable] = {}


def _update_sign(func: Callable, extra_kwargs: dict) -> Callable:
    if not extra_kwargs:
        return func
    sig: Signature = signature(func)
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    new_params: list[Parameter] = []
    for name, param in sig.parameters.items():
        value: Any = extra_kwargs.get(name, param.default)
        if param.kind in (
            Parameter.POSITIONAL_ONLY,
            Parameter.POSITIONAL_OR_KEYWORD,
        ):
            args.append(value)
        else:
            kwargs[name] = value
        new_params.append(param.replace(name=name, default=value))
    new_sig: Signature = sig.replace(parameters=new_params)
    if kwdefault := getattr(func, "__kwdefaults__", False):
        kwdefault.update(kwargs)
        setattr(func, "__kwdefaults__", kwdefault)
    elif default := getattr(func, "__defaults__", False):
        default = tuple(args)
        setattr(func, "__defaults__", default)
    elif hasattr(func, "__signature__"):
        func.__signature__ = new_sig
    else:
        func = partial(func, *args, **kwargs)
    return func


def _resolve_annotation(annotated: Any, module_name: str):
    cnt_split = module_name.count(".")
    if isinstance(annotated, str):
        not_resolved = []
        search_area = module_name
        cnt_split = module_name.count(".")
        annotations = annotated.split("|")
        result = []
        for annot in annotations:
            loop_cnt = 0
            while cnt_split >= loop_cnt:
                annot = annot.strip()
                try:
                    import builtins
                    import io
                    import typing

                    mod = import_module(search_area)
                    typeclass = (
                        getattr(mod, annot, False)
                        or getattr(builtins, annot, False)
                        or getattr(io, annot, False)
                        or getattr(typing, annot, ast.literal_eval(annot))
                    )
                    if typeclass is not None:
                        result.append(typeclass)
                    break
                except Exception:
                    loop_cnt += 1
                    search_area = search_area.rsplit(".", loop_cnt)[0]
                    if loop_cnt > cnt_split:
                        not_resolved.append(annot)
        if not_resolved:
            msg = f"Для разрешения аннотации, используемой в модуле {module_name}, необходимо импортировать следующие классы: {not_resolved}"
            raise NameError(msg)
        return tuple(result)
    else:
        return tuple(
            t for t in [*get_args(annotated), get_origin(annotated)] if bool(t)
        )


_f: Callable[[type, Any, Parameter], bool] = (
    lambda target_type, _types, param: issubclass(target_type, _types)
    and param.kind
    in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
)


def _handle_generic(
    param: Parameter, annotation: Any, target_type: type
) -> bool:
    if annotation is None or not annotation:
        return False
    status: bool = _f(target_type, annotation, param)
    return status


def _check_input_sig(func: Callable) -> bool:
    overloads: Sequence[Callable] = get_overloads(func) or [func]
    for overload_sign in overloads:
        sig: Signature = signature(overload_sign)
        param: Parameter = list(sig.parameters.values())[0]
        try:
            annot = get_type_hints(overload_sign)[param.name]
        except Exception:
            module_name = str(func.__module__)
            annot = _resolve_annotation(param.annotation, module_name)
        if status := _handle_generic(param, annot, bytes):
            return status
    return False


def _check_output_sig(func: Callable) -> bool:
    overloads: Sequence[Callable] = get_overloads(func) or [func]
    for overload_sign in overloads:
        sig: Signature = signature(overload_sign)
        param: Parameter = list(sig.parameters.values())[1]
        try:
            annot = get_type_hints(overload_sign)[param.name]
        except Exception:
            module_name = str(func.__module__)
            annot = _resolve_annotation(param.annotation, module_name)
        if status := _handle_generic(param, annot, BytesIO):
            return status
    return False


def reg_input(extension: str, func: Callable, extra_args: dict[str, Any] = {}):
    if _check_input_sig(func):
        func = _update_sign(func, extra_args) if extra_args else func
    else:
        raise RuntimeError(
            "Первым аргументом функции ввода должен быть объект типа bytes"
        ) from None
    input_map[extension] = func


def reg_output(
    extension, func: Callable, extra_args: dict[str, Any] = {}
) -> None:
    if _check_output_sig(func):
        func = _update_sign(func, extra_args) if extra_args else func
    else:
        raise RuntimeError(
            "Вторым аргументом функции вывода должен быть объект типа BytesIO или совместимый с ним байтовый контейнер"
        ) from None
    output_map[extension] = func


def deserialize(
    content: bytes, format: str, extra_args: dict[str, Any] = {}
) -> "Proxied":
    func: Callable[..., Proxied] = input_map[format]
    dataobj: Proxied = func(content, **extra_args)
    return dataobj


def serialize(
    dataobj: object, format: str, extra_args: dict[str, Any] = {}
) -> bytes:
    byte_buf = BytesIO()
    func = output_map[format]
    func(dataobj, byte_buf, **extra_args)
    return byte_buf.getvalue()


Proxied = TypeVar("Proxied", covariant=True)


class DataProxyProto(Protocol[Proxied]):
    data_format: str

    def to_bytes(self) -> bytes:
        ...

    def convert(self, format: str) -> Self:
        ...

    def get_repr(self) -> Proxied:
        ...

    def __getattr__(self) -> Any:
        ...


class WrapperNewMixin(Generic[Proxied]):
    def __new__(cls: type, wrapped: Proxied) -> Proxied:
        new_cls: cls = cast(cls, type(wrapped))
        return super().__new__(new_cls)


class DataProxy(DataProxyProto, WrapperNewMixin[Proxied]):
    data_format: str

    def __new__(cls, content: bytes) -> Proxied:
        try:
            wrapped: Proxied = input_map[cls.data_format](content)
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
) -> type[DataProxyProto]:
    new_dtype: type[DataProxyProto] = type(
        format_name.capitalize(), (DataProxy,), {}
    )  # type: ignore[type]
    new_dtype.data_format = format_name
    if not format_name in input_map or not format_name in output_map:
        reg_input(format_name, input_func, extra_args_in)
        reg_output(format_name, output_func, extra_args_out)
    datatype_registry[format_name] = new_dtype
    return new_dtype


DataTypeInfo: TypeAlias = dict[str, dict[str, Any]]


def allowed_datatypes(
    *, display: bool = False, verbose: bool = False
) -> list[DataTypeInfo]:
    ndrows: list[DataTypeInfo] = [
        datatype_info(k, verbose) for k in datatype_registry.keys()
    ]
    if display:
        for row in ndrows:
            pprint(row, depth=2, sort_dicts=False)
    return ndrows


def datatype_info(datatype: str, verbose: bool = False) -> DataTypeInfo:
    info: DataTypeInfo = {
        datatype: {
            "type": datatype_registry.get(datatype, None),
            "output func": output_map.get(datatype, None),
            "input func": input_map.get(datatype, None),
            "data container": signature(
                input_map.get(datatype, None)
            ).return_annotation,
        }
    }
    if verbose:
        # FIXME: добить это место
        # добавляется источник кода, тип функции и т.п.
        ...
    return info


from jinja2 import Template

Placeholder = dict[str, Any]


class PathTemplate:
    def __init__(
        self,
        file_part: Placeholder,
        *,
        root_part: Placeholder = {},
        folder_part: Placeholder = {},
        delim: str = "_",
    ):
        self.root_part: Placeholder = root_part
        self.folder_part: Placeholder = folder_part
        self.file_part: Placeholder = file_part
        self.delim: str = delim

    def render_path(self):
        part_list: list[partial[str]] = [
            self.make_part(part, self.delim)
            for part in self._get_placeholders()
            if part
        ]
        part_str: list[str] = [part() for part in part_list]
        path: str | Path = r"/".join(part_str).replace(" ", self.delim)
        return str(path)

    def with_ext(self, ext: str) -> str:
        return self.render_path() + f".{ext}"

    def _get_placeholders(self) -> tuple[Placeholder, ...]:
        return tuple(
            v for v in self.__dict__.values() if v and isinstance(v, dict)
        )

    @staticmethod
    def make_part(placeholders: Placeholder, delim: str) -> partial[str]:
        start, end = "{{ ", " }}"
        parts: list[str] = []
        for key, value in placeholders.items():
            if callable(value):
                key = key + "()"
            s = start + key + end
            parts.append(s)
        res: str = delim.join(parts)
        tpl = Template(res)
        return partial(tpl.render, placeholders)

    def __str__(self):
        return self.render_path()


empty_eor_checker = lambda x: x


@dataclass
class DataProcPipeline:
    binding_id: int = field()
    functions: list[Callable] = field(default_factory=list)
    eor_checker: Callable = field(default=empty_eor_checker)
    timeout: int = field(default=20)

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
            for ctx in chain(*io_context_map.values())
            if ctx.ctx_id == self.binding_id
        ][0]
        return_annot: type = signature(func).return_annotation
        func_args_annot: list[type] = [
            param.annotation for param in signature(func).parameters.values()
        ]
        input_func = input_map[ctx.in_format]
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
            for ctx in chain(*io_context_map.values())
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


io_context_map: dict[str | int, list[_IOContext]] = defaultdict(list)


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
    io_context_map[id(target)].append(ctx)
    return ctx


if __name__ == "__main__":
    ...
    # import polars
    # overloadeds = get_overloads(polars.read_json) or [polars.read_json]
    # ann = signature(polars.read_csv).return_annotation
    # ann2 = list(signature(polars.DataFrame.write_csv).parameters.values())[1].annotation
    # print(_resolve_annotation(ann, str(polars.read_csv.__module__)))
    # print(_resolve_annotation(ann2, str(polars.DataFrame.write_csv.__module__)))
