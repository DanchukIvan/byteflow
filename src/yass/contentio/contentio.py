from __future__ import annotations

import os
from asyncio import Future, gather, to_thread
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from functools import reduce
from inspect import signature
from io import BytesIO
from itertools import chain
from pprint import pprint
from sys import platform
from typing import IO, TYPE_CHECKING, Any, Self, cast

if TYPE_CHECKING:
    from yass.storages import BaseBufferableStorage

from yass.contentio.common import *
from yass.contentio.helpers import *
from yass.core import Undefined, YassUndefined

__all__ = [
    "IOBoundPipeline",
    "IOContext",
    "PathSegment",
    "PathTemplate",
    "allowed_datatypes",
    "create_datatype",
    "create_io_context",
    "deserialize",
    "reg_input",
    "reg_output",
    "serialize",
]

"""
This module provides functions and classes responsible for input/output and serialization/deserialization of content.
"""


def reg_input(
    extension: str, func: Callable, extra_args: dict[str, Any] = {}
) -> None:
    """
    Registers a data deserialization function.

    Args:
        extension (str): the data format for which the function is intended (for example, json, csv, etc.).
        func (Callable): function for deserializing data.
        extra_args (dict[str, Any], optional): values of function arguments that need to be bound instead of the default ones. Defaults to {}.

    Raises:
        RuntimeError: thrown if the function fails validation. The error message indicates which part of the function is invalid.
    """
    if check_input_sig(func):
        func = update_sign(func, extra_args) if extra_args else func
    else:
        raise RuntimeError(
            "Первым аргументом функции ввода должен быть объект типа bytes"
        ) from None
    INPUT_MAP[extension] = func


def reg_output(
    extension, func: Callable, extra_args: dict[str, Any] = {}
) -> None:
    """
    Registers a data serialization function.

    Args:
        extension (str): the data format for which the function is intended (for example, json, csv, etc.).
        func (Callable): function for deserializing data.
        extra_args (dict[str, Any], optional): values of function arguments that need to be bound instead of the default ones. Defaults to {}.

    Raises:
        RuntimeError: thrown if the function fails validation. The error message indicates which part of the function is invalid.
    """
    if check_output_sig(func):
        func = update_sign(func, extra_args) if extra_args else func
    else:
        raise RuntimeError(
            "Вторым аргументом функции вывода должен быть объект типа BytesIO или совместимый с ним байтовый контейнер"
        ) from None
    OUTPUT_MAP[extension] = func


def deserialize(
    content: bytes, format: str, extra_args: dict[str, Any] = {}
) -> Any:
    """
    Deserializes byte content into an object of the specified format.

    Args:
        content (bytes): content received in byte representation.
        format (str): the format of the data that the resource provides.
        extra_args (dict[str, Any], optional): values of function arguments that need to be bound instead of the default ones. Defaults to {}.

    Raises:
        KeyError: thrown if there is no registered function of the given format.

    Returns:
        Any: data object of any type (for example, pandas df, polars df, dict from json, etc.).
    """
    func: Callable[[bytes, dict], Any] = INPUT_MAP[format]
    dataobj: Any = func(content, **extra_args)
    return dataobj


def serialize(
    dataobj: object, format: str, extra_args: dict[str, Any] = {}
) -> bytes:
    """
    _summary_

    Args:
        dataobj (object): data object of any type (for example, pandas df, polars df, dict from json, etc.).
        format (str): the target data format in which the object should be written. The required format is determined by the user.
        extra_args (dict[str, Any], optional): values of function arguments that need to be bound instead of the default ones. Defaults to {}.

    Raises:
        KeyError: thrown if there is no registered function of the given format.

    Returns:
        bytes: byte representation of content.
    """
    byte_buf = BytesIO()
    func: Callable[[Any, IO, dict], Any] = OUTPUT_MAP[format]
    func(dataobj, byte_buf, **extra_args)
    return byte_buf.getvalue()


def create_datatype(
    *,
    format_name: str,
    input_func: Callable,
    extra_args_in: dict = {},
    output_func: Callable,
    extra_args_out: dict = {},
    replace: bool = False,
) -> None:
    """
    Registers a new data format and functions for processing content of the specified format.

    Args:
        format_name (str): target data format.
        input_func (Callable): function for deserializing data.
        output_func (Callable): function for serializing data.
        extra_args_in (dict, optional): values of deserializing function arguments that need to be bound instead of the default ones. Defaults to {}.
        extra_args_out (dict, optional): the same for the serialization function.. Defaults to {}.

    Raises:
        RuntimeError: thrown if the data format is already registered.
    """
    if (
        not format_name in INPUT_MAP
        or not format_name in OUTPUT_MAP
        or replace
    ):
        reg_input(format_name, input_func, extra_args_in)
        reg_output(format_name, output_func, extra_args_out)
    else:
        msg = "Данный тип данных уже зарегистрирован"
        raise RuntimeError(msg)


_DataTypeInfo = dict[str, dict[str, Any]]


def allowed_datatypes(*, display: bool = False) -> list[_DataTypeInfo]:
    """
    Returns the currently registered and therefore available for processing data formats.

    Args:
        display (bool, optional): outputting a list of data formats to the console. Defaults to False.

    Returns:
        list[_DataTypeInfo]: list of available data formats.
    """
    ndrows: list[_DataTypeInfo] = [_datatype_info(k) for k in INPUT_MAP]
    if display:
        for row in ndrows:
            pprint(row, depth=2, sort_dicts=False)
    return ndrows


def _datatype_info(datatype: str) -> _DataTypeInfo:
    """
    Helper function for formatting information about registered content types.

    Args:
        datatype (str): name of the registered data type (for example, json, csv, etc).

    Raises:
        KeyError: thrown if the content type is not registered.

    Returns:
        _DataTypeInfo: a dictionary containing information about the format of the content.
    """
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
        msg = f"Тип данных {datatype} не зарегистрирован."
        raise KeyError(msg)


@dataclass
class PathSegment:
    """
    Is a representation of the path fragment where the retrieved data is stored.
    A collection of PathSegments represent the full path to the data storage location.

    Args:
        concatenator (str): literal through which parts of the segment will be combined.
        segment_order (int): the position that the segment will occupy along the path.
        segment_parts (list[str | Callable]): parts of the segment. There can be both callables that will be called to format a string
                                                (for example, functions from the datetime package), and standard strings. Defaults to [].

    Attributes:
        concatenator (str): literal through which parts of the segment will be combined.
        segment_order (int): the position that the segment will occupy along the path.
        segment_parts (list[str | Callable]): parts of the segment. There can be both callables that will be called to format a string
                                                (for example, functions from the datetime package), and standard strings.
    """

    concatenator: str
    segment_order: int = field(compare=True)
    segment_parts: list[str | Callable] = field(default_factory=list)

    def add_part(self, *part: str | Callable) -> None:
        """
        The method adds an arbitrary number of elements to the list of parts.
        The order of the elements matters to get the desired look of the path string.

        Args:
            part (str | Callable): strings or any functions. The argument accepts an unlimited number of parts.
        """
        self.segment_parts.extend(part)

    def change_concat(self, concatenator: str) -> Self:
        """
        The method replaces the concatenator with a new one.

        Args:
            concatenator (str): any string.

        Returns:
            Self: segment with updated concatenator.
        """
        kwds: dict[str, Any] = locals()
        return replace(kwds.pop("self"), **kwds)

    def __str__(self) -> str:
        str_represent: list[str] = list(
            map(lambda x: f"{x()}" if callable(x) else x, self.segment_parts)
        )
        return self.concatenator.join(str_represent)


class PathTemplate:
    """
    The class manages a collection of segments and is responsible for generating the full path to the data as a string.
    When generating the final path string, it takes into account the environment for which the path is being generated.
    The generated paths are the address where the data is stored in the storage.

    Attributes:
        segments (list[PathSegment], optional): list of path segments.
        is_local (bool, optional): if set to True, then PathTemplate will form the path using the operating system path separator.
    """

    def __init__(self, is_local: bool = False) -> None:
        """
        Args:
            is_local (bool, optional): if set to True, then PathTemplate will form the path using the operating system path separator. Defaults to False.
        """
        self.segments: list[PathSegment] = list()
        self.is_local: bool = is_local

    def add_segment(
        self,
        concatenator: str,
        segment_order: int,
        segment_parts: list[str | Callable[..., Any]],
    ) -> None:
        """
        A factory method that allows you to generate a path segment.
        The new segment is immediately added to the class field and is not returned.
        For a description of the arguments, see PathSegment.
        """
        self.segments.append(
            PathSegment(concatenator, segment_order, segment_parts)
        )

    def render_path(self, ext: str = "") -> str:
        """
        Generates and returns the data path. The optional ext parameter is used to add an extension to the path.

        Args:
            ext (str, optional): data format identifier. Defaults to "".

        Returns:
            str: path to data with or without extension.
        """
        self.segments.sort(key=lambda x: x.segment_order)
        nonull_segments = map(
            lambda x: str(x),
            filter(lambda x: x.__str__() != "", self.segments),
        )
        nonull_segments: Iterable[str] = cast(Iterable[str], nonull_segments)
        if self.is_local or (platform == "linux" and not self.is_local):
            sep: str = os.sep
        elif platform == "win32" and not self.is_local:
            sep = cast(str, os.altsep)
        return (
            sep.join(nonull_segments) + f".{ext}"
            if ext
            else sep.join(nonull_segments)
        )


@dataclass
class IOBoundPipeline:
    """
    The class is responsible for registering handler functions and applying them to incoming data.
    Creates a separate thread for processing to avoid blocking the event loop.
    The data is processed in batches, the size of which depends on the settings of the resource involved
    and the current load on the resource. Each pipeline is inextricably linked to an I/O context, and
    for any such context there can only be one pipeline.

    Attributes:
        io_context (IOContext): an I/O context object to which the pipeline will be associated.
        functions (list[Callable]): list of registered functions. The order of the functions in the list directly indicates the order in which they are tried on.
        on_error (Callable): exception catching function. Can be used for specific exception handling. By default, it is a lambda function that returns
                            any object passed to it unchanged.
        data_filter (Callable): a function for filtering content for invalid blocks. Registered via the appropriate method. By default, it is a lambda function
                            that returns any object passed to it unchanged.

    Args:
        io_context (IOContext): an I/O context object to which the pipeline will be associated.
        functions (list[Callable]): list of registered functions. The order of the functions in the list directly indicates the order in which they are tried on.
        on_error (Callable): exception catching function. Can be used for specific exception handling. By default, it is a lambda function that returns
                            any object passed to it unchanged.
        data_filter (Callable): a function for filtering content for invalid blocks. Registered via the appropriate method. By default, it is a lambda function
                            that returns any object passed to it unchanged.
    """

    io_context: IOContext
    functions: list[Callable] = field(default_factory=list)
    on_error: Callable = lambda x: x
    data_filter: Callable = lambda x: True

    def step(
        self, order: int, *, extra_kwargs: dict[str, Any] = {}
    ) -> Callable:
        """
        Method for registering a handler function.

        Args:
            order (int): position of the function in the pipeline. This argument ultimately determines the order in
                        which handlers are applied to the data.
            extra_kwargs (dict[str, Any], optional): additional handler function arguments. Under the hood, _update_sign is applied. Defaults to {}.

        Returns:
            Callable: function registered as a data handler without modification.
        """

        def wrapper(func):
            self._check_sig(func)
            updated_func: Callable = update_sign(
                func, extra_kwargs=extra_kwargs
            )
            self.functions.insert(order - 1, updated_func)
            return func

        return wrapper

    def content_filter(self, func: Callable[..., bool]) -> None:
        """
        The method registers a function that is used to filter content. Such a function must return a boolean.

        Args:
            func (Callable[..., bool]): any function containing user-relevant logic for validating content.
        """
        self.data_filter = func

    @asynccontextmanager
    async def run_transform(
        self, dataobj: Iterable[Any]
    ) -> AsyncIterator[Future]:
        """
        The method starts the data processing process. At this stage, content is validated and filtered,
        as well as its transformation in a separate thread.

        Args:
            dataobj (Iterable[Any]): batch with data objects of any type.

        Yields:
            AsyncIterator[Future]: asyncio futures (in the form of an asynchronous iterator) to wait for the processing of a data batch to complete.
        """
        valid_content: list[Any] = [
            data for data in dataobj if self.data_filter(data)
        ]
        try:
            coros = []
            for data in valid_content:
                coro: Coroutine[Any, None, Any] = to_thread(
                    reduce, lambda arg, func: func(arg), self.functions, data
                )
                coros.append(coro)
            yield gather(*coros)
        except Exception as exc:
            res: Any = self.on_error(exc)
            if isinstance(res, Exception):
                raise res

    # TODO: нужно проверять, что тип возвращаемого значения функции обработки есть в аргументах функции десериализации входящих значений
    def _check_sig(self, func: Callable) -> None:
        """
        Utility method for checking the compatibility of registered processing functions.
        This check is carried out by analyzing the signatures of registered functions.

        Args:
            func (Callable): a function registered as a handler.

        Raises:
            ValueError: thrown if the signature of the input (serialization) function and the handler function are incompatible.
                        The discrepancy can be found in both the arguments and the return value.
        """
        ctx: IOContext = self.io_context
        return_annot: type = signature(func).return_annotation
        if isinstance(return_annot, str):
            return_annot = resolve_annotation(return_annot, func)[0]
        func_args_annot: list[type] = list(
            chain(
                *[
                    resolve_annotation(param.annotation, str(func.__module__))
                    for param in signature(func).parameters.values()
                ]
            )
        )
        input_func = INPUT_MAP[ctx.in_format]
        return_input_annot = resolve_annotation(
            signature(input_func).return_annotation, str(input_func.__module__)
        )
        valid_annot: list[type] = [
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
        """
        The method changes the position of the function in the pipeline.

        Args:
            old_idx (int): old position.
            new_idx (int): new position.
        """
        func: Callable = self.functions.pop(old_idx)
        self.functions.insert(new_idx, func)

    def show_pipeline(self) -> str:
        """
        The method returns a string describing the chain of function calls in the pipeline.

        Returns:
            str: a chain of function calls as a string.
        """
        ctx: IOContext = self.io_context
        pipe: list[str] = [
            f"{order}: {func.__name__}"
            for order, func in enumerate(self.functions)
        ]
        return " -> ".join(pipe) + f" for {ctx.in_format}."


class IOContext:
    """
    The I/O context class specifies the format of incoming and outgoing data (which may or may not be the same -
    it all depends on the user's tasks) and also where the corresponding data is stored. Instances of this class
    are required to create requests for resources and register data processing pipelines.
    The context object also stores information about the data path pattern.

    Attributes:
        in_format (str): format of incoming data.
        out_format (str): the format in which the data should be saved.
        storage (BaseBufferableStorage): storage in which the data will be stored.
        path_temp (PathTemplate): path generator for storing data in storage. See PathTemplate for details.
        pipeline (IOBoundPipeline): a pipeline object initiated within the current I/O context. See IOBoundPipeline for details.
    """

    def __init__(
        self,
        *,
        in_format: str,
        out_format: str,
        storage: BaseBufferableStorage,
    ) -> None:
        """
        Args:
            in_format (str): format of incoming data.
            out_format (str): the format in which the data should be saved.
            storage (BaseBufferableStorage): storage in which the data will be stored.
        """
        self.in_format: str = in_format
        self.out_format: str = out_format
        self._check_io()
        self.storage: BaseBufferableStorage = storage
        self.path_temp: PathTemplate | Undefined = YassUndefined
        self.pipeline: IOBoundPipeline | Undefined = YassUndefined

    @property
    def out_path(self) -> str:
        """
        Path to save data with extension.

        Returns:
            str: path to the data in the storage.
        """
        return self.path_temp.render_path(self.out_format)

    def attache_pipeline(self) -> IOBoundPipeline:
        """
        Method creates and links a data processing pipeline.

        Returns:
            IOBoundPipeline: pipeline instance.
        """
        self.pipeline = IOBoundPipeline(self)
        return self.pipeline

    def attache_pathgenerator(self, is_local: bool = False) -> PathTemplate:
        """
        The method creates and binds an instance of the data path template.

        Returns:
            PathTemplate: data path template instance.
        """
        path_temp = PathTemplate(is_local)
        self.path_temp = path_temp
        return path_temp

    def _check_io(self) -> bool:
        """
        A utility function that checks that an instance of a class can be created with the specified data formats.

        Raises:
            ValueError: thrown if the input and/or output data format is not registered.

        Returns:
            bool: returns True if all specified data formats are registered.
        """
        try:
            in_obj = INPUT_MAP[self.in_format]
            out_obj = INPUT_MAP[self.out_format]
        except KeyError as exc:
            raise ValueError(
                f"Не зарегистрирован тип данных {exc.args}."
            ) from exc
        return True

    def update_ctx(
        self,
        *,
        in_format: str | None = None,
        out_format: str | None = None,
        storage: BaseBufferableStorage | None = None,
    ) -> Self:
        """
        The method updates context attributes.
        """
        default_params = vars(self)
        kwds: dict[str, Any] = {
            k: v for k, v in locals().items() if v is not None
        }
        kwds.pop("self")
        default_params.update(kwds)
        for attr, value in default_params.items():
            setattr(self, attr, value)
        return self


def create_io_context(
    *, in_format: str, out_format: str, storage: BaseBufferableStorage
) -> IOContext:
    """
    Module level function for creating IO context instances. Accepts the arguments necessary to initialize objects of this type.

    Returns:
        IOContext: new instance of IOContext.
    """
    kwargs: dict[str, Any] = {k: v for k, v in locals().items()}
    ctx = IOContext(**kwargs)
    return ctx
