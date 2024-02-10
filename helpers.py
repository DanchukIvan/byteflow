import asyncio
import inspect
from asyncio import to_thread
from collections.abc import Awaitable, Callable
from functools import wraps
from inspect import iscoroutinefunction, isfunction
from typing import Any, Literal, ParamSpec, Self, TypeVar

T = TypeVar("T")
P = ParamSpec("P")

from dataclasses import dataclass


def to_async(func: Callable[P, T]) -> Callable[..., Awaitable[T]]:
    @wraps(func)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> Awaitable[T]:
        return to_thread(func, *args, **kwargs)

    return wrapped


def make_async(cls: type[Any]) -> type:
    members: list[tuple[str, Callable]] = inspect.getmembers(cls, isfunction)
    for name, meth in members:
        if not name.startswith("__"):
            if iscoroutinefunction(meth):
                continue
            setattr(cls, name, to_async(meth))  # type: ignore
    return cls


class MakeAsyncMixin:
    def __getattribute__(self: Self, __name: str) -> Awaitable | Any:
        attr: Any = object.__getattribute__(self, __name)
        if (
            not __name.startswith("__")
            and not iscoroutinefunction(attr)
            and callable(attr)
        ):
            return to_async(attr)
        return attr


SizeUnit = Literal[
    "b", "bytes", "kb", "kilobytes", "mb", "megabytes", "gb", "gigabytes"
]


def scale_bytes(sz: int | float, unit: SizeUnit) -> int | float:
    """Scale size in bytes to other size units (eg: "kb", "mb", "gb", "tb")."""
    if unit in {"b", "bytes"}:
        return sz
    elif unit in {"kb", "kilobytes"}:
        return sz / 1024
    elif unit in {"mb", "megabytes"}:
        return sz / 1024**2
    elif unit in {"gb", "gigabytes"}:
        return sz / 1024**3
    else:
        raise ValueError(
            f"`unit` must be one of {{'b', 'kb', 'mb', 'gb', 'tb'}}, got {unit!r}"
        )


if __name__ == "__name__":

    @dataclass
    class Example:
        boolish: bool = True

        @to_async
        def hellower(self) -> None:
            print("Hello!")

        @property
        def boolbool(self):
            return self.boolish

    async def test():
        e: Example = Example()
        await e.hellower()

    asyncio.run(test())
