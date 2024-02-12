from collections.abc import Callable
from functools import update_wrapper
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from base import ActionCondition

__all__: list[str] = ["as_trigger", "ACTIVE_CONDITIONS"]


class Conditions(dict[ActionCondition, Callable]):
    ...


ACTIVE_CONDITIONS = Conditions()


class _ProxyMethodDescriptor:
    def __init__(self: Self, func: Callable) -> None:
        self.func: Callable = func
        self.obj: object | None = None
        print(f"Create trigger on {self.func}")

    def __get__(self, instance: object, owner: type | None = None) -> Self:
        if self.obj is None:
            self.obj = instance
        self.func = self.func.__get__(self.obj, self.obj.__class__)
        return self

    def setup_trigger(self, condition: ActionCondition) -> None:
        ACTIVE_CONDITIONS[condition] = self.func

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.func(args, kwds)


def as_trigger(func) -> _ProxyMethodDescriptor:
    proxy = _ProxyMethodDescriptor(func)
    update_wrapper(proxy, func)
    return proxy
