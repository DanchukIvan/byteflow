from asyncio import to_thread
from functools import update_wrapper, wraps
from inspect import iscoroutinefunction
from typing import Any

from base import conditions_instances
from conditions import conditions_set


class ProxyMethodDesc:
    def __init__(self, func):
        self.func = func
        self.obj = None
        print(f"Create trigger on {self.func}")

    def __get__(self, instance, owner=None):
        if self.obj is None:
            self.obj = instance
        self.func = self.func.__get__(self.obj, self.obj.__class__)
        return self

    def setup_trigger(self, *args, tr_type="time_condition", **kwargs):
        cond = conditions_set[tr_type](*args, **kwargs)
        conditions_instances[cond] = self.func
        # conditions_instances[cond] = partial(self.func, self.obj)

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.func(*args, **kwds)


def mark_as_trigger(func) -> "ProxyMethodDesc":
    proxy = ProxyMethodDesc(func)
    update_wrapper(proxy, func)
    return proxy


def to_async(func):
    @wraps(func)
    async def wrapped(*args, **kwargs):
        if not iscoroutinefunction(func):
            print("Make function asynchronus")
            return await to_thread(func, *args, **kwargs)
        else:
            print("Funcion yet is coroutine")
            return await func(*args, **kwargs)

    return wrapped


def register(container, class_object, name):
    container[name] = class_object


class TestThread:
    data: ProxyMethodDesc = 100

    @mark_as_trigger
    def printer(self, value):
        """Тестовая функция"""
        return value


# print(get_origin(t.__annotations__['data']))


# async def main(value):
#     t = TestThread()
#     t.printer.setup_trigger(period=1, start_time='13.00')
#     num = await t.printer(value)


# run(main(10))
