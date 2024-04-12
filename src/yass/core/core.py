from collections import defaultdict
from collections.abc import Callable
from itertools import chain
from typing import (
    Any,
    Generic,
    Protocol,
    TypeGuard,
    TypeVar,
    cast,
    runtime_checkable,
)

from beartype import beartype

__all__ = [
    "AYC",
    "EmptyClass",
    "SingletonMixin",
    "YassCore",
    "get_all_factories",
    "get_factory",
    "is_empty_instance",
    "make_empty_instance",
    "reg_type",
]


class SingletonMixin:
    """
    A mixin that turns a class into a Singleton.

    Raises:
        RuntimeError: the error occurs when trying to create more than one instance of a class.
    """

    _instances: int = 0

    def __new__(cls, *args, **kwargs):
        """
        A modified new method in which the number of instances of the class is controlled via the _instances variable.
        """
        if cls._instances == 0:
            cls._instances += 1
            return super().__new__(cls)
        else:
            msg = f"Невозможно инициализировать более одного экземпляра {cls.__name__}"
            raise RuntimeError(msg) from None


_AC = TypeVar("_AC", bound="YassCore")
AYC = TypeVar("AYC", covariant=True)


@runtime_checkable
class YassCore(Protocol):
    @classmethod
    def available_impl(cls: type) -> dict[str, type[_AC]]:
        _cls_ns: dict[str, type[_AC]] = get_factory(cls)
        return _cls_ns


class _F_ACtoryRepo(
    SingletonMixin, defaultdict[type[_AC], dict[str, type[_AC]]]
): ...


_FACTORY_REPO = _F_ACtoryRepo(dict)


@beartype
def reg_type(name: str) -> Callable[[type[_AC]], type[_AC]]:
    if name in list(chain(*[_FACTORY_REPO.values()])):
        msg = "Конфликт имен: такое имя уже зарегистрировано в репозитории классов."
        raise AttributeError(msg) from None

    def wrapped_subcls(_cls: type[_AC]) -> type[_AC]:
        search_area = _cls.__mro__[::-1]
        for _obj in filter(lambda x: issubclass(x, YassCore), search_area):
            _cls_ns = _FACTORY_REPO[_obj]
            _cls_ns[name] = _cls
            break
        return _cls

    return wrapped_subcls


@beartype
def get_factory(id: str | type) -> dict[str, type[_AC]]:
    if isinstance(id, str):
        ns: dict[str, type[_AC]]
        for ns in filter(lambda x: id in x, _FACTORY_REPO.values()):
            return ns
    else:
        for key in filter(lambda x: issubclass(id, x), _FACTORY_REPO.keys()):
            return _FACTORY_REPO[key]
    msg = "Ни один класс не зарегистрирован под данным идентификатором."
    raise AttributeError(msg)


def get_all_factories() -> (
    defaultdict[type[YassCore], dict[str, type[YassCore]]]
):
    return _FACTORY_REPO


_MC = TypeVar("_MC")


class EmptyClass(Generic[_MC]):
    def __new__(cls, proxy: type[_MC]) -> _MC:
        instance = super().__new__(cls)
        new_cls: _MC = cast(proxy, instance)
        return new_cls

    def __init__(self, proxy: type[_MC]):
        self.empty = True

    def __bool__(self):
        return False

    def __getattribute__(self, __name: str) -> Any:
        msg = "Пустой класс является заглушкой и не имеет аттрибутов или методов."
        raise RuntimeError(msg) from None


@beartype
def make_empty_instance(cls: type[_MC]) -> _MC:
    return EmptyClass(cls)


@beartype
def is_empty_instance(instance: _MC | type[_MC]) -> TypeGuard[EmptyClass]:
    return getattr(instance, "empty", False)
