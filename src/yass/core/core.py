from collections import defaultdict
from collections.abc import Callable
from inspect import isabstract, isclass
from itertools import chain
from types import MappingProxyType
from typing import Any, Protocol, TypeVar, runtime_checkable

__all__ = [
    "AYC",
    "SingletonMixin",
    "YassCore",
    "get_all_factories",
    "reg_type",
    "YassUndefined",
    "Undefined",
]


class SingletonMixin:
    """
    A mixin class that turns a class into a Singleton.

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
    """
    The base class for all other classes.

    """

    @classmethod
    def available_impl(cls: type) -> MappingProxyType[str, type[_AC]]:
        """
        The method returns all available implementations of the class from the class repository.

        Returns:
            MappingProxyType[str, type[_AC]]: child class factory proxy.
        """

        _cls_ns: MappingProxyType[str, type[_AC]] = _get_factory(cls)
        return _cls_ns


class _FactoryRepo(
    SingletonMixin, defaultdict[type[_AC], dict[str, type[_AC]]]
):
    """
    A factory repository that accumulates information about implementations of all classes.
    The repository keys are the corresponding base classes, and the values are a dictionary
    with string identifiers of implementations as keys and objects themselves as values.

    """


_FACTORY_REPO = _FactoryRepo(dict)
_FACTORY_REPO_TYPE = MappingProxyType[type[_AC], dict[str, type[_AC]]]


def reg_type(name: str) -> Callable[[type[_AC]], type[_AC]]:
    """
    A utility function that registers a class in the repository. The function analyzes the MRO of the class,
    isolates the parent class and fixes it as a grouping attribute in the repository (in other words, as a key),
    and then places the class in the “branch” of the repository under the given identifier. Abstract classes
    cannot be registered as implementations. Using this function is not necessary if there is no need to create
    a factory for the corresponding class.

    Args:
        name (str): identifier under which the class will be registered.

    Raises:
        AttributeError: thrown if such an identifier already exists in the repository.

    Returns:
        Callable[[type[_AC]], type[_AC]]: a wrapper that returns the registered class unchanged.
    """
    if name in list(chain(*[_FACTORY_REPO.values()])):
        msg = "Конфликт имен: такое имя уже зарегистрировано в репозитории классов."
        raise AttributeError(msg) from None

    def wrapped_subcls(_cls: type[_AC]) -> type[_AC]:
        search_area: tuple[type, ...] = _cls.__mro__[::-1]
        for _obj in filter(
            lambda x: issubclass(x, YassCore) and issubclass(_cls, x),
            search_area,
        ):
            if _obj is YassCore:
                continue
            _cls_ns = _FACTORY_REPO[_obj]
            if isabstract(_cls):
                break
            _cls_ns[name] = _cls
            break
        return _cls

    return wrapped_subcls


def _get_factory(id: object | type) -> MappingProxyType[str, type[_AC]]:
    """
    Returns a factory, defining the required "branch" of implementations for the object class.

    Args:
        id (object | type): the instance or class for which the factory search is being conducted.

    Raises:
        AttributeError: thrown if no factory for this class is found.

    Returns:
        MappingProxyType[str, type[_AC]]: child class factory proxy.
    """
    if not isclass(id):
        base_lst: list[type] = list(id.__class__.__mro__)[::-1]
        id = [
            cls
            for cls in filter(
                lambda x: issubclass(x, YassCore)
                and issubclass(id.__class__, x),
                base_lst,
            )
        ][0]
    try:
        repo: _FACTORY_REPO_TYPE = get_all_factories()
        return MappingProxyType(repo[id])
    except KeyError:
        msg = "Ни один класс не зарегистрирован под данным идентификатором."
        raise AttributeError(msg)


def get_all_factories() -> _FACTORY_REPO_TYPE:
    """
    Returns the factory repository proxy.

    """
    return MappingProxyType(_FACTORY_REPO)


_MC = TypeVar("_MC")

YassUndefined = object()
Undefined = Any

# @beartype
# def make_empty_instance(cls: type) -> _MC:
#     """
#     The conventional way to create instances of empty classes.

#     Args:
#         cls (type[_MC]): any class for which you need to create a proxy.

#     Returns:
#         _MC: a proxy instance that undergoes static type checking.
#     """
#     return EMPTY_INSTANCE


# @beartype
# def is_empty_instance(instance: _MC | type[_MC]) -> TypeGuard[_EmptyClass]:
#     """
#     Helper function to check that the provided instance is an empty class.
#     Can be used to improve the unambiguity of certain operations in the code.

#     Args:
#         instance (_MC | type[_MC]): instance of any class or any class.

#     Returns:
#         TypeGuard[_EmptyClass]: If the class instance is "empty", it will return True.
#     """
#     return hasattr(instance, "_empty")
