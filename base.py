import warnings
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import KW_ONLY, dataclass
from functools import update_wrapper
from inspect import isabstract
from itertools import takewhile
from types import NoneType
from typing import (
    Any,
    Generic,
    ParamSpec,
    Protocol,
    Self,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    overload,
)

__all__ = ["YassCore", "NOCONVERTATTR"]

TYassClass = TypeVar("TYassClass", covariant=True)
PYassClass = ParamSpec("PYassClass")
factory_registry: dict[type, dict[str, type]] = defaultdict(dict)


@overload
def _get_init_args(var: type) -> tuple[None, None]:
    ...


@overload
def _get_init_args(var: type) -> tuple[type, None]:
    ...


@overload
def _get_init_args(var: type) -> tuple[type[Iterable] | type[Any], type[Any]]:
    ...


DescriptorArgs = (
    tuple[None, None]
    | tuple[type, None]
    | tuple[type[Iterable] | type[Any], type[Any]]
    | None
)


def _get_init_args(var: type) -> DescriptorArgs:
    new_args: list[Any] = []
    try:
        yass_base = YassCore
    except NameError:
        return None, None
    while var:
        if hasattr(var, "__args__"):
            if attrib := list(
                takewhile(
                    lambda x: YassCore in x.mro(), getattr(var, "__args__")
                )
            ):
                origin: Any = get_origin(var)
                if (
                    origin
                    and origin is not Union
                    and issubclass(origin, Iterable)
                ):
                    return attrib[0], origin  # type: ignore
            args = list(get_args(var))
            new_args.extend(args)
            var = new_args.pop()
        else:
            if YassCore in var.mro():
                return var, None
            if new_args:
                var = new_args.pop()
            else:
                return None, None


def _configure_attrs(attr_type: type) -> tuple[Any, Any]:
    _class, container = _get_init_args(attr_type)
    return _class, container


iterable_handlers: dict[type, Callable] = {}


def iter_handler(container_type: type[Iterable]) -> Callable[..., Any]:
    def registration(func: Callable) -> Callable:
        iterable_handlers[container_type] = func
        return func

    return registration


iter_handler(list)(lambda _list, value: _list.append(value))
iter_handler(dict)(lambda _dict, key, value: _dict.update({key: value}))
iter_handler(set)(lambda _set, value: _set.add(value))
iter_handler(tuple)(lambda _tuple, value: _tuple + (value,))


class NOCONVERT:
    """Константа - маркер, запрещающая конвертацию аттрибута, аннотированного типом YassCore,
    в фабрику"""


NOCONVERTATTR: object = NOCONVERT()

ClassRegistry = dict[type[TYassClass], dict[str, type[TYassClass]]]
SubclassRegistry = dict[str, type[TYassClass]]
ClassFactory = Callable[[str, str], Callable[[Any, Any], TYassClass]]
ClassInitializer = Callable[[Any, Any], TYassClass]


class FactoryField(Generic[TYassClass]):
    _registry: ClassRegistry = cast(ClassRegistry, factory_registry)

    def __init__(self):
        self._container: Iterable[TYassClass] | TYassClass | None = None
        self._concrete_factory: SubclassRegistry = {}
        self._name: str = ""

    def __set_name__(self, instance: object, name: str):
        if not self._name:
            self._name = name
            self._update_init(instance)

    @overload
    def __get__(self, instance, owner=None) -> Self:
        ...

    @overload
    def __get__(self, instance, owner=None) -> TYassClass:
        ...

    @overload
    def __get__(self, instance, owner=None) -> None:
        ...

    @overload
    def __get__(self, instance, owner=None) -> Iterable[TYassClass]:
        ...

    def __get__(
        self, instance, owner=None
    ) -> Self | Iterable[TYassClass] | TYassClass | None:
        if instance._edit_mode:
            instance._edit_mode = False
            return self
        return self._container

    def _update_init(self, instance: object) -> None:
        attr_annot: type = instance.__annotations__[self._name]
        _class, container = _configure_attrs(attr_annot)
        self._container = (
            container if not issubclass(container, Iterable) else container()
        )
        self._concrete_factory = factory_registry[_class]
        self._updated = True

    def __call__(
        self, subcls_key: str, bound_name: str = ""
    ) -> ClassInitializer[TYassClass]:
        _class: type[TYassClass] = self._concrete_factory[subcls_key]

        def wrapper(*args: Any, **kwargs: Any) -> TYassClass:
            attr_instance: TYassClass = _class(*args, **kwargs)
            self._update_container(attr_instance, bound_name)
            return attr_instance  # возвращаем объект для дальнейшей настройки/использования

        return update_wrapper(wrapper, _class)

    # TODO: нужно сделать более универсальную функцию
    def _update_container(self, ioc, bound_name):
        container_type = type(self._container)
        if container_type is not NoneType:
            func: Callable = iterable_handlers[container_type]
            if issubclass(container_type, dict) and bound_name:
                key: str | bool = getattr(ioc, bound_name, False)
                if key and not callable(key):
                    return func(self._container, key, ioc)
                else:
                    raise NameError(
                        "Необходимо задать имя аттрибута объекта, значение которого будет использоваться в качестве ключа"
                    )
            else:
                func(self._container, ioc)
        self._container = ioc


class YassCoreMeta(type):
    _registry: dict[type, dict[str, type]] = factory_registry

    def __new__(
        mcls,
        name: str,
        bases: tuple,
        namespace: dict[str, Any],
        *,
        subcls_key: str = "",
    ) -> type:
        instance: type = super().__new__(mcls, name, bases, namespace)
        if bases:
            mcls._update_registry(instance, subcls_key)
        return instance

    @classmethod
    def _update_registry(cls, new_class: type, subcls_key: str = ""):
        keys: list[type] = [
            key for key in cls._registry.keys() if issubclass(new_class, key)
        ]
        if keys and not isabstract(new_class):
            key = keys[0]
            if subcls_key:
                cls._registry[key][subcls_key] = new_class
            else:
                msg = f"Отсутствует subcls_key, подкласс {new_class} не будет зарегистрирован в регистре классов"
                warnings.warn(msg)
        else:
            cls._registry[new_class]


class YassMeta(type(Protocol), YassCoreMeta):
    ...


@dataclass
class YassCore(metaclass=YassMeta):
    _: KW_ONLY
    _edit_mode: bool = False

    @property
    def set(self) -> Self:
        self._edit_mode = True
        return self

    @property
    def available_impl(self):
        for superclass in factory_registry.keys():
            if issubclass(type(self), superclass):
                subcls_series: list[tuple[str, type]] = [
                    (k, v) for k, v in factory_registry[superclass].items()
                ]
                return subcls_series
            else:
                print("Not avaible implementation")
                return []


if __name__ == "__main__":
    ...
