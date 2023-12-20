from abc import ABC, ABCMeta
from asyncio import (
    FIRST_COMPLETED,
    Condition,
    create_task,
    gather,
    sleep,
    wait,
)
from inspect import isclass
from typing import get_type_hints

from attrs import define, field, fields

import meta

__all__ = ["YassActor", "YassService", "YassAttr"]


from collections import defaultdict
from collections.abc import Callable, MutableMapping
from threading import Lock
from typing import Any, ClassVar, get_args, get_origin

from attrs import define, field, fields

lock = Lock()


@define(slots=False)
class IOSettings:
    input_fmt: str = field(default="json")
    output_fmt: str = field(default="csv")
    path_temp: str = field(default="")
    schema_registry: dict = field(factory=dict)


@define(slots=False)
class CommonSettings:
    max_retry: int = field(default=3)
    req_delay: int = field(default=1)
    strict_schema_definition: bool = False


@define
class Settings:
    io: "IOSettings" = IOSettings()
    common: "CommonSettings" = CommonSettings()


import builtins

from dotted_dict import DottedDict


class Context(DottedDict):
    def __init__(self):
        self.settings = Settings()

    # TODO: вызывается только в двух случаях - из билдера и из приложения
    def bind_args(self):
        ctx_classes = list(self.values())
        ctx_classes.remove(self.settings)
        for yass_cls in ctx_classes:
            current_cls = yass_cls
            annotations: dict = {
                k: v
                for k, v in current_cls.get_annotations().items()
                if v not in builtins.__dict__.values()
            }
            maybe_attr = [
                c for c in self.values() if c != current_cls
            ]  # TODO: нужно добавить проверку что объект не является подклассом класса-аттрибута
            for attr, annotation in annotations.items():
                print(attr, annotation)
                for obj in maybe_attr:
                    mro = obj.__class__.mro()
                    if (
                        annotation in mro
                        or get_origin(annotation) in mro
                        or any(i in get_args(annotation) for i in mro)
                    ):
                        setattr(current_cls, attr, obj)
                        print(
                            f"Set attr {attr} as bind obj {obj.__class__.__name__}"
                        )


fabric_registry = {}


def create_instance(self, func, attr, cls_type, *args, **kwargs):
    impl = func(cls_type, *args, **kwargs)
    self.context[attr] = impl
    setattr(impl, "context", self.context)
    return impl


from functools import partialmethod


class EnvMeta(type):
    attr_registry = fabric_registry

    def __call__(cls, *args: Any, **kwds: Any) -> Any:
        instance = super().__call__(*args, **kwds)
        for attr in cls.attr_registry:
            method_name = "create_" + attr
            # TODO: нужно как-то сделать так чтобы метод подсвечивался при вызове и
            # заинжектить сигнатуру в функцию. Наверное, нужен будет свой дескриптор ибо бля ниче
            # не биндится как нужно - нам нужен метод с геттом и подсветкой синтаксиса.
            method = partialmethod(
                create_instance, cls.attr_registry[attr].build_class, attr
            )
            method.__name__ = method_name
            setattr(
                instance, method_name, method.__get__(instance, type(instance))
            )
        return instance


class EnvBuilder(metaclass=EnvMeta):
    def __init__(self, context):
        self.context: Context = context

    def check_ready(self):
        self.context.bind_args()


@define(slots=False)
class InitContext:
    container: ClassVar[dict] = field(default=defaultdict(list))

    def get(self, sign):
        return self.container[sign]

    def set(self, sign, instance):
        instance_set = self.container[sign]
        instance_set.append(instance)
        self.bind_args(instance_set)

    def set_initial_sign(self, sign):
        self.container[sign]

    def bind_args(self, container):
        # TODO: нам нужно зачищать контейнер от установленных аттрибутов, так как смысла нет их заново формировать
        for obj in container:
            annotations = obj.get_annotations()
            for attr, annotation in annotations.items():
                lst = [
                    c
                    for c in container
                    if get_base(c) == annotation
                    or get_base(c) == get_args(annotation)
                    or get_base(c) in get_args(annotation)
                ]
                if lst:
                    # TODO: нужно понять как обрабатывать случаи когда некоторых объектов будет больше одного,
                    # и какие аттрибуты можно ставить больше одного
                    setattr(obj, attr, lst) if get_origin(
                        annotation
                    ) is not None else setattr(obj, attr, lst[0])
                    # obj.__setattr__(attr, lst) if get_origin(
                    #     annotation) is not None else obj.__setattr__(attr, lst[0])

    @property
    def registred_urls(self):
        return [key for key in self.container.keys()]

    @property
    def url_args(self):
        return self.container


class KeyCounter(dict):
    def __getitem__(self, __key: Any) -> Any:
        if __key not in self:
            self[__key] = 1
        return super().__getitem__(__key)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        if __key in self:
            __value = self[__key]
            __value += 1
        else:
            __value = 1
        return super().__setitem__(__key, __value)

    def __iter__(self):
        return iter(self.keys())


@define(slots=False)
class ContextLock:
    counter: ClassVar[object] = field(factory=KeyCounter)

    current_obj: object | None = field(default=None)
    waiters: set = field(factory=set)
    barrier: int = field(default=0)

    def __get_base(self, instance):
        return get_base(instance)

    def __compare(self, instance):
        if instance == self.current_obj:
            print(
                f"{instance} is complimentation to {self.current_obj}. Continue working"
            )
            self.barrier += 1
            return True
        else:
            print(
                f"{instance} is not complimentation to {self.current_obj}. Stop working"
            )
            self.counter[instance] = True
            print(f"Barries of pending object is {self.counter}")
            return False

    async def __aenter__(self):
        print("Entering in context")
        return self

    async def watch(self, instance):
        if not self.current_obj:
            self.current_obj = self.__get_base(instance)
            print(f"Now current object is {self.current_obj}")
            self.barrier += 1
            print(f"Barrier for current object is {self.barrier}")
        else:
            waiter = self.__get_base(instance)
            if not self.__compare(waiter):
                print(
                    f"Append {instance} with base class {waiter} to the queue"
                )
                self.waiters.add(waiter)
                print(f"Waiters queue is {self.waiters}")
                await self.__wait(instance)
            else:
                print(
                    "Object not blocking because current object is complementared with instance"
                )
                print(f"Now barrier is {self.barrier}")

    async def __wait(self, inst_base):
        condition = Condition()
        print(f"{inst_base} pending for unlocking")
        async with condition:
            await condition.wait_for(
                lambda: self.current_obj not in self.waiters
            )
        print(f"{inst_base} is unlocking!")

    def __step(self):
        self.barrier -= 1
        print(f"After exit barrier is {self.barrier}")
        if self.barrier == 0:
            try:
                print(self.waiters)
                print(f"Old current object is {self.current_obj}")
                self.current_obj = self.waiters.pop()
                self.barrier = self.counter[self.current_obj]
            except (IndexError, KeyError):
                print("Waiters queue is empty")
                self.current_obj = None
                self.barrier = 0
            finally:
                print(
                    f"New current object is {self.current_obj}. Barrier of new object is {self.barrier}"
                )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.__step()


ctx_lock = ContextLock()
conditions_set = {}
conditions_instances = {}


@define(slots=False)
class MakedTrigger:
    # TODO: продвинутая версия триггера.
    # TODO: здесь будет контейнер условий, который наполняется после настройки триггера
    conditions: dict = field(default=conditions_instances)
    loop_timeout: int = field(default=60)
    # TODO: вообще эту функцию можно выкинуть - триггер будет контекстно-управляемым объектом, в контексте эта функция есть

    async def pending(self):
        # TODO: нужно где-то здесь перехватить исключение
        awaiting_tasks = [
            create_task(cond.pending()) for cond in self.conditions.keys()
        ]
        while awaiting_tasks:
            done, pending = await wait(
                awaiting_tasks,
                timeout=self.loop_timeout,
                return_when=FIRST_COMPLETED,
            )
            print(f"Done is {done}, pending is {pending}")
            awaiting_tasks = pending
            for task in done:
                if task.exception() is None:
                    key = task.result()
                    if key in self.conditions.keys():
                        callback = self.context_wrapper(self.conditions[key])
                        print(f"Triggered callback is {callback}")
                        awaiting_tasks.add(callback)
                        print(f"Awaiting tasks is {awaiting_tasks}")
                else:
                    # TODO: здесь будут обрабатываться ошибки, которые можно исправить
                    # и перезагрузить исполнение
                    print(
                        f"Condition {task.get_coro()} finished execution with an error {task.exception()}"
                    )
                    task.cancel()
        print("Start pending another conditions cycle")
        return await self.pending()

    def context_wrapper(self, coro):
        async def actor_controller():
            async with ctx_lock as lock:
                await lock.watch(coro.__self__)
                await coro()

        return create_task(actor_controller())

    def __hash__(self) -> int:
        return hash(id(self))


def get_base(instance_or_class):
    check_set = {YassActor, YassService}
    if not isclass(instance_or_class):
        bases = [
            c
            for c in instance_or_class.__class__.__bases__
            if c not in (ContextMixin, ABC, ABCMeta, object)
        ]
    else:
        bases = [
            c
            for c in instance_or_class.__bases__
            if c not in (ContextMixin, ABC, ABCMeta, object)
        ]
    for base in bases:
        if base in check_set:
            return (
                instance_or_class.__class__
                if not isclass(instance_or_class)
                else instance_or_class
            )
        elif len(check_set.intersection(base.__bases__)) > 0:
            return base
        else:
            return get_base(base)


initial_context = InitContext()
application_context = defaultdict(dict)

from inspect import signature


class BaseFabric:
    registry: dict

    @classmethod
    def build_class(cls, cls_type, *args, **kwargs):
        impl = cls.registry[cls_type]
        sig = signature(impl)
        return impl() if len(sig.parameters) == 0 else impl(*args, **kwargs)

    @classmethod
    def get_classes(cls):
        lst = [key for key in cls.registry]
        return lst

    @classmethod
    def new_class(cls, cls_type, impl):
        cls.registry[cls_type] = impl
        return


from inspect import isabstract
from typing import Protocol


class YassCore:
    # TODO: чисто теоретически данный класс должен создавать для классов-аттрибутов методы геттеры/сеттеры,
    # но подумаю потом как это реализовать
    context: dict  # контекст заполняется в момент биндинга аргументов

    def get_annotations(self):
        d = {}
        base_classes = [
            c for c in self.__class__.mro() if c not in (Protocol, object)
        ]
        for c in base_classes:
            try:
                d.update(**get_type_hints(c))
            except AttributeError:
                # object, at least, has no __annotations__ attribute.
                pass
        return d

    def __init_subclass__(cls, cls_type=None) -> None:
        if registry := getattr(cls, "_registry", False):
            if cls_type is None:
                raise AttributeError(
                    "Подкласс базового класса должен быть объявлен с типом класса"
                )
            if isabstract(cls):
                print(
                    f"Class {cls.__name__} is abstract class, registration is aborted"
                )
                return
            print(f"New class {cls.__name__} in registry")
            registry.new_class(cls_type, cls)
            return
        elif YassCore not in cls.__bases__:
            name = cls.__name__.lower()
            fabric_name = name.capitalize() + "Fabric"
            registr = dict()
            fabric = type(fabric_name, (BaseFabric,), {"registry": registr})
            fabric_registry[name] = fabric
            setattr(cls, "_registry", fabric)
            return

    @property
    def settings(self):
        return self.context.settings


@define(slots=False)
class ContextMixin:
    app_context: ClassVar[MutableMapping] = application_context
    init_context: ClassVar[InitContext] = initial_context

    def get_context(self, instance=False, rtrn_supercls=False):
        searching_cls = instance if instance else self
        if rtrn_supercls:
            key = self.find_supercls(searching_cls)
        else:
            key = self.find_param(searching_cls)
        if key:
            return key
        else:
            print(
                "No such instance in context, maybe searching class not support context operation"
            )
            return False

    def find_supercls(self, instance):
        current_context = self.app_context
        for key, value in current_context.items():
            last_key = key
            if not isinstance(value, dict) or last_key == instance:
                continue
            if instance in value.values():
                return last_key
        if instance in self.app_context.keys():
            return instance

        return False

    def find_param(self, instance):
        for key in self.app_context.keys():
            return self.app_context[instance]

        return False

    def __setattr__(self, __name: str, __value: Any):
        current_ctx = {__name: __value}
        self.app_context[self].update(current_ctx)
        if isinstance(getattr(type(self), __name, None), property):
            prop = getattr(self, __name)
            prop.__set__(self, __value)
            return
        return object.__setattr__(self, __name, __value)


@define(slots=False)
class YassActor(YassCore):
    trigger: MakedTrigger = field(factory=MakedTrigger)


@define(slots=False)
class YassService(YassCore):
    ...


@define(slots=False)
class YassAttr(YassCore):
    def __init_subclass__(cls) -> None:
        pass


if __name__ == "__main__":
    url = "https://bububu.com"

    class One(YassActor):
        meta = "faker"
        value = "I'm One!"

        async def job(self):
            await sleep(5)
            return self.value

        def __hash__(self) -> int:
            return hash(id(self))

    class Two(YassActor):
        meta = "faker"
        value = "I'm Two"

        async def job(self):
            await sleep(5)
            return self.value

        def __hash__(self) -> int:
            return hash(id(self))

    class ChildOne(One):
        pass

    class ChildTwo(Two):
        pass

    class Three(YassActor):
        meta = "faker"
        value = "I'm Three"

        async def job(self):
            await sleep(5)
            return self.value

        def __hash__(self) -> int:
            return hash(id(self))

    ctx_locker = ContextLock()

    async def context_controll(coro):
        async with ctx_lock as lock:
            await lock.watch(coro.__self__)
            await coro()

    async def test_lock(url):
        coros = [
            Two(url=url),
            ChildTwo(url=url),
            One(url=url),
            Three(url=url),
            ChildOne(url=url),
            ChildOne(url=url),
        ]
        locked_coros = [context_controll(coro.job) for coro in coros]
        tasks = [create_task(lcoro) for lcoro in locked_coros]
        group = gather(*tasks)
        await group

    # run(test_lock(url))
