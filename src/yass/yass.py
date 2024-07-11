from __future__ import annotations

import asyncio
from asyncio import FIRST_COMPLETED, create_task, wait
from dataclasses import dataclass, field
from importlib import import_module
from threading import Thread
from types import ModuleType
from typing import TYPE_CHECKING, Literal, TypeVar

from yass.data_collectors import ApiDataCollector, BaseDataCollector
from yass.resources import ApiResource
from yass.resources.base import BaseResource
from yass.storages.base import BaseBufferableStorage

if TYPE_CHECKING:
    from asyncio import Task

    from yass.storages import FsBlobStorage

__all__ = ["EntryPoint"]

_AR = TypeVar("_AR", bound="BaseResource", covariant=True)
_AS = TypeVar("_AS", bound="BaseBufferableStorage", covariant=True)


@dataclass
class EntryPoint:
    lookup_interval: int = field(default=600)
    registred_resources: list = field(default_factory=list, init=False)
    debug_mode: bool = field(init=False, default=False)

    """
    The main class responsible for configuring, running and controlling 
    the application. Provides methods for registering resources and stores.

    Args:
        lookup_interval (int): the interval in seconds at which task completion is checked.
        registred_resources (list): list of registered resources. Resources store data about requests, for each of which a
                                    data collector is created.
        debug_mode (bool): debug mode indicator. Default is False.
    """

    def define_resource(
        self, *, resource_type: Literal["api"], url: str
    ) -> ApiResource:
        """
        The method allows you to create and register resources. At the moment, only the resource API implementation is available.

        Args:
            resource_type (Literal[&quot;api&quot;]): resource type. At the moment, only the API resource is available.
            url (str): As a rule, this is a full-fledged link without dynamic parts of the URL address.

        Returns:
            ApiResource: instance of api resource.
        """
        impl = BaseResource.available_impl()[resource_type]
        instance = impl(url)
        self.registred_resources.append(instance)
        return instance

    def define_storage(
        self, *, storage_type: Literal["blob"]
    ) -> FsBlobStorage:
        """
        The method creates a store. The storage configuration (limits, creds, and so on) is carried out after creating
        an instance of the class.
        At the moment, only storage with file system engines is available.

        Args:
            storage_type (Literal[blob]): storage type.

        Returns:
            FsBlobStorage: FsBlobStorage instance.
        """
        impl = BaseBufferableStorage.available_impl()[storage_type]
        instance = impl()
        return instance

    async def _collect_data(self) -> None:
        # Мы запускаем все триггеры на ожидание в конкрутентом исполнении и рекурсивно их перезапускаем
        """
        The method starts the work of data collectors and periodically checks their readiness.
        In the same method, errors are intercepted if the asyncio task fails with an error.
        """
        awaiting_tasks: list[Task] | set[Task] = [
            create_task(dc.start(), name=dc._name)
            for dc in self._prepare_collectors()
        ]
        while awaiting_tasks:
            done, pending = await wait(
                awaiting_tasks,
                timeout=self.lookup_interval,
                return_when=FIRST_COMPLETED,
            )
            print(f"Done is {done}, pending is {pending}")
            awaiting_tasks = pending
            for task in done:
                if task.exception() is None:
                    awaiting_tasks.add(task.result())
                else:
                    print(
                        f"Condition {task.get_coro()} finished execution with an error {task.exception()}"
                    )
                    task.cancel()

    def run(self, *, debug: bool = False) -> None:
        """
        Method for launching the application. After calling it, data collectors are created and launched.

        Args:
            debug (bool, optional): if True, then the application will start in debug mode and write a detailed log.
                                    Important! At the moment, logging is not implemented. Defaults to False.
        """
        self.debug_mode = debug
        threaded_loop = Thread(target=self._run_async)
        threaded_loop.start()
        threaded_loop.join()

    def _run_async(self) -> None:
        """
        The method starts the asyncio event loop after attempting to resolve the type of event loop available for use.
        """
        self._resolve_el_policy()
        asyncio.run(self._collect_data(), debug=self.debug_mode)

    def _resolve_el_policy(self) -> None:
        """
        The method tries to set the faster uvloop as the main event loop type. If this fails, the default event loop remains.

        """
        try:
            uvloop: ModuleType = import_module("uvloop")
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass

    def _prepare_collectors(self) -> list[BaseDataCollector]:
        """
        The method prepares data collectors for all requests that are generated in relation to registered resources.
        At the moment, data collectors are being created only for API resources.

        Returns:
            list (BaseDataCollector): list of data collectors.
        """
        collectors: list[BaseDataCollector] = list()
        for resource in self.registred_resources:
            if isinstance(resource, ApiResource):
                # FIXME: почему-то дочерний тип не воспринимается как субтип суперкласса (апи-ресурс несовместим с базовым ресурсом).
                # Нужно разобраться почему это происходит.
                collectors.extend(
                    ApiDataCollector(x, resource)  # type:ignore
                    for x in resource.queries.values()
                )
        return collectors
