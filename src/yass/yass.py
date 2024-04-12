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
    from asyncio import AbstractEventLoop, Task

    from yass.storages import FsBlobStorage

__all__ = ["EntryPoint"]

_AR = TypeVar("_AR", bound="BaseResource", covariant=True)
_AS = TypeVar("_AS", bound="BaseBufferableStorage", covariant=True)


@dataclass
class EntryPoint:
    lookup_interval: int = field(default=600)
    registred_resources: list = field(default_factory=list, init=False)
    debug_mode: bool = field(init=False, default=False)

    def define_resource(
        self, *, resource_type: Literal["api"], url: str
    ) -> ApiResource:
        impl = BaseResource.available_impl()[resource_type]
        instance = impl(url)
        self.registred_resources.append(instance)
        return instance

    def define_storage(
        self, *, storage_type: Literal["blob"]
    ) -> FsBlobStorage:
        impl = BaseBufferableStorage.available_impl()[storage_type]
        instance = impl()
        return instance

    async def _collect_data(self) -> None:
        # Мы запускаем все триггеры на ожидание в конкрутентом исполнении и рекурсивно их перезапускаем
        awaiting_tasks: list[Task] | set[Task] = [
            create_task(dc.start()) for dc in self._prepare_collectors()
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
                    await task

    def run(self, *, debug: bool = False) -> None:
        self.debug_mode = debug
        threaded_loop = Thread(target=self._run_async)
        threaded_loop.start()

    def _run_async(self):
        loop: AbstractEventLoop = self._resolve_el_policy()
        asyncio.run(self._collect_data(), debug=self.debug_mode)

    def _resolve_el_policy(self) -> AbstractEventLoop:
        try:
            uvloop: ModuleType = import_module("uvloop")
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass
        finally:
            loop: AbstractEventLoop = asyncio.new_event_loop()
            return loop

    def _prepare_collectors(self) -> list[BaseDataCollector]:
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
