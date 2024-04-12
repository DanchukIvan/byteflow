from __future__ import annotations

from abc import abstractmethod
from asyncio import Task
from datetime import date
from time import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from rich.pretty import pprint as rpp

from yass.contentio import PathSegment, PathTemplate
from yass.core import YassCore
from yass.storages.base import BaseBufferableStorage

if TYPE_CHECKING:
    from yass.contentio import IOContext
    from yass.contentio.contentio import IOBoundPipeline
    from yass.resources import BaseResource, BaseResourceRequest
    from yass.scheduling.base import ActionCondition
    from yass.storages import ContentQueue

__all__ = ["BaseDataCollector"]


class BaseDataCollector(YassCore):
    def __init__(self, resource: BaseResource, query: BaseResourceRequest):
        self.delay: int | float = resource.delay
        self.timeout: int | float = resource.request_timeout
        self.collect_trigger: ActionCondition = query.collect_interval
        io_context: IOContext = query.io_context
        storage: BaseBufferableStorage = io_context.storage
        self.eor_status = False
        self._write_channel: ContentQueue = storage.create_buffer(query)
        self.url_series = query.gen_url
        self.pipeline: IOBoundPipeline = io_context.pipeline
        self.input_format: str = io_context.in_format
        self.output_format: str = io_context.out_format
        if io_context.path_temp:
            self.path_producer: PathTemplate = io_context.path_temp
        else:
            self.path_producer = self._make_default_path(resource, query)

    def _make_default_path(
        self, resource: BaseResource, query: BaseResourceRequest
    ) -> PathTemplate:
        rpp(
            "Генератор пути для сохранения файлов не задан. Будет сформирован дефолтный путь"
        )
        root = PathSegment("", 1, [urlparse(resource.url)[1]])
        folder = PathSegment("", 2, [query.name])
        file = PathSegment("_", 3, [date.today, query.name, time])
        path = PathTemplate([root, folder, file], is_local=False)
        rpp(
            f"Пример сформированного дефолтного пути: {path.render_path(self.output_format)}"
        )
        return path

    def _has_open_connect(self, task: Task):
        if task.exception() is None:
            self.storage_activiti_state = True
        else:
            msg = "Подключение к хранилищу завершилось ошибкой."
            raise RuntimeError(msg) from task.exception()

    @abstractmethod
    async def start(self):
        await self.collect_trigger.pending()
        while not self.storage_activiti_state:
            ...
        """Основной метод скрапинга, частный метод реализации которого зависит от типа паука"""

    @abstractmethod
    async def process_requests(self, urls: list[str]) -> list[bytes]: ...
