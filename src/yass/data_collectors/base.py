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
    """
    Base data collector class. Data collectors are objects that directly make a request to a resource,
    call the data handler pipeline, and send data to the store. Data collectors take into account the
    restrictions associated with the resource, such as the rate limit, the interval for processing the
    resource, and the data format received when accessing the resource. For each instance of a request
    to a resource, a data collector is created.
    At the moment, only work with API resources is available.

    Args:
        query (ApiRequest): an instance of the request to the resource for which the data collector is being created.
        resource (ApiResource): a resource from which additional information is retrieved to initialize the data collector.
    """

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
        """
        The method generates a default path template, consisting of the resource url, request
        name and a compound string (date, request name, timestamp) if no path generation template
        is specified for the request to the resource.
        There is a high probability that the generated template will not meet the user's needs.
        This method is called only when the data collector is initialized.

        Args:
            resource (BaseResource): a resource from which additional information is retrieved to initialize the data collector.
            query (BaseResourceRequest): an instance of the request to the resource for which the data collector is being created.

        Returns:
            PathTemplate: automatically generated path template.
        """
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

    @abstractmethod
    async def start(self) -> Task:
        """
        Entry point for running the data collector. The method starts the procedure
        for crawling the resource according to the parameters of the request sent to the data collector.
        The method must return itself, wrapped in an asyncio task.
        """
        await self.collect_trigger.pending()
        await self._write_channel.storage.launch_session()
        ...

    @abstractmethod
    async def process_requests(self, urls: list[str]) -> list[bytes]:
        """
        The method sends requests to the list of urls passed as a parameter. The method must return a batch
        of serialized content. It is also assumed that this method will also implement the necessary checks
        (for example, whether the resource has expired or not, whether all response codes suit us, etc.)

        Args:
            urls (list[str]): list of urls to process.

        Returns:
            list (bytes): _description_
        """
        ...
