from __future__ import annotations

from abc import abstractmethod
from asyncio import Task
from collections.abc import AsyncGenerator, Callable
from datetime import date
from time import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from rich.pretty import pprint as rpp

from byteflows.contentio import PathTemplate
from byteflows.core import ByteflowCore
from byteflows.storages.base import BaseBufferableStorage

if TYPE_CHECKING:
    from byteflows.contentio import IOContext
    from byteflows.contentio.contentio import IOBoundPipeline
    from byteflows.resources import BaseResource, BaseResourceRequest
    from byteflows.scheduling.base import ActionCondition
    from byteflows.storages import ContentQueue

__all__ = ["BaseDataCollector"]


class BaseDataCollector(ByteflowCore):
    """
    Base data collector class. Data collectors are objects that directly make a request to a resource,
    call the data handler pipeline, and send data to the store. Data collectors take into account the
    restrictions associated with the resource, such as the rate limit, the interval for processing the
    resource, and the data format received when accessing the resource. For each instance of a request
    to a resource, a data collector is created.
    At the moment, only work with API resources is available.

    Attributes:
        delay (int | float): delay before sending requests.
        timeout (int | float): waiting time for a response from the data source.
        collect_trigger (ActionCondition): a trigger, the firing of which allows you to begin processing the data source.
        eor_status (bool): the status of no payload in the data source.
        _write_channel (ContentQueue): a buffer in memory in which data is stored before uploading to the backend.
        url_series (AsyncGenerator): a link generator created by a Request instance.
        pipeline (IOBoundPipeline): an instance of the pipeline class associated with a request that is processed by the data collector.
        input_format (str): format of incoming data.
        output_format (str): the format in which the data should be saved.
        path_producer (PathTemplate): data path generator.
    """

    def __init__(self, resource: BaseResource, query: BaseResourceRequest):
        """
        Args:
            query (ApiRequest): an instance of the request to the resource for which the data collector is being created.
            resource (ApiResource): a resource from which additional information is retrieved to initialize the data collector.
        """
        self._name: str = query.name
        self.delay: int | float = resource.delay
        self.timeout: int | float = resource.request_timeout
        self.collect_trigger: ActionCondition = query.collect_interval
        io_context: IOContext = query.io_context
        storage: BaseBufferableStorage = io_context.storage
        self.eor_status = False
        self._write_channel: ContentQueue = storage.create_buffer(query)
        self.url_series: Callable[..., AsyncGenerator[str]] = query.gen_url
        self.pipeline: IOBoundPipeline = io_context.pipeline
        self.input_format: str = io_context.in_format
        self.output_format: str = io_context.out_format
        if io_context.path_temp:
            self.path_producer: PathTemplate = io_context.path_temp
        else:
            self.path_producer = io_context.attache_pathgenerator()
            self.path_producer.add_segment("", 1, [urlparse(resource.url)[1]])
            self.path_producer.add_segment("", 2, [query.name])
            self.path_producer.add_segment(
                "_", 3, [date.today, query.name, time]
            )
            rpp(
                f"Пример сформированного дефолтного пути: {self.path_producer.render_path(self.output_format)}"
            )

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
