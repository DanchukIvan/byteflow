from __future__ import annotations

from abc import abstractmethod
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Literal, Self, overload

from yass.core import YassCore

if TYPE_CHECKING:
    from aiohttp import ClientResponse

    from yass.contentio import IOContext
    from yass.scheduling import ActionCondition, AlwaysRun

__all__ = ["ApiEORTrigger", "BaseResource", "BaseResourceRequest"]


class BaseResourceRequest(YassCore):
    def __init__(
        self,
        name: str,
        base_url: str,
        io_context: IOContext,
        collect_interval: ActionCondition = AlwaysRun(),
        has_pages: bool = True,
    ):
        self.name: str = name
        self.base_url: str = base_url
        self.io_context = io_context
        self.collect_interval = collect_interval
        self.has_pages = has_pages
        self.enable = True

    @abstractmethod
    async def gen_url(self: Self) -> AsyncGenerator[str, None]: ...

    def get_io_context(self) -> IOContext:
        return self.io_context


class BaseResource(YassCore):
    def __init__(
        self,
        url: str,
        *,
        delay: int | float = 1,
        request_timeout: int | float = 5,
    ):
        self.url: str = url
        self.delay: int | float = delay
        self.request_timeout: int | float = request_timeout
        self.queries: dict[str, BaseResourceRequest] = dict()

    @abstractmethod
    def configure(self) -> Self: ...
    @abstractmethod
    def make_query(self) -> BaseResourceRequest: ...
    @abstractmethod
    def get_query(self, name: str) -> BaseResourceRequest: ...
    @abstractmethod
    def delete_query(self, name: str) -> None: ...


class ApiEORTrigger(YassCore):
    def __init__(self):
        self.search_type: Literal["content", "headers"]

    @overload
    @abstractmethod
    def is_end_of_resource(self, response: ClientResponse) -> bool: ...

    @overload
    @abstractmethod
    def is_end_of_resource(self, response: bytes) -> bool: ...

    @abstractmethod
    def is_end_of_resource(self, response: ClientResponse | bytes) -> bool: ...
