from __future__ import annotations

from abc import abstractmethod
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Literal, Self, overload

from byteflows.core import ByteflowCore
from byteflows.scheduling import AlwaysRun

if TYPE_CHECKING:
    from aiohttp import ClientResponse

    from byteflows.contentio import IOContext
    from byteflows.scheduling import ActionCondition

__all__ = ["ApiEORTrigger", "BaseResource", "BaseResourceRequest"]


class BaseResourceRequest(ByteflowCore):
    """
    Base class for resource request objects. The request object forms part
    of the resource processing context and generates URLs to which the
    resource will be processed (the implementation of this functionality
    depends on what type of resource the request is being prepared for).
    Each resource can have an unlimited number of requests - it all depends
    on how the resource is logically divided and which parts of it are required by the user.

    Args:
        name (str): request name. Must be unique within a single resource.
        io_context (IOContext): I/O context instance. Specifies the actions that need to be performed with the data obtained as a
                                result of the request execution (in what format to deserialize, where to save, whether the information needs to be further processed, and so on).
        collect_interval (ActionCondition, optional): request activity interval. See ActionCondition for details. Defaults to AlwaysRun().
        has_pages (bool, optional): if True, then links to crawl the resource will be generated taking into account the paginator. Defaults to True.
    """

    def __init__(
        self,
        name: str,
        io_context: IOContext,
        collect_interval: ActionCondition = AlwaysRun(),
        has_pages: bool = True,
    ):
        self.name: str = name
        self.io_context: IOContext = io_context
        self.collect_interval: ActionCondition = collect_interval
        self.has_pages: bool = has_pages
        self.enable = True

    @abstractmethod
    async def gen_url(self) -> AsyncGenerator[str]:
        """
        The method creates a url generator to crawl the resource. The specific implementation
        of the method depends on the type of resource (there is a direct functional connection
        between the type of the request object and the type of the resource).

        Returns:
            AsyncGenerator (str, None): asynchronous link generator.
        """
        ...

    def get_io_context(self) -> IOContext:
        """
        The method returns the I/O context object assigned to the request instance.

        Returns:
            IOContext: instance of I/O context.
        """
        return self.io_context


class BaseResource(ByteflowCore):
    """
    Base class for resources. Essentially, the Resource class is an
    abstraction of the entry point for interacting with some real data source
    (in the case of Sfn, a network resource like a site or api). The class
    accumulates information about restrictions on the use of a data source,
    including the permissible frequency of accessing it, all planned queries
    to the source, and other information that forms the context for
    working with the data source.
    At the moment, only the resource API implementation is available.

    Args:
        url (str): url address of the resource. As a rule, this is a full-fledged link without dynamic parts of the URL address.
        delay (int | float, optional): delay before sending a request to a resource, in seconds. Defaults to 1.
        request_timeout (int | float, optional): timeout for requests to the resource. Defaults to 5.
    """

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
    def configure(self, **kwargs) -> Self:
        """
        The method replaces one or more class parameters and returns the updated class.
        """
        ...

    @abstractmethod
    def make_query(self, *args, **kwargs) -> BaseResourceRequest:
        """
        Factory method for creating request instances. The newly created object is
        registered in the instance attribute of the resource class.
        .

        Returns:
            BaseResourceRequest: instance of the request object.
        """
        ...

    @abstractmethod
    def get_query(self, name: str) -> BaseResourceRequest:
        """
        The method returns the registered instance of the request object by its identifier (name).

        Args:
            name (str): request name.

        Raises:
            KeyError: thrown if a request with the same name is not registered.

        Returns:
            BaseResourceRequest: instance of the request object.
        """
        ...

    @abstractmethod
    def delete_query(self, name: str) -> None:
        """
        The method removes a request from those registered by identifier (name).

        Args:
            name (str): request name.

        Raises:
            KeyError: thrown if a request with the same name is not registered.
        """
        ...


class ApiEORTrigger(ByteflowCore):
    """
    The base class of triggers for API resources. Triggers are used as an indicator of the end of the payload
    in a resource based on a given criterion. This sign can be either the value of the response header or some
    value contained in the content.
    """

    def __init__(self):
        self.search_type: Literal["content", "headers"]

    @overload
    @abstractmethod
    def is_end_of_resource(self, response: ClientResponse) -> bool: ...

    @overload
    @abstractmethod
    def is_end_of_resource(self, response: bytes) -> bool: ...

    @abstractmethod
    def is_end_of_resource(self, response: ClientResponse | bytes) -> bool:
        """
        The method returns True if the payload in the resource has ended. The implementation of a
        method depends on the validation rule inherent in the corresponding concrete class. Can
        accept a request response as either serialized content or an instance of the request response
        object.

        Args:
            response (ClientResponse | bytes): content or request object.

        Returns:
            bool: returns True if the payload in the resource has ended.
        """
        ...
