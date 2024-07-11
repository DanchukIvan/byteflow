from __future__ import annotations

from asyncio import Event, Lock
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Generator,
    Iterable,
    Iterator,
    MutableMapping,
    MutableSequence,
)
from functools import cached_property
from itertools import count, product, zip_longest
from typing import TYPE_CHECKING, Any, Literal, Self, cast, overload

from aioitertools.itertools import product as async_product

from yass.core import Undefined, YassUndefined, reg_type
from yass.resources.base import (
    ApiEORTrigger,
    BaseResource,
    BaseResourceRequest,
)

__all__ = [
    "ApiRequest",
    "ApiResource",
    "BatchCounter",
    "ContentLengthEORTrigger",
    "EndpointPath",
    "FixEndpointSection",
    "MaxPageEORTrigger",
    "MutableEndpointSection",
    "SimpleEORTrigger",
    "StatusEORTrigger",
]

if TYPE_CHECKING:
    from aiohttp import ClientResponse

    from yass.contentio import IOContext
    from yass.scheduling import ActionCondition, AlwaysRun


class FixEndpointSection:
    section_type: str = "fix"
    """
    The part of the endpoint that does not change from request to request.

    Attributes:
        prior (int): the position occupied by this part of the endpoint in the extended part of the request link to the resource.
        value (str): line with the name of the endpoint.
    """

    def __init__(self, value: str | list[str], prior: int = 0):
        """
        Args:
            prior (int): the position occupied by this part of the endpoint in the extended part of the request link to the resource.
            value (str): line with the name of the endpoint.
        """
        self.prior: int = prior
        self.value: Iterable[str] = (
            value if isinstance(value, list) else [value]
        )

    @cached_property
    def fix_url_part(self) -> str:
        """
        The property returns a string representation of the fixed part of the endpoint.

        Returns:
            str: endpoint part.
        """
        return "/".join(self.value)

    def __str__(self) -> str:
        return self.fix_url_part


class MutableEndpointSection:
    section_type: str = "mutable"
    """
    The variable part of the endpoint.

    Attributes:
        prior (int): the position occupied by this part of the endpoint in the extended part of the request link to the resource.
        value (str): line with the name of the endpoint.
    """

    def __init__(self, value: str | list[str], prior: int = 0):
        """
        Args:
            prior (int): the position occupied by this part of the endpoint in the extended part of the request link to the resource.
            value (str): line with the name of the endpoint.
        """
        self.prior: int = prior
        self.value: Iterable[str] = (
            value if isinstance(value, list) else [value]
        )
        self._current_gen: Generator[str, Any, None] | None = None

    def _make_mutable_url_part(self) -> Generator[str, Any, None]:
        """
        The method generates the next part of the endpoint.

        Yields:
            Generator: generator of string representations of endpoint parts.
        """
        yield from self.value

    def mutable_url_part(self) -> str:
        """
        The method returns part of the endpoint from the next iteration of the generator.

        Raises:
            StopIteration: thrown if the endpoint parts generator runs out.

        Returns:
            str: endpoint part.
        """
        if self._current_gen is None:
            self._current_gen = self._make_mutable_url_part()
        try:
            part: str = next(self._current_gen)
        except StopIteration:
            self._current_gen = None
            raise StopIteration
        return part

    def __str__(self) -> str:
        return self.mutable_url_part()


class EndpointPath:
    """
    The variable part of the endpoint.

    Attributes:
        base_url (str): the base url received from the resource class object.
        parts (list[FixEndpointSection | MutableEndpointSection]): list of fixed and mutable endpoint sections. Defaults to [].
        template (str): a final endpoint template used to expand the base url. Defaults to "".
        last_prior (int): the serial number of the last fixed element in the list of sections. Defaults to 0.
    """

    def __init__(self, id_name: str, base_url: str):
        """
        Args:
            id_name (str): endpoint name. Ideally it should be identical to the real endpoint.
            base_url (str): the base url received from the resource class object.
        """
        self.base_url: str = base_url
        self.parts: list[FixEndpointSection | MutableEndpointSection] = []
        self.template: str = ""
        self.last_prior = 0

    def add_fix_part(self, value: str | list[str], prior: int | None = 0):
        """
        The method adds a fixed section to the endpoint.

        Args:
            value (str): line with the name of the endpoint.
            prior (int | None, optional): the position occupied by this part of the endpoint in the extended part of the request link to the resource. Defaults to 0.
        """
        if not prior:
            prior = self.last_prior
        self.parts.append(FixEndpointSection(value, prior))
        self.last_prior += 1

    def add_mutable_parts(self, value: list[str], prior: int | None = 0):
        """
        The method adds a mutable section to the endpoint.

        Args:
            value (str): line with the name of the endpoint.
            prior (int | None, optional): the position occupied by this part of the endpoint in the extended part of the request link to the resource. Defaults to 0.
        """
        if not prior:
            prior = self.last_prior
        self.parts.append(MutableEndpointSection(value, prior))
        self.last_prior += 1

    def get_extended_base(self) -> Generator[str, Any, None]:
        """
        The method returns a url, which is an extended endpoint part of the base url.

        Yields:
            Generator: Link generator with endpoint part.
        """
        if not self.template:
            self.parts.sort(key=lambda x: x.prior)
            temp_details = [
                str(s) if s.section_type == "fix" else "{}" for s in self.parts
            ]
            self.template = f"{self.base_url}/" + "/".join(temp_details)
        mut_parts_gen = product(
            *[s.value for s in self.parts if s.section_type == "mutable"]
        )
        for mpart in mut_parts_gen:
            yield self.template.format(*mpart)


@reg_type("api_req")
class ApiRequest(BaseResourceRequest):
    """
    API resource request class.

    Attributes:
        name (str): request ID.
        endpoint (EndpointPath): the API endpoint that will be processed by this request.
        fix_params (MutableMapping[str, str]): HTTP request parameters that do not change from request to request. Defaults to YassUndefined.
        mutable_params (MutableMapping[str, MutableSequence]): HTTP request parameters that change from request to request. Defaults to YassUndefined.
        io_context (IOContext): I/O context instance. Specifies the actions that need to be performed with the data obtained as a result of the request execution (in what format to deserialize, where to save, whether the information needs to be further processed, and so on).
        collect_interval (ActionCondition, optional): request activity interval. See ActionCondition for details. Defaults to AlwaysRun().
        has_pages (bool, optional): if True, then the class will try to crawl the resource with the request parameters specified in the next generated url, page by page. Defaults to True.
    """

    def __init__(
        self,
        name: str,
        endpoint: EndpointPath,
        io_context: IOContext,
        collect_interval: ActionCondition = AlwaysRun(),
        fix_params: MutableMapping[str, str] | Undefined = YassUndefined,
        mutable_params: MutableMapping[str, MutableSequence]
        | Undefined = YassUndefined,
        has_pages: bool = True,
    ) -> None:
        """
        Args:
            name (str): request ID.
            endpoint (EndpointPath): the API endpoint that will be processed by this request.
            io_context (IOContext): I/O context instance. Specifies the actions that need to be performed with the data obtained as a
                                    result of the request execution (in what format to deserialize, where to save, whether the information needs to be further processed, and so on).
            collect_interval (ActionCondition, optional): request activity interval. See ActionCondition for details. Defaults to AlwaysRun().
            fix_params (MutableMapping[str, str]): HTTP request parameters that do not change from request to request. Defaults to YassUndefined.
            mutable_params (MutableMapping[str, MutableSequence]): HTTP request parameters that change from request to request. Defaults to YassUndefined.
            has_pages (bool, optional): if True, then the class will try to crawl the resource with the request parameters specified in the next generated url, page by page. Defaults to True.
        """
        super().__init__(name, io_context, collect_interval, has_pages)
        self.endpoint: EndpointPath = endpoint
        self.fix_params: MutableMapping[str, str] | Undefined = fix_params
        self.mutable_params: (
            MutableMapping[str, MutableSequence] | Undefined
        ) = mutable_params

    def get_io_context(self) -> IOContext:
        return self.io_context

    def change_interval(self, interval: ActionCondition) -> None:
        """
        The method replaces the original activity interval with a new one.

        Args:
            interval (ActionCondition): a new instance of the activity control class.
        """
        self.collect_interval: ActionCondition = interval

    @overload
    def set_persist_field(self, params: tuple[str, str]) -> None: ...

    @overload
    def set_persist_field(self, params: dict[str, str]) -> None: ...

    def set_persist_field(self, params: Iterable) -> None:
        """
        Setter for http request fields with a constant value.

        Args:
            params (dict|list|tuple): either a dictionary with one key-value pair, or a tuple or list with two elements.
        """
        if isinstance(params, (list, tuple)):
            self.fix_params.__setitem__(*params)
        else:
            self.fix_params.update(params)

    @overload
    def set_mutable_field(self, params: tuple[str, list]) -> None: ...

    @overload
    def set_mutable_field(self, params: dict[str, list]) -> None: ...

    def set_mutable_field(self, params: Iterable) -> None:
        """
        Setter for http request fields with changeable values.

        Args:
            params (dict|list|tuple): either a dictionary with one key-value pair, or a tuple or list with two elements.
        """
        if isinstance(params, (list, tuple)):
            self.mutable_params.__setitem__(*params)
        else:
            self.mutable_params.update(params)

    def _build_mf_iterator(self) -> AsyncIterator[tuple[tuple[str, str], ...]]:
        """
        The method creates an asynchronous iterator for fields with variable values. The constructed
        iterator returns a tuple of tuples, allowing you to build disjoint sets of http request parameters.

        Returns:
            AsyncIterator (tuple[tuple[str, str], ...]): tuple iterator with combinations of http request parameters.
        """
        mf_lst: list[Iterator[tuple[str, str]]] = [
            zip_longest([key], value, fillvalue=key)
            for key, value in self.mutable_params.items()
        ]
        iterator: AsyncIterator[tuple[tuple[str, str], ...]] = async_product(
            *mf_lst
        )
        return iterator

    async def _build_mf_string(self) -> AsyncGenerator[str, None]:
        """
        The method creates an asynchronous generator that generates a part of the url with variable request parameters.

        Returns:
            AsyncGenerator (str): query parameter string generator.
        """
        if isinstance(self.mutable_params, dict):
            iterator: AsyncIterator[tuple[tuple[str, Any | str], ...]] = (
                self._build_mf_iterator()
            )
            async for mutable_query in iterator:
                mutable_part: str = "".join(
                    f"&{name}={value}" for name, value in mutable_query
                )
                yield mutable_part
        else:
            yield ""

    async def _build_pf_string(self) -> AsyncGenerator[str, Any]:
        """
        The method creates an asynchronous generator that generates parts of the url with constant values ​​of the request parameters.

        Returns:
            AsyncGenerator (str): query parameter string generator.
        """
        if isinstance(self.fix_params, dict):
            persist_part: str = "?" + "&".join(
                f"{name}={value}" for name, value in self.fix_params.items()
            )
            yield persist_part
        else:
            yield ""

    async def _build_full_query(self) -> AsyncGenerator[str, Any]:
        """
        The method creates a generator that generates a complete string part of the url with the query parameters.
        This is the final stage of assembling the url part, which reflects the query parameters.

        Returns:
            AsyncGenerator (str): query parameter string generator.
        """
        persist_part: str = await anext(self._build_pf_string())
        async for mutable_part in self._build_mf_string():
            full_string: str = persist_part + mutable_part
            yield full_string

    async def gen_url(self) -> AsyncGenerator[str, Any]:
        for extended_url in self.endpoint.get_extended_base():
            async for query_part in self._build_full_query():
                base: str = f"{extended_url}{query_part}"
                if self.has_pages:
                    for page in count(1, 1):
                        url = f"{base}&page={page}"
                        sentinel: bool = yield url
                        if sentinel:
                            break
                else:
                    sentinel = yield base
                    if sentinel:
                        break


class SimpleEORTrigger(ApiEORTrigger):
    """
    A trigger that limits the time spent working with a resource by the number of processed links.

    Attributes:
        max_rounds (int): maximum number of links to process.
        current_rounds (int): number of links processed.
        search_type (str): search area (content or headings), in fact an implicit trigger type.
    """

    def __init__(self, max_rounds: int):
        """
        Args:
            max_rounds (int): maximum number of links to process.
        """
        self.max_rounds: int = max_rounds
        self.current_rounds: int = 0
        self.search_type = "headers"

    def is_end_of_resource(self, response: ClientResponse) -> bool:
        self.current_rounds += 1
        return self.current_rounds <= self.max_rounds


class MaxPageEORTrigger(ApiEORTrigger):
    """
    A trigger that looks for the maximum page attribute in the content or headers.
    The current page attribute is also looked up for matching.

    Attributes:
        fields (tuple[str, str]): a tuple containing the current and maximum page values.
        search_type (Literal["content", "headers"]): search area (content or headings), in fact an implicit trigger type.
        content_handler (Callable): a custom function that is used to extract the required fields from the content. By default, this is a lambda that returns the content as is.
    """

    def __init__(
        self,
        *,
        search_area: Literal["content", "headers"],
        current_page_field: str,
        max_page_field: str,
    ):
        """
        Args:
            search_area (Literal["content", "headers"]): alias for search_type. In this class, it acts as a method resolver,
                    since the search for the required fields can be carried out in different areas of the response to the request.
            current_page_field (str): attribute name with the value of the current page.
            max_page_field (str): the name of the attribute with the maximum page value.
        """
        self.fields: tuple[str, str] = (current_page_field, max_page_field)
        self.content_handler: Callable = lambda x: x
        self.search_type = search_area

    def is_end_of_resource(self, response: ClientResponse | bytes) -> bool:
        if self.search_type == "headers":
            response = cast(ClientResponse, response)
            return self._handle_headers(response)
        else:
            response = cast(bytes, response)
            return self._handle_content(response)

    def _handle_headers(self, response: ClientResponse) -> bool:
        headers = response.headers
        cur_page = cast(int, headers.get(self.fields[0]))
        max_pages = cast(int, headers.get(self.fields[1]))
        return int(cur_page) <= int(max_pages)

    def _handle_content(self, response: bytes) -> bool:
        content = self.content_handler(response)
        print(
            f"Обработано страниц {content[self.fields[0]]} из {content[self.fields[1]]}."
        )
        return int(content[self.fields[0]]) <= int(content[self.fields[1]])

    def set_content_handler(self, func: Callable) -> None:
        self.content_handler = func


class StatusEORTrigger(ApiEORTrigger):
    """
    A trigger that signals the end of a resource based on the status of the response.
    For example, the API may continue to accept requests, but return a 204 response code.
    The specified code can act as a control value for this trigger.

    Attributes:
        status_code (int): response control http code.
        search_type (Literal["content", "headers"]): search area (content or headings), in fact an implicit trigger type.
    """

    def __init__(self, status_code: int):
        """
        Args:
            status_code (int): response control http code.
        """
        self.stop_status: int = status_code
        self.search_type = "headers"

    def is_end_of_resource(self, response: ClientResponse) -> bool:
        status: int = response.status
        return status == self.stop_status


class ContentLengthEORTrigger(ApiEORTrigger):
    """
    A trigger that terminates interaction with a resource based on the length of the content.
    Not only zero length, but also any other value can act as a control value.

    Attributes:
        min_content_length (int): minimum content length in bytes. Used as a threshold - all values
                                equal to or below min_content_length cause the trigger to fire.
        search_type (Literal["content", "headers"]): search area (content or headings), in fact an implicit trigger type.
    """

    def __init__(self, min_content_length: int):
        """
        Args:
            min_content_length (int): minimum content length in bytes. Used as a threshold - all values
                                    equal to or below min_content_length cause the trigger to fire.
        """
        self.stop_value = min_content_length
        self.search_type = "headers"

    def is_end_of_resource(self, response: ClientResponse) -> bool:
        headers = response.headers
        return int(headers.get("Content-Length")) <= self.stop_value  # type: ignore


class BatchCounter:
    """
    The class is a special data structure for API resources that helps implement throttling - limiting
    the load on a resource. With BatchCounter, the resource request limit is constantly redistributed
    between running data collectors in order not to overload the resource with requests. Operations to
    change the limit counter occur atomically.

    Attributes:
        barrier (int): the remainder of the resource request limit.
        active_tasks (int): number of active date collectors. Used to redistribute the limit. number of active date collectors. Used to distribute the request limit.

    """

    def __init__(self, resource: ApiResource):
        """
        Args:
            resource (ApiResource): resource for which the limit counter will be created.
        """
        self.barrier: int = resource.max_batch
        self._max_batch: int = resource.max_batch
        self.active_tasks: int = 0
        self._count_lock = Lock()
        self._zero_control = Event()

    @property
    def min_batch(self) -> tuple[int, int]:
        """
        Minimum batch size range. Depends on the number of active data collectors. Cannot be less than one.

        Returns:
            tuple (int, int): boundaries of the norm of requests for one data collector.
        """
        return (
            self._max_batch // self.active_tasks or 1,
            self._max_batch % self.active_tasks or 1,
        )

    async def acquire_batch(self) -> int:
        """
        The method is used to capture part (in the case of several data collectors) or the entire
        limit (in the case of one running data collector) of requests.
        Control over the remainder of the limit is implemented on the principle of a
        semaphore - in the case of a zero balance, the data collector will not continue
        execution until another releases part of the limit.
        Each call to the method increases the active task counter by one.

        Returns:
            int: batch size.
        """
        self.active_tasks += 1
        print(
            f"Количество активных задач на текущий момент {self.active_tasks}."
        )
        # async with self.count_lock:
        async with self._count_lock:
            print("Ожидаю освобождения блокировок.")
            if self.barrier < min(self.min_batch):
                await self._zero_control.wait()
            print("Блокировки захвачены.")
            acquire_size: int = max(self.barrier, *self.min_batch)
            print(f"Захваченный лимит составляет {acquire_size}.")
            self.barrier = self.barrier - acquire_size
            print(f"Доступный лимит составляет {self.barrier}.")
        self._zero_control.clear()
        return acquire_size

    def release_batch(self, current_size: int) -> None:
        """
        Using this method, part of the resource request limit is released.

        Args:
            current_size (int): current batch size.
        """
        self.barrier += current_size
        print(
            f"Высвободился лимит на {current_size} запросов. Доступный лимит составляет {self.barrier}."
        )
        self.active_tasks -= 1

    def recalc_limit(self, current_size: int) -> int:
        """
        The method is used to override the available request limit. If excess limits
        have been captured (for example, one data collector starts earlier than all
        the others and captures the entire available limit), they will be released for
        redistribution between all running data collectors. And vice versa, if it is
        possible to select the entire balance of the limit, it will be used.

        Args:
            current_size (int): current batch size.

        Returns:
            int: updated available limit.
        """
        if not self._count_lock.locked() and self.barrier >= 0:
            new_batch: int = current_size + self.barrier
            self.barrier = 0
        else:
            standart_batch: int = min(self.min_batch)
            if (new_batch := current_size - standart_batch) > 0:
                print(
                    f"Пересчет размера батча. Текущий размер составляет {current_size}, избыток захваченных батчей составляет {standart_batch}."
                )
                self.barrier += standart_batch
                self._zero_control.set()
            else:
                new_batch = current_size
        return new_batch


"""
Stub for an API trigger (see “empty” class for more details).
"""


@reg_type("api")
class ApiResource(BaseResource):
    """
    Class - access point to the API resource.

    Attributes:
        url (str): api start url.
        extra_headers (dict, optional): additional headers. For example, these could be headers required for authorization in the API. Defaults to {}.
        eor_triggers (list[ApiEORTrigger], optional): a list of triggers for notifying about the end of a resource. Defaults to YassUndefined.
        max_batch (int, optional): the maximum number of requests to a resource. Most often, you can use the rate limit value of the api service for this parameter. Defaults to 1.
        delay (int | float, optional): delay before sending the next batch of requests. Defaults to 1.
        request_timeout (int | float, optional): the maximum waiting time for a response to a request. Applies to every single http request. Defaults to 5.
    """

    def __init__(
        self,
        url: str,
        *,
        extra_headers: dict = {},
        eor_triggers: list[ApiEORTrigger] | Undefined = YassUndefined,
        max_batch: int = 1,
        delay: int | float = 1,
        request_timeout: int | float = 5,
    ):
        """
        Args:
            url (str): api start url.
            extra_headers (dict, optional): additional headers. For example, these could be headers required for authorization in the API. Defaults to {}.
            eor_triggers (list[ApiEORTrigger] | Undefined, optional): a list of triggers for notifying about the end of a resource. Defaults to YassUndefined.
            max_batch (int, optional): the maximum number of requests to a resource. Most often, you can use the rate limit value of the api service for this parameter. Defaults to 1.
            delay (int | float, optional): delay before sending the next batch of requests. Defaults to 1.
            request_timeout (int | float, optional): the maximum waiting time for a response to a request. Applies to every single http request. Defaults to 5.
        """
        super().__init__(url, delay=delay, request_timeout=request_timeout)
        self.max_batch: int = max_batch
        self.endpoints: dict[str, EndpointPath] = {}
        self.batch = BatchCounter(self)
        self.extra_headers: dict = extra_headers
        self.eor_triggers: list[ApiEORTrigger] | Undefined = eor_triggers

    def configure(
        self,
        *,
        extra_headers: dict | None = None,
        max_batch: int | None = None,
        delay: int | float | None = None,
        eor_triggers: list[ApiEORTrigger] | None = None,
    ) -> Self:
        """
        The method replaces one or more class parameters and returns the updated class.
        """
        new_params = {
            key: value
            for key, value in locals().items()
            if key != "self" and value is not None
        }
        self.batch.barrier = cast(int, max_batch)
        default_params: dict[str, Any] = vars(self)
        default_params.update(new_params)
        for param, value in filter(
            lambda x: hasattr(self, x[0]), default_params.items()
        ):
            setattr(self, param, value)
        return self

    def add_endpoint(self, endpoint_id: str):
        endpoint = EndpointPath(endpoint_id, self.url)
        self.endpoints[endpoint_id] = endpoint
        return endpoint

    def make_query(
        self,
        name: str,
        endpoint: EndpointPath | str,
        io_context: IOContext,
        collect_interval: ActionCondition = AlwaysRun(),
        has_pages: bool = True,
        replace: bool = False,
        fix_params: MutableMapping[str, str] | Undefined = YassUndefined,
        mutable_params: MutableMapping[str, MutableSequence]
        | Undefined = YassUndefined,
    ) -> ApiRequest:
        if name in self.queries and not replace:
            msg = "Запрос к ресурсу с таким именем уже существует. Если требуется заменить запрос, установите 'replace=True'."
            raise AttributeError(msg) from None
        query = ApiRequest(
            name,
            endpoint
            if isinstance(endpoint, EndpointPath)
            else self.endpoints[endpoint],
            io_context,
            collect_interval,
            fix_params,
            mutable_params,
            has_pages,
        )
        self.queries[name] = query
        return query

    def get_query(self, name: str) -> ApiRequest:
        return self.queries[name]  # type:ignore

    def delete_query(self, name: str) -> None:
        del self.queries[name]

    def disable_query(self, name: str) -> None:
        self.queries[name].enable = False

    def enable_query(self, name: str) -> None:
        self.queries[name].enable = True


if __name__ == "__main__":
    ...
