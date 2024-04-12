from __future__ import annotations

from asyncio import Event, Lock
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    MutableMapping,
    MutableSequence,
)
from itertools import count, zip_longest
from typing import TYPE_CHECKING, Any, Literal, Self, cast, overload

from aioitertools.itertools import product as async_product
from rich.pretty import pprint as rpp

from yass.core import make_empty_instance, reg_type
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
    "MaxPageEORTrigger",
    "StatusEORTrigger",
]

if TYPE_CHECKING:
    from aiohttp import ClientResponse

    from yass.contentio import IOContext
    from yass.scheduling import ActionCondition, AlwaysRun


@reg_type("api_req")
class ApiRequest(BaseResourceRequest):
    def __init__(
        self,
        name: str,
        base_url: str,
        persist_fields: MutableMapping[str, str],
        mutable_fields: MutableMapping[str, MutableSequence],
        io_context: IOContext,
        collect_interval: ActionCondition = AlwaysRun(),
        has_pages: bool = True,
    ) -> None:
        super().__init__(
            name, base_url, io_context, collect_interval, has_pages
        )
        self.persist_fields: MutableMapping[str, str] = persist_fields
        self.mutable_fields: MutableMapping[str, MutableSequence] = (
            mutable_fields
        )

    def get_io_context(self) -> IOContext:
        return self.io_context

    def change_interval(self, interval: ActionCondition):
        self.collect_interval: ActionCondition = interval

    @overload
    def set_persist_field(self: Self, params: tuple[str, str]) -> None: ...

    @overload
    def set_persist_field(self: Self, params: dict[str, str]) -> None: ...

    def set_persist_field(self: Self, params: Iterable) -> None:
        if isinstance(params, (list, tuple)):
            self.persist_fields.__setitem__(*params)
        else:
            self.persist_fields.update(params)

    @overload
    def set_mutable_field(self: Self, params: tuple[str, list]) -> None: ...

    @overload
    def set_mutable_field(self: Self, params: dict[str, list]) -> None: ...

    def set_mutable_field(self: Self, params: Iterable) -> None:
        if isinstance(params, (list, tuple)):
            self.mutable_fields.__setitem__(*params)
        else:
            self.mutable_fields.update(params)

    def _build_mf_iterator(
        self: Self,
    ) -> AsyncIterator[tuple[tuple[str, str], ...]]:
        mf_lst: list[Iterator[tuple[str, str]]] = [
            zip_longest([key], value, fillvalue=key)
            for key, value in self.mutable_fields.items()
        ]
        iterator: AsyncIterator[tuple[tuple[str, str], ...]] = async_product(
            *mf_lst
        )
        return iterator

    async def _build_mf_string(self: Self) -> AsyncGenerator[str, None]:
        if self.mutable_fields:
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

    async def _build_pf_string(self: Self) -> AsyncGenerator[str, Any]:
        if self.persist_fields:
            persist_part: str = "?" + "&".join(
                f"{name}={value}"
                for name, value in self.persist_fields.items()
            )
            yield persist_part
        else:
            yield ""

    async def build_full_query(self: Self) -> AsyncGenerator[str, Any]:
        async for mutable_part in self._build_mf_string():
            persist_part: str = await anext(self._build_pf_string())  # type:ignore
            full_string: str = persist_part + mutable_part
            yield full_string

    async def gen_url(self) -> AsyncGenerator[str, Any]:
        async for query_part in self.build_full_query():
            base = f"{self.base_url}{query_part}"
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


class MaxPageEORTrigger(ApiEORTrigger):
    def __init__(
        self,
        *,
        search_area: Literal["content", "headers"],
        current_page_field: str,
        max_page_field: str,
    ):
        self.fields: tuple[str, str] = (current_page_field, max_page_field)
        self.search_area: str = search_area
        self.content_handler: Callable = lambda x: x
        self.search_type = search_area

    def is_end_of_resource(self, response: ClientResponse | bytes) -> bool:
        if self.search_area == "headers":
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
        rpp(
            f"Обработано страниц {content[self.fields[0]]} из {content[self.fields[1]]}."
        )
        return int(content[self.fields[0]]) <= int(content[self.fields[1]])

    def set_content_handler(self, func: Callable) -> None:
        self.content_handler = func


class StatusEORTrigger(ApiEORTrigger):
    def __init__(self, status_code: int):
        self.stop_status = status_code
        self.search_type = "headers"

    def is_end_of_resource(self, response: ClientResponse) -> bool:
        status: int = response.status
        return status == self.stop_status


class ContentLengthEORTrigger(ApiEORTrigger):
    def __init__(self, min_content_length: int):
        self.stop_value = min_content_length
        self.search_type = "headers"

    def is_end_of_resource(self, response: ClientResponse) -> bool:
        headers = response.headers
        return int(headers.get("Content-Length")) <= self.stop_value  # type: ignore


class BatchCounter:
    def __init__(self, resource: ApiResource):
        self.barrier: int = resource.max_batch
        self._max_batch: int = resource.max_batch
        self.active_tasks: int = 0
        self.count_lock = Lock()
        self.zero_control = Event()

    @property
    def min_batch(self):
        return (
            self._max_batch // self.active_tasks or 1,
            self._max_batch % self.active_tasks or 1,
        )

    async def acquire_batch(self) -> int:
        self.active_tasks += 1
        rpp(
            f"Количество активных задач на текущий момент {self.active_tasks}."
        )
        # async with self.count_lock:
        async with self.count_lock:
            rpp("Ожидаю освобождения блокировок.")
            if self.barrier < min(self.min_batch):
                await self.zero_control.wait()
            rpp("Блокировки захвачены.")
            acquire_size: int = max(self.barrier, *self.min_batch)
            rpp(f"Захваченный лимит составляет {acquire_size}.")
            self.barrier = self.barrier - acquire_size
            rpp(f"Доступный лимит составляет {self.barrier}.")
        self.zero_control.clear()
        return acquire_size

    def release_batch(self, current_size: int, recalc: bool = False):
        self.barrier += current_size
        rpp(
            f"Высвободился лимит на {current_size} запросов. Доступный лимит составляет {self.barrier}."
        )
        if not recalc:
            self.active_tasks -= 1

    def recalc_limit(self, current_size: int) -> int:
        if not self.count_lock.locked() and self.barrier >= 0:
            new_batch: int = current_size + self.barrier
            self.barrier = 0
        else:
            standart_batch: int = min(self.min_batch)
            if (new_batch := current_size - standart_batch) > 0:
                rpp(
                    f"Пересчет размера батча. Текущий размер составляет {current_size}, избыток захваченных батчей составляет {standart_batch}."
                )
                self.release_batch(standart_batch, True)
                self.zero_control.set()
            else:
                new_batch = current_size
        return new_batch


_EMPTY_EOR_TRIGGER: ApiEORTrigger = make_empty_instance(ApiEORTrigger)


@reg_type("api")
class ApiResource(BaseResource):
    def __init__(
        self,
        url: str,
        *,
        extra_headers: dict = {},
        eor_triggers: list[ApiEORTrigger] = [_EMPTY_EOR_TRIGGER],
        max_batch: int = 1,
        delay: int | float = 1,
        request_timeout: int | float = 5,
    ):
        super().__init__(url, delay=delay, request_timeout=request_timeout)
        self.max_batch: int = max_batch
        self.batch = BatchCounter(self)
        self.extra_headers: dict = extra_headers
        self.eor_triggers: list[ApiEORTrigger] = eor_triggers

    def configure(
        self,
        *,
        extra_headers: dict | None = None,
        max_batch: int | None = None,
        delay: int | float | None = None,
        eor_triggers: list[ApiEORTrigger] | None = None,
    ) -> Self:
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

    def make_query(
        self,
        name: str,
        persist_fields: MutableMapping[str, str],
        mutable_fields: MutableMapping[str, MutableSequence],
        io_context: IOContext,
        collect_interval: ActionCondition = AlwaysRun(),
        has_pages: bool = True,
        replace: bool = False,
    ) -> ApiRequest:
        if name in self.queries and not replace:
            msg = "Запрос к ресурсу с таким именем уже существует. Если требуется заменить запрос, установите 'replace=True'."
            raise AttributeError(msg) from None
        query = ApiRequest(
            name,
            self.url,
            persist_fields,
            mutable_fields,
            io_context,
            collect_interval,
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
