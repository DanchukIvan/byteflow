from collections import defaultdict
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Generator,
    Iterable,
    Iterator,
)
from dataclasses import dataclass, field
from functools import partial
from itertools import zip_longest
from typing import Any, NoReturn, Self, overload

from aioitertools.itertools import product as async_product

from .base import BaseResource


@dataclass
class ApiRequest:
    part_name: str = field()
    persist_fields: dict[str, str] = field(default_factory=dict)
    mutable_fields: dict[str, list] = field(
        default_factory=partial(defaultdict, list)
    )

    @overload
    def set_persist_field(self: Self, params: tuple[str, str]) -> None:
        ...

    @overload
    def set_persist_field(self: Self, params: dict[str, str]) -> None:
        ...

    def set_persist_field(self: Self, params: Iterable) -> None:
        if isinstance(params, (list, tuple)):
            self.persist_fields.__setitem__(*params)
        else:
            self.persist_fields.update(params)

    @overload
    def set_mutable_field(self: Self, params: tuple[str, list]) -> None:
        ...

    @overload
    def set_mutable_field(self: Self, params: dict[str, list]) -> None:
        ...

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
            iterator: AsyncIterator[
                tuple[tuple[str, Any | str], ...]
            ] = self._build_mf_iterator()
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


@dataclass
class ApiResource(BaseResource, subcls_key="api"):
    current_query: ApiRequest | None = field(default=None)
    queries: dict[str, ApiRequest] = field(default_factory=dict)

    @overload
    def get_request(
        self, *, name: str, autocreate: bool = False
    ) -> ApiRequest | NoReturn:
        ...

    @overload
    def get_request(self, *, name: str, autocreate: bool = True) -> ApiRequest:
        ...

    def get_request(self, *, name: str, autocreate: bool = True) -> ApiRequest:
        try:
            req: ApiRequest = self.queries[name]
            return req
        except KeyError as exc:
            if autocreate:
                req = ApiRequest(name)
                self.queries[name] = req
                return req
            else:
                raise KeyError from exc

    async def gen_url_for(
        self, request_name: str = ""
    ) -> AsyncGenerator[str, None]:
        if request_name:
            queries: list[ApiRequest] = [
                q for q in self.queries.values() if q.part_name == request_name
            ]
        else:
            queries = list(self.queries.values())
        while len(queries):
            self.current_query = queries.pop()
            print(f"Set query {self.current_query.part_name}")
            async for query_part in self.current_query.build_full_query():
                pager: Generator[int, Any, None] = self.page()
                for page in pager:
                    yield f"{self.url}{query_part}&page={page}"
                    if self._next_url:
                        self._next_url = False
                        break


if __name__ == "__main__":
    ...
