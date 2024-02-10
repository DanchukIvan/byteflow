from abc import abstractmethod
from asyncio import gather, sleep
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import KW_ONLY, InitVar, dataclass, field
from itertools import filterfalse
from random import choice
from typing import Any, Self, TypeVar

import aiohttp
from aiohttp import ClientResponse, ClientSession
from aioitertools.more_itertools import chunked
from yarl import URL

from base import YassCore
from constants import NOTHING
from exceptions import EndOfResource
from repo import RepoManager
from resources import ApiResource, Resource
from triggers import as_trigger

# TODO: перенести в специальные переменные

S = TypeVar("S", bound="Scraper")

proxy_list: list = []


# TODO: метод для скрапера "сырых" ресурсов
async def process_requests(self: Self, urls: list[str]) -> list[bytes]:
    if not proxy_list:
        proxy_list = [None] * len(urls)
    targets = zip(urls, proxy_list)
    session: type[ClientSession]
    async with self.client_factory(
        timeout=self.req_timeout, headers=self.extra_headers
    ) as session:
        tasks: list[Coroutine[Awaitable[Any], None, ClientResponse]] = [
            session.get(url, proxy=proxy) for url, proxy in targets
        ]  # type: ignore
        responses: list[ClientResponse] = await gather(*tasks)
    error_resp: ClientResponse
    for error_resp in filterfalse(
        lambda x: x.status in list(range(200, 299, 1)), responses
    ):
        msg: str = (await error_resp.content.read()).decode()
        # TODO: нужно подумать как мы будем кэтчить ошибки - если режим нестрогий, то можно только логировать ошибки подробно и усе
        raise RuntimeError(error_resp.url, error_resp.headers, msg)
    contents: list[bytes] = await gather(
        *[resp.content.read() for resp in responses]
    )
    return contents


@dataclass
class Scraper(YassCore):
    url: InitVar[str]
    resource: Resource = field(init=False)
    _: KW_ONLY
    extra_headers: dict = field(default_factory=dict)
    storage: RepoManager = field(default_factory=RepoManager)
    req_timeout: int = field(default=10)
    req_delay: float = field(default=0.5)

    @abstractmethod
    async def scrape(self):
        """Основной метод скрапинга, частный метод реализации которого зависит от типа паука"""

    def build_proxyurl(
        *,
        address: str = "",
        port: int | None = None,
        username: str = "",
        password: str = "",
        display_url: bool = False,
    ) -> str:
        url_string: URL = (
            URL(address)
            .with_port(port)
            .with_password(password)
            .with_user(username)
        )
        proxy_list.append(url_string.human_repr())
        if display_url:
            print(f"Prepared proxy url {url_string.human_repr()}")
        return url_string.human_repr()

    @asynccontextmanager
    async def watch_eor(self: Self) -> AsyncGenerator[None, None]:
        try:
            yield
        except EndOfResource:
            self.resource._next_url = True


@dataclass
class ApiScraper(Scraper, subcls_key="api"):
    _: KW_ONLY
    client_factory: Callable = field(default=ClientSession, kw_only=True)
    batch_size: int = field(default=1)
    use_proxy: bool = False

    def __post_init__(self, url: str):
        self.resource = ApiResource(url)

    async def process_requests(self: Self, urls: list[str]) -> list[bytes]:
        session: type[ClientSession]
        proxy = choice(proxy_list) if proxy_list and self.use_proxy else None  # nosec CWE-330
        async with self.client_factory(
            timeout=aiohttp.ClientTimeout(self.req_timeout),
            headers=self.extra_headers,
        ) as session:
            tasks: list[Coroutine[Awaitable[Any], None, ClientResponse]] = [
                session.get(url, proxy=proxy) for url in urls
            ]  # type: ignore
            responses: list[ClientResponse] = await gather(*tasks)
        error_resp: ClientResponse
        for error_resp in filterfalse(
            lambda x: x.status in list(range(200, 299, 1)), responses
        ):
            msg: str = (await error_resp.content.read()).decode()
            raise RuntimeError(error_resp.url, error_resp.headers, msg)
        contents: list[bytes] = await gather(
            *[resp.content.read() for resp in responses]
        )
        return contents

    @as_trigger
    async def scrape(self: Self) -> None:
        async for urls in chunked(
            self.resource.gen_url_for(), self.batch_size
        ):
            print(f"Process urls: {urls}")
            context_anchor = self.resource.current_query
            async with self.storage.start_stream(context_anchor) as repo:
                async with self.watch_eor():
                    content: list[bytes] = await self.process_requests(urls)
                    await repo.put(content)
                await sleep(self.req_delay)
