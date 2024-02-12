from asyncio import gather, sleep
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import KW_ONLY, dataclass, field
from itertools import filterfalse
from random import choice
from typing import Any

import aiohttp
from aiohttp import ClientResponse, ClientSession
from aioitertools.more_itertools import chunked

from ..resources import ApiResource
from ..scheduling import as_trigger
from .base import BaseScraper
from .common import PROXY_LIST

__all__: list[str] = ["ApiScraper"]


@dataclass
class ApiScraper(BaseScraper, subcls_key="api"):
    _: KW_ONLY
    client_factory: Callable = field(default=ClientSession, kw_only=True)
    batch_size: int = field(default=1)
    use_proxy: bool = False

    def __post_init__(self, url: str):
        self.resource = ApiResource(url)

    async def process_requests(self, urls: list[str]) -> list[bytes]:
        session: type[ClientSession]
        proxy = choice(PROXY_LIST) if PROXY_LIST and self.use_proxy else None  # nosec CWE-330
        async with self.client_factory(
            timeout=aiohttp.ClientTimeout(self.req_timeout),
            headers=self.extra_headers,
        ) as session:
            tasks: list[Coroutine[Awaitable[Any], None, ClientResponse]] = [
                session.get(url, proxy=proxy)
                for url in urls  # type: ignore
            ]
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
    async def scrape(self) -> None:
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
