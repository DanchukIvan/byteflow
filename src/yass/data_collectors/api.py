from __future__ import annotations

from asyncio import create_task, gather, sleep
from collections.abc import AsyncGenerator, Awaitable, Coroutine, Sequence
from itertools import compress, filterfalse
from time import time
from typing import TYPE_CHECKING, Any

import aiohttp
import orjson
from aiohttp import ClientResponse, ClientSession
from aioitertools.more_itertools import take
from rich.pretty import pprint as rpp

from yass.contentio import deserialize
from yass.core import is_empty_instance, reg_type
from yass.data_collectors.base import BaseDataCollector

__all__ = ["ApiDataCollector", "EORTriggersResolver"]

if TYPE_CHECKING:
    from yass.resources import (
        ApiEORTrigger,
        ApiRequest,
        ApiResource,
        BatchCounter,
    )

__all__: list[str] = ["ApiDataCollector"]


class EORTriggersResolver:
    def __init__(self, resource: ApiResource):
        self.content_searchers: list[ApiEORTrigger] = list()
        self.headers_searchers: list[ApiEORTrigger] = list()
        self._resolve_searchers(resource.eor_triggers)

    def eor_signal(
        self, content: list[bytes], responses: list[ClientResponse]
    ) -> list[bool]:
        signal_seq: list[list[bool]] = []
        for searcher in self.content_searchers:
            bits = [searcher.is_end_of_resource(cont) for cont in content]
            signal_seq.append(bits)
        for searcher in self.headers_searchers:
            bits = [searcher.is_end_of_resource(resp) for resp in responses]
            signal_seq.append(bits)
        return self._resolve_bitmap(signal_seq)

    def _resolve_bitmap(self, signal_seq: Sequence) -> list[bool]:
        # Выбирает самую "строгую" последовательность битов - где меньше всего подтвержденных значений
        bits_sum: list[int] = [sum(seq) for seq in signal_seq]
        index: int = bits_sum.index(min(bits_sum))
        return signal_seq[index]

    def _resolve_searchers(self, triggers: list[ApiEORTrigger]):
        for trigger in triggers:
            if trigger.search_type == "content":
                self.content_searchers.append(trigger)
            elif trigger.search_type == "headers":
                self.headers_searchers.append(trigger)


@reg_type("api_dc")
class ApiDataCollector(BaseDataCollector):
    def __init__(self, query: ApiRequest, resource: ApiResource):
        super().__init__(resource, query)
        self.client_factory = ClientSession
        self.batcher: BatchCounter = resource.batch
        self.headers: dict = resource.extra_headers
        self.eor_checker: EORTriggersResolver = EORTriggersResolver(resource)
        self.current_bs: int = 0

    async def start(self):
        await self.collect_trigger.pending()
        await self._write_channel.storage.launch_session()
        self.current_bs: int = await self.batcher.acquire_batch()
        rpp(
            f"Текущий размер батча {self.current_bs}. Минимальный размер батча {self.batcher.min_batch}."
        )
        url_gen: AsyncGenerator[str, None] = self.url_series()
        print(f"Статус генератора ссылок (ag_running): {url_gen.ag_running}.")
        while True:
            urls: list[str] = await take(self.current_bs, url_gen)
            rpp(f"Количество полученных ссылок {len(urls)}")
            raw_content: list[bytes] = await self.process_requests(urls)
            start = int(time())
            if len(raw_content) > 0:
                deser_content = tuple(
                    [
                        deserialize(raw_bytes, self.input_format)
                        for raw_bytes in raw_content
                    ]
                )
            if not is_empty_instance(self.pipeline):
                async with self.pipeline.run_transform(
                    deser_content
                ) as pipeline:
                    deser_content = await pipeline
            if deser_content:
                prepared_content = tuple(
                    (
                        self.path_producer.render_path(self.output_format),
                        dataset,
                    )
                    for dataset in deser_content
                )
            async with self._write_channel.block_state() as buf:
                await buf.parse_content(prepared_content)
            rpp(f"Ресурс закончился? {self.eor_status}")
            if self.eor_status:
                try:
                    await url_gen.asend(self.eor_status)  # type:ignore
                except StopAsyncIteration:
                    rpp("Обход ресурса завершен.")
                    break
                self.eor_status = False
            rpp(f"Новый размер батча составил {self.current_bs}")
            end = int(time())
            print(f"Время обработки запросов составило {end - start} секунд")
            effect_delay: int | float = self.delay - (end - start)
            rpp(f"Эффективная задержка составила {effect_delay} секунд.")
            if effect_delay > 0:
                await sleep(effect_delay)
        self.batcher.release_batch(self.current_bs)
        coro = self.start()
        return create_task(coro)

    async def process_requests(self, urls: list[str]) -> list[bytes]:
        session: ClientSession
        async with self.client_factory(
            timeout=aiohttp.ClientTimeout(self.timeout),
            headers=self.headers,
            json_serialize=orjson.dumps,  # type:ignore
        ) as session:
            tasks: list[Coroutine[Awaitable[Any], None, ClientResponse]] = [
                session.get(url)
                for url in urls  # type: ignore
            ]
            self.current_bs = self.batcher.recalc_limit(self.current_bs)
            responses: list[ClientResponse] = await gather(*tasks)
        error_resp: ClientResponse
        # FIXME: А это норма ваще?
        for error_resp in filterfalse(
            lambda x: x.status in list(range(200, 299, 1)), responses
        ):
            msg: str = (await error_resp.content.read()).decode()
            raise RuntimeError(error_resp.url, error_resp.headers, msg)
        contents: list[bytes] = await gather(
            *[resp.content.read() for resp in responses]
        )
        eor_check: list[bool] = self.eor_checker.eor_signal(
            contents, responses
        )
        contents = list(compress(contents, eor_check))
        self.eor_status: bool = not all(eor_check)
        return contents
