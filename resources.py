import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import suppress
from functools import partial
from itertools import zip_longest
from typing import ClassVar

from aioitertools import cycle
from aioitertools import product as async_product
from aioitertools.more_itertools import chunked
from attrs import asdict, define, field, fields, validators

import base
from schemas import BaseTextSchema


@define(slots=False)
class Resource(base.YassService):
    sentinel: ClassVar[bool] = field(default=False, kw_only=True)

    def __init_subclass__(cls, rsr_type=None) -> None:
        return super().__init_subclass__(rsr_type)


# TODO: понять как можно вытаскивать чанки из ресурса - на уровне скрапера или на уровне ресурса?
# А что делать если это файловый ресурс? Какие тогда чанки или что вместо них?


@define(slots=False)
class BaseNetworkResource(ABC):
    url: str = field()
    delay: int = field(default=0)

    @abstractmethod
    async def get_url_for(self):
        """Метод, который выдает корректную ссылку на очередную порцию данных. Должен быть генератором, а также
        в обязательном порядке предоставлять параметр задержки для корректировки нагрузки на ресурс"""
        await asyncio.sleep(self.delay)
        yield

    def set_delay(self, delay):
        self.delay = delay

    # def __init_subclass__(cls, rsr_type):
    # if rsr_type is None:
    #     raise ValueError(
    #         "У подклассов BaseNetworkResorce обязательно должен быть указан тип ресурса для регистрации в фабрике классов"
    #     )
    # resources_registry[rsr_type] = cls
    # cls.resource_type = field(default=rsr_type, type=str, kw_only=True)


def parse_to_dict(instance, attribute, value):
    for item in value:
        value = {item.query_name: item}
        return value


mf = partial(defaultdict, list)


@define(slots=False)
class QueryString(base.YassAttr):
    query_name: str = field()
    schema: BaseTextSchema = field(init=False)
    persist_fields: dict[str, str] = field(factory=dict)
    mutable_fields: dict[str, list] = field(factory=mf)
    mf_iterator: Iterator = None
    persist_query: str = None

    def get_schema_metadata(self):
        return self.schema.metadata

    def get_schema_name(self):
        return self.schema.schema_name

    def set_persist_field(self, p_field):
        self.persist_fields.update(p_field)

    def set_mutable_field(self, mf_field):
        self.mutable_fields.update(mf_field)

    async def build_full_query(self):
        try:
            self.build_pf_string()
            if self.mutable_fields:
                m_string = await anext(self.build_mf_string())
                p_string = await anext(self.persist_query)
                full_string = p_string + m_string
            else:
                full_string = next(self.persist_query)
            yield full_string
        except (StopAsyncIteration, StopIteration):
            print(f"Gen {self.build_full_query.__name__} is over")

    async def build_mf_string(self):
        if not self.mf_iterator:
            self.build_mf_iterator()
        try:
            while mutable_query := await anext(self.mf_iterator):
                result_str = ""
                for name, value in mutable_query:
                    result_str += f"&{name}={value}"
                yield result_str
        except StopAsyncIteration:
            print(f"Gen {self.build_mf_string.__name__} is over")
            self.build_mf_iterator()

    def build_pf_string(self):
        if not self.persist_query and self.persist_fields:
            persist_query = "?" + "&".join(
                [
                    f"{key}={value}"
                    for key, value in self.persist_fields.items()
                ]
            )
            if self.mutable_fields:
                self.persist_query = cycle([persist_query])
            else:
                self.persist_query = iter([persist_query])

    def build_mf_iterator(self):
        mf_lst = [
            zip_longest([key], value, fillvalue=key)
            for key, value in self.mutable_fields.items()
        ]
        iterator = async_product(*mf_lst)
        self.mf_iterator = iterator

    def __aiter__(self):
        return self.build_full_query()

    async def __anext__(self):
        try:
            iterable = self.build_full_query()
            string = await anext(iterable)
            return string
        except (StopAsyncIteration, StopIteration):
            pass

    def __hash__(self) -> int:
        return hash(id(self))


@define(slots=False)
class ApiResource(BaseNetworkResource, Resource, rsr_type="api"):
    queries: dict[str, QueryString] = field(factory=dict)
    max_pages: int = field(default=100)
    current_query: "QueryString" = field(init=False, eq=False)
    chunk_size: int = field(default=1)

    def set_correct_query(self):
        query_set = {}
        for item in self.queries:
            query_set.update({item.query_name: item})
        self.queries = query_set

    async def get_url_for(self, query_string_name=False):
        queries_lst = [
            query
            for query in self.queries.values()
            if query.query_name == query_string_name or not query_string_name
        ]
        while len(queries_lst):
            query = queries_lst.pop()
            self.current_query = query
            try:
                while query_path := await anext(query):
                    pages = self.pager()
                    for page in pages:
                        if self.sentinel:
                            pages.close()
                            self.sentinel = False
                            break
                        await asyncio.sleep(self.delay)
                        yield f"{self.url}{query_path}&page={page}"

            except (StopAsyncIteration, StopIteration):
                print(f"Gen {self.get_url_for.__name__} is over")

    def pager(self):
        with suppress(GeneratorExit):
            yield from range(self.max_pages)

    def set_current_query(self, query_string_name):
        self.current_query = self.queries[query_string_name]

    def set_max_pages(self, pages):
        self.max_pages = pages

    def put_query(self, query):
        self.queries.update({query.query_name: query})

    def cleare_data(self, raw_data):
        try:
            return self.schema.take_data(raw_data)
        except Exception:
            print("There is no data here, go to the next url")
            self.sentinel = True
            raise ValueError

    @property
    def schema(self):
        return self.current_query.schema

    @property
    def query_name(self):
        return self.current_query.query_name

    @property
    def schema_name(self):
        return self.current_query.get_schema_name()

    @property
    def allowed_queries(self):
        return [name for name, value in self.queries.items()]

    def __hash__(self) -> int:
        return hash(id(self))


if __name__ == "__main__":
    # d1 = {'a': ['s', 'd', 'f']}
    # d2 = {'b': ['n', 'm', 'l']}
    # d3 = {'c': ['g', 'h', 'j']}

    # q = QueryString('hh', 'schema')
    # q.set_mutable_field(d1)
    # q.set_mutable_field(d2)
    # q.set_mutable_field(d3)
    # q.set_persist_field({'country': 'India'})
    # q2 = QueryString('bine', 'schema')
    # q2.set_persist_field({'site': 'Private'})
    # r = ApiResource('hh.ru/', max_pages=3)
    # r.put_query(q)
    # r.put_query(q2)
    # print(scrapers_registry)
    # print(r.context)

    # async def test_gen():
    #     count = 0
    #     async for url in r.get_url_for():
    #         i = url
    #         print(i)
    #         # count += 1
    #         # if count > 20:
    #         #     break

    # async def gen():
    #     async for i in chunked(q, 10):
    #         print(i)

    # async def test_gen_two():
    #     async for i in gen():
    #         for x in range(0, 100):
    #             print(i, x)

    # asyncio.run(test_gen())

    for i in range(0, 100, 10):
        print(i)
        for y in range(1000):
            print(y)
            if y == 10:
                break
