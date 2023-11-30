from typing import ClassVar
from typing import ClassVar, Dict, Any, Optional
from attrs import define, field
from asyncio import run, gather, create_task
from itertools import chain
import base
from collections import defaultdict


@define(slots=False)
class Yass(base.ContextMixin):
    attr_mapping: ClassVar[dict] = field(default=defaultdict(list))

    async def run_coro(self):
        containers = chain(*[getattr(self, attr)
                           for attr in self.attr_mapping.keys()])
        tasks = [create_task(coro.pending()) for coro in containers]
        await gather(*tasks)

    def run(self):
        run(self.run_coro())

    def __hash__(self) -> int:
        return hash(id(self))

    def __attrs_post_init__(self):
        self.__prepare_app()

    def __prepare_app(self):
        self.__build_attr_mapping()
        if not self.attr_mapping.keys():
            raise AttributeError(
                'Не установлено ни одного аттрибута. Возможно, не иницилизирован ни один подкласс YassActor')
        for attr, collection in self.attr_mapping.items():
            setattr(self, attr, collection)

    def __build_attr_mapping(self):
        instances_lst = [c for c in chain(
            *self.init_context.container.values()) if isinstance(c, base.YassActor)]
        d = defaultdict(list)
        mapping = {d[bcls.meta].append(
            bcls) for bcls in instances_lst}
        self.attr_mapping = d


if __name__ == "__main__":
    pass
