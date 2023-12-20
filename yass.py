import importlib
import os.path
import pkgutil
from asyncio import create_task, gather, run
from collections import defaultdict
from itertools import chain
from typing import ClassVar

import dotted_dict
from attrs import define, field

import base


@define(slots=False)
class Yass:
    attr_mapping: ClassVar[dict] = field(default=defaultdict(list))
    context_registry: dict = field(factory=dict)
    is_load_modules: bool = field(default=False)

    def __attrs_post_init__(self):
        # TODO: нужно оттестировать хорошенько данную часть, чтобы она было более универсальна
        if not self.is_load_modules:
            modules_path = os.path.curdir
            for pkg in pkgutil.iter_modules([modules_path]):
                importlib.import_module(pkg.name) if not pkg.name.startswith(
                    "test_"
                ) else None
            self.is_load_modules = True

    def create_context(self):
        ctx = base.Context()
        self.context_registry[id(ctx)] = ctx
        return base.EnvBuilder(ctx)

    async def run_coros(self):
        containers = chain(
            *[getattr(self, attr) for attr in self.attr_mapping.keys()]
        )
        tasks = [create_task(coro.pending()) for coro in containers]
        await gather(*tasks)

    def run(self):
        self.__prepare_app()
        run(self.run_coros())

    def __prepare_app(self):
        self.__build_attr_mapping()
        if not self.attr_mapping.keys():
            raise AttributeError(
                "Не установлено ни одного аттрибута. Возможно, не иницилизирован ни один подкласс YassActor"
            )
        for attr, collection in self.attr_mapping.items():
            setattr(self, attr, collection)

    def __build_attr_mapping(self):
        if len(self.context_registry.values()) < 1:
            raise RuntimeError(
                "Не создано ни одного юнита работы, создайте хотя бы один контекст работы"
            )
        contexts = list(self.context_registry.values())
        instances_lst = [
            c
            for c in chain(*[list(v.values()) for v in contexts])
            if isinstance(c, base.YassActor)
        ]
        d = defaultdict(list)
        mapping = {
            d[f"{bcls.__class__.__name__.lower()}s"].append(bcls)
            for bcls in instances_lst
        }
        self.attr_mapping = d


if __name__ == "__main__":
    pass
