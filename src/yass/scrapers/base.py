from abc import abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import KW_ONLY, InitVar, dataclass, field

from yarl import URL

from yass.exceptions.exceptions import EndOfResource

from ..core import YassCore
from ..resources import BaseResource
from ..storages import StorageManager

__all__ = ["BaseScraper"]


@dataclass
class BaseScraper(YassCore):
    url: InitVar[str]
    resource: BaseResource = field(init=False)
    _: KW_ONLY
    extra_headers: dict = field(default_factory=dict)
    storage: StorageManager = field(default_factory=StorageManager)
    req_timeout: int = field(default=10)
    req_delay: float = field(default=0.5)

    @abstractmethod
    async def scrape(self):
        """Основной метод скрапинга, частный метод реализации которого зависит от типа паука"""

    @abstractmethod
    async def process_requests(self, urls: list[str]) -> list[bytes]:
        ...

    @asynccontextmanager
    async def watch_eor(self) -> AsyncGenerator[None, None]:
        try:
            yield
        except EndOfResource:
            self.resource._next_url = True
