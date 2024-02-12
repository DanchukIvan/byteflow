from abc import abstractmethod
from collections.abc import AsyncGenerator, Generator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Self

from ..core import YassCore


@dataclass
class BaseResource(YassCore):
    url: str
    _next_url: bool = field(default=False)
    max_pages: int = field(default=100, kw_only=True)

    @abstractmethod
    async def gen_url_for(
        self: Self, query_name: str = ""
    ) -> AsyncGenerator[str, None]:
        ...

    def page(self) -> Generator[int, Any, None]:
        with suppress(GeneratorExit):
            yield from range(self.max_pages)
