from abc import abstractmethod
from typing import Self

from yass.core import YassCore

__all__ = ["ActionCondition", "BaseLimit"]


class ActionCondition(YassCore):
    @abstractmethod
    async def pending(self) -> Self: ...

    @abstractmethod
    def is_able(self) -> bool: ...


class BaseLimit(YassCore):
    @abstractmethod
    def is_overflowed(self) -> bool: ...
