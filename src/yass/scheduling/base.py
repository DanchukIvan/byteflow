from abc import abstractmethod
from typing import Protocol, Self

__all__: list[str] = ["ActionCondition"]


class ActionCondition(Protocol):
    @abstractmethod
    async def pending(self) -> Self:
        ...

    @abstractmethod
    def is_able(self) -> bool:
        ...
