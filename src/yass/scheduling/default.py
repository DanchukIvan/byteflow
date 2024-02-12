from asyncio import Condition
from typing import Self

from .base import ActionCondition

__all__: list[str] = ["AlwaysRun"]


class AlwaysRun(ActionCondition):
    def __init__(self):
        self.cond = Condition()

    def is_able(self) -> bool:
        return True

    async def pending(self) -> Self:
        await self.cond.wait_for(lambda: self.is_able())
        return self
