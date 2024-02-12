from asyncio import FIRST_COMPLETED, Task, create_task, run, wait
from dataclasses import dataclass, field
from typing import Literal

from .scheduling.triggers import ACTIVE_CONDITIONS
from .scrapers import ApiScraper


@dataclass
class Yass:
    lookup_interval: int = field(default=600)

    def make_crawl(self, url: str, *, crawl_type: Literal["api", "raw"]):
        if crawl_type == "api":
            return ApiScraper(url)
        if crawl_type == "raw":
            msg = (
                "Скрапер для ресурсов без API в настоящее время не реализован"
            )
            raise NotImplementedError(msg) from None

    async def _run_coros(self) -> None:
        # Мы запускаем все триггеры на ожидание в конкрутентом исполнении и рекурсивно их перезапускаем
        awaiting_tasks = {
            create_task(cond.pending()) for cond in ACTIVE_CONDITIONS.keys()
        }
        while awaiting_tasks:
            done, pending = await wait(
                awaiting_tasks,
                timeout=self.lookup_interval,
                return_when=FIRST_COMPLETED,
            )
            print(f"Done is {done}, pending is {pending}")
            awaiting_tasks: set[Task] = pending
            for task in done:
                if task.exception() is None:
                    key = task.result()
                    if key in ACTIVE_CONDITIONS.keys():
                        callback: Task = create_task(ACTIVE_CONDITIONS[key]())
                        print(f"Triggered callback is {callback}")
                        awaiting_tasks.add(callback)
                        print(f"Awaiting tasks is {awaiting_tasks}")
                else:
                    # TODO: здесь будут обрабатываться ошибки, которые можно исправить
                    # и перезагрузить исполнение
                    print(
                        f"Condition {task.get_coro()} finished execution with an error {task.exception()}"
                    )
                    task.cancel()
                    await task
        print("Start pending another conditions cycle")
        return await self._run_coros()

    def run(self) -> None:
        run(self._run_coros())


if __name__ == "__main__":
    pass
