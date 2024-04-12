from asyncio import Condition, sleep
from collections.abc import Iterable
from dataclasses import KW_ONLY, InitVar, dataclass, field
from datetime import datetime, time, timedelta
from itertools import cycle
from typing import Literal

import dateparser

from yass.core import reg_type
from yass.scheduling import ActionCondition

__all__ = [
    "AllowedWeekDays",
    "AlwaysRun",
    "DailyInterval",
    "TimeCondition",
    "WeekDaysType",
    "WeekdayInterval",
]


class AlwaysRun(ActionCondition):
    def __init__(self):
        self.cond = Condition()

    def is_able(self) -> bool:
        return True

    async def pending(self):
        await self.cond.wait_for(lambda: self.is_able())
        return self


class DailyInterval:
    def __init__(
        self,
        day_interval: int,
        start_time: str,
        end_time: str,
        launch: datetime | str | None = None,
    ):
        self._interval = day_interval
        self.start: time = dateparser.parse(start_time).time()  # type:ignore
        self.end: time = dateparser.parse(end_time).time()  # type: ignore
        self.launch: datetime = (
            datetime.combine(date=datetime.now().date(), time=self.start)
            if launch is None
            else dateparser.parse(launch)  # type: ignore
        )

    def shift_launch(self, frequency: float):
        hours = timedelta(hours=1)
        # если мы запускаем скрипт с глубоким лагом (например, в 12:00 при заданном старте в 9:00),
        # то нам нужно "выровнять" время следующего запуска во избежание череды ложных запусков
        lag = (datetime.now() - self.launch) // hours
        frequency = lag + frequency if lag > frequency else frequency
        self.launch += timedelta(hours=frequency)

    def next_launch(self):
        next_date = self.launch.date() + timedelta(days=self._interval)
        self.launch = datetime.combine(date=next_date, time=self.start)

    def __bool__(self):
        return (
            datetime.now()
            >= self.launch
            < datetime.combine(date=self.launch.date(), time=self.end)
        )

    def setted_period(self):
        return self._interval


AllowedWeekDays = Literal[1, 2, 3, 4, 5, 6, 7]
WeekDaysType = Iterable[AllowedWeekDays]


class WeekdayInterval:
    def __init__(
        self,
        weekday_interval: WeekDaysType,
        start_time: str,
        end_time: str,
        launch: datetime | str | None = None,
    ):
        self._interval = weekday_interval
        self.weekday_it = cycle(set(sorted(weekday_interval)))
        self.current_weekday: AllowedWeekDays = next(self.weekday_it)
        self.start: time = dateparser.parse(start_time).time()  # type: ignore
        self.end: time = dateparser.parse(end_time).time()  # type: ignore
        self.launch: datetime = (
            datetime.combine(date=datetime.now().date(), time=self.start)
            if launch is None
            else dateparser.parse(launch)  # type: ignore
        )

    def shift_launch(self, frequency):
        hours = timedelta(hours=1)
        lag = (datetime.now() - self.launch) // hours
        frequency = lag + frequency if lag > frequency else frequency
        self.launch += timedelta(hours=frequency)

    def next_launch(self):
        interval = abs(next(self.weekday_it) - self.current_weekday)
        next_date = self.launch.date() + timedelta(days=interval)
        self.launch = datetime.combine(date=next_date, time=self.start)

    def __bool__(self):
        return (
            datetime.now()
            >= self.launch
            < datetime.combine(date=self.launch.date(), time=self.end)
        )

    def setted_period(self):
        return self._interval


@reg_type("timer")
@dataclass
class TimeCondition(ActionCondition):
    _: KW_ONLY
    period: InitVar[int | WeekDaysType] = field()
    # любая строка, которая может быть интепретирована как время. Датой считается текущая дата
    start_time: InitVar[str] = field(default="0:01")
    end_time: InitVar[str] = field(default="")
    frequency: float = field(default=0)
    launch_date: InitVar[datetime | None] = field(default=None)
    schedule_interval: DailyInterval | WeekdayInterval = field(init=False)
    _one_run: bool = field(default=False, init=False)

    def __post_init__(self, period, start_time, end_time, launch_date):
        if not end_time:
            hours: int = int(23 - (self.frequency // 1))
            minutes: int = int(59 - round((self.frequency % 1) * 60, 0))
            end_time = time(hours, minutes).strftime("%H:%M:%S")
        if isinstance(period, int):
            self.schedule_interval = DailyInterval(
                abs(period), start_time, end_time, launch_date
            )
        else:
            self.schedule_interval = WeekdayInterval(
                period, start_time, end_time, launch_date
            )
        if not self.frequency:
            self._one_run = True

    def is_able(self):
        print("Start check condition")
        print(f"Next launch is {self.schedule_interval.launch}")
        return bool(self.schedule_interval)

    async def pending(self):
        while not self.is_able():
            print("Sleeping")
            delta: timedelta = self.get_delay()
            print(f"Condition was sleep above {delta} seconds")
            await sleep(delta.total_seconds())
        self.reset()

    def get_delay(self) -> timedelta:
        current_datetime: datetime = datetime.now()
        delta: timedelta = self.schedule_interval.launch - current_datetime
        if delta.total_seconds() > 0:
            return delta
        else:
            return timedelta(seconds=0)

    def reset(self):
        self.schedule_interval.shift_launch(self.frequency)
        # TODO: можно сократить проверку до вызова is_able и проверки на one_run - ниже условие по смыслу такое самое
        if (
            self.schedule_interval.end < self.schedule_interval.launch.time()
            or self._one_run
        ):
            self.schedule_interval.next_launch()

    def get_period(self):
        return self.schedule_interval.setted_period()

    def get_next_run(self):
        return self.schedule_interval.launch
