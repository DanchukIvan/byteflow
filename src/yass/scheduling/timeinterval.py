from asyncio import sleep
from collections.abc import Iterable
from dataclasses import KW_ONLY, InitVar, dataclass, field
from datetime import date, datetime, time, timedelta
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
    """
    A special class that makes the collection of data from a resource constantly active.
    """

    def is_able(self) -> bool:
        return True

    async def pending(self) -> None:
        await sleep(0)
        self.is_able()


class DailyInterval:
    """
    Helper class for checking daily time intervals. Used in the TimeCondition class.

    Args:
        day_interval (int): activity interval, in days. If you want the arrival of
                            the required time to be checked every day, you need to
                            specify 1 in this parameter; to check the condition every
                            two days 2 and so on.
        start_time (str): start of the check time interval. Indicated in a form that
                        allows you to recognize the time, for example, “10:00”, “10.00”, “10-00” and so on.
        end_time (str): end of the check time interval. It is assumed that after this time threshold is overcome,
                        the starting point will shift to the next calendar interval.
        launch (datetime | str | None, optional): The starting point for checking the time interval as
                        a recognizable date or date and time (for example, "10/20/2024 10:00"). If not specified,
                        the start of waiting for the required time starts immediately. If you need to delay
                        the occurrence of a control event, you need to specify a future date. If an urgent
                        start of data collection is required, then a past date must be specified. Defaults to None.
    """

    def __init__(
        self,
        day_interval: int,
        start_time: str,
        end_time: str,
        launch: datetime | str | None = None,
    ):
        self._interval: int = day_interval
        self.start: time = dateparser.parse(start_time).time()  # type:ignore
        self.end: time = dateparser.parse(end_time).time()  # type: ignore
        self.launch: datetime = (
            datetime.combine(date=datetime.now().date(), time=self.start)
            if launch is None
            else dateparser.parse(launch)  # type: ignore
        )

    def shift_launch(self, frequency: int | float) -> None:
        """
        The method shifts the next launch time by a specified interval in hours.
        Also, in this method, the next time interval is aligned in the case of starting from the “past” time.
        The lag in this case is determined in integer form.

        Args:
            frequency (float): start time shift interval.
        """
        hours = timedelta(hours=1)
        # если мы запускаем скрипт с глубоким лагом (например, в 12:00 при заданном старте в 9:00),
        # то нам нужно "выровнять" время следующего запуска во избежание череды ложных запусков
        lag: int = (datetime.now() - self.launch) // hours
        frequency = lag + frequency if lag > frequency else frequency
        self.launch += timedelta(hours=frequency)

    def next_launch(self) -> None:
        """
        The method determines the next key date according to the specified daily interval.
        """
        next_date: date = self.launch.date() + timedelta(days=self._interval)
        self.launch = datetime.combine(date=next_date, time=self.start)

    def __bool__(self) -> bool:
        """
        The method is overridden and compares the current date and time, the control date and
        time, and the end time of the activity interval.

        Returns:
            bool: returns True if the current time exceeds the control threshold and is also included in the active time interval.
        """
        return (
            datetime.now()
            >= self.launch
            < datetime.combine(date=self.launch.date(), time=self.end)
        )

    def setted_period(self) -> int:
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
        """
        Helper class for checking time intervals by day of the week. Used in the TimeCondition class.

        Args:
            weekday_interval (WeekDaysType): check interval in the form of a tuple with numbers of days of the week.
                                            The days of the week are specified in the order prescribed by ISO 8601.
                                            For example, if you need to control time on Mondays, Wednesdays and Fridays,
                                            then the tuple should look like (1,3,5). The maximum value for days of the week
                                            is seven. The order of the weekday numbers is not important.
            start_time (str): start of the check time interval. Indicated in a form that
                            allows you to recognize the time, for example, “10:00”, “10.00”, “10-00” and so on.
            end_time (str): end of the check time interval. It is assumed that after this time threshold is overcome,
                            the starting point will shift to the next calendar interval.
            launch (datetime | str | None, optional): The starting point for checking the time interval as
                        a recognizable date or date and time (for example, "10/20/2024 10:00"). If not specified,
                        the start of waiting for the required time starts immediately. If you need to delay
                        the occurrence of a control event, you need to specify a future date. If an urgent
                        start of data collection is required, then a past date must be specified. Defaults to None.
        """
        self._interval = weekday_interval
        self._weekday_it = cycle(set(sorted(weekday_interval)))
        self.current_weekday: AllowedWeekDays = next(self.weekday_it)
        self.start: time = dateparser.parse(start_time).time()  # type: ignore
        self.end: time = dateparser.parse(end_time).time()  # type: ignore
        self.launch: datetime = (
            datetime.combine(date=datetime.now().date(), time=self.start)
            if launch is None
            else dateparser.parse(launch)  # type: ignore
        )

    def shift_launch(self, frequency: int | float) -> None:
        """
        The method shifts the next launch time by a specified interval in hours.
        Also, in this method, the next time interval is aligned in the case of starting from the “past” time.
        The lag in this case is determined in integer form.

        Args:
            frequency (float): start time shift interval.
        """
        hours = timedelta(hours=1)
        lag = (datetime.now() - self.launch) // hours
        frequency = lag + frequency if lag > frequency else frequency
        self.launch += timedelta(hours=frequency)

    def next_launch(self) -> None:
        """
        The method determines the next key date according to the specified period.
        """
        interval = abs(next(self._weekday_it) - self.current_weekday)
        next_date = self.launch.date() + timedelta(days=interval)
        self.launch = datetime.combine(date=next_date, time=self.start)

    def __bool__(self) -> bool:
        """
        The method is overridden and compares the current date and time, the control date and
        time, and the end time of the activity interval.

        Returns:
            bool: returns True if the current time exceeds the control threshold and is also included in the active time interval.
        """
        return (
            datetime.now()
            >= self.launch
            < datetime.combine(date=self.launch.date(), time=self.end)
        )

    # TODO: возможно удалить данный метод
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
    frequency: int | float = field(default=0)
    launch_date: InitVar[datetime | None] = field(default=None)
    schedule_interval: DailyInterval | WeekdayInterval = field(init=False)
    _one_run: bool = field(default=False, init=False)

    """
    A class that implements a timer function. Its functionality is similar to a standard asynchronous
    lock or event, except that its release depends on time.

    Args:
        period (int | WeekDaysType): activity interval in days or days of the week.
        start_time (str): start of the check time interval. Indicated in a form that 
                        allows you to recognize the time, for example, “10:00”, “10.00”, “10-00” and so on.
        end_time (str): end of the check time interval. It is assumed that after this time threshold is overcome,
                        the starting point will shift to the next calendar interval.
        frequency (int | float): start time shift interval.
        launch_date (datetime | str | None, optional): The starting point for checking the time interval as
                        a recognizable date or date and time (for example, "10/20/2024 10:00"). If not specified,
                        the start of waiting for the required time starts immediately. If you need to delay
                        the occurrence of a control event, you need to specify a future date. If an urgent
                        start of data collection is required, then a past date must be specified. Defaults to None.
    """

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

    def is_able(self) -> bool:
        """
        The method checks whether a checkpoint (a specified timestamp) has been reached.

        Returns:
            bool: returns True if the checkpoint has arrived.
        """
        print("Start check condition")
        print(f"Next launch is {self.schedule_interval.launch}")
        return bool(self.schedule_interval)

    async def pending(self) -> None:
        """
        The method initializes waiting for a reference timestamp.
        If the checkpoint is not reached, the class's utility method determines
        the size of the delta between the current time and the checkpoint and
        starts waiting for the specified delta.
        Upon reaching it, the method unlocks further execution.
        """
        while not self.is_able():
            print("Sleeping")
            delta: timedelta = self._get_delay()
            print(f"Condition was sleep above {delta} seconds")
            await sleep(delta.total_seconds())
        self.reset()

    def _get_delay(self) -> timedelta:
        """
        The method returns the time delta between the current period and the reference timestamp.

        Returns:
            timedelta: time interval before reaching the reference timestamp.
        """
        current_datetime: datetime = datetime.now()
        delta: timedelta = self.schedule_interval.launch - current_datetime
        if delta.total_seconds() > 0:
            return delta
        else:
            return timedelta(seconds=0)

    def reset(self) -> None:
        """
        The method shifts the control point by frequency and checks whether the new value is
        within the specified activity range. If not, then the next scan launch is scheduled
        for the next period.
        """
        self.schedule_interval.shift_launch(self.frequency)
        # TODO: можно сократить проверку до вызова is_able и проверки на one_run - ниже условие по смыслу такое самое
        if (
            self.schedule_interval.end < self.schedule_interval.launch.time()
            or self._one_run
        ):
            self.schedule_interval.next_launch()

    # TODO: возможно удалить данный метод
    def get_period(self):
        return self.schedule_interval.setted_period()

    # TODO: возможно удалить данный метод
    def get_next_run(self):
        return self.schedule_interval.launch
