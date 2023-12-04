import asyncio
from abc import ABC, abstractmethod
from asyncio import Condition
from collections.abc import Iterable, Iterator
from copy import deepcopy
from datetime import datetime, time, timedelta
from itertools import cycle
from types import MethodType

from attrs import define, field, validators
from regex import regex

from base import YassAttr, conditions_set

__all__ = ["conditions_registry", "TimeCondition"]


class OneShotRun:
    done_shot: bool = False

    def __lt__(self, other):
        if self.done_shot:
            return False
        return True

    def __gt__(self, other):
        if self.done_shot:
            return False
        return True

    def __eq__(self, other):
        if self.done_shot:
            return False
        return True

    def set(self):
        self.done_shot = True

    def reset(self):
        self.done_shot = False


# TODO: нужно отконтролить что время следующего запуска не больше время окончания


def np_by_day_interval(self):
    current_date = datetime.now().date()
    if not self.next_launch:
        self.next_launch = datetime.combine(
            date=current_date, time=self.start_time
        )
        if not self.end_time > datetime.now().time():
            np_by_day_interval(self)
        return
    self.next_launch = datetime.combine(
        date=current_date, time=self.start_time
    )
    delta = next(self.period)
    self.next_launch += timedelta(days=delta)


def np_by_weekday(self):
    current_date = datetime.now().date()
    if not self.next_launch:
        self.next_launch = datetime.combine(
            date=current_date, time=self.start_time
        )
        if not self.end_time > datetime.now().time():
            np_by_weekday(self)
        return
    self.next_launch = datetime.combine(
        date=current_date, time=self.start_time
    )
    current_weekday = self.next_launch.isoweekday()
    next_weekday = next(self.period)
    delta = abs(current_weekday - next_weekday)
    self.next_launch += timedelta(days=delta)


def validate_int_period(period):
    if period < 1:
        raise ValueError(
            "В качестве периода должно быть передано целое число больше или равное 1"
        )


def validate_tuple_period(period):
    t = filter(lambda x: isinstance(x, int), period)
    t = list(t)
    if len(t) < len(period) or len(period) == 0:
        print(f"Len t is {len(t)}, len period is {len(period)}")
        raise ValueError(
            "В качестве периода может быть передан только непустой итерируемый объект с целыми числами"
        )


def time_string_converter(time_str):
    time_time = None
    if isinstance(time_str, str):
        split_pattern = regex.compile(r"[\.:\-\s]", cache_pattern=True)
        if regex.search(
            r"[0-2]?(?(?<=[01]?)[0-9])(?(?<=2)[0-3])[\.\-:;\s]([\.\-:;\s]?[0-5][0-9]){1,3}(?(?=\1)\1)",
            time_str,
        ):
            if len(time_str) == 4:
                time_str = "0" + time_str
            time_array = split_pattern.split(time_str)
            time_time = time.fromisoformat(":".join(time_array))
            return time_time
    elif isinstance(time_str, OneShotRun) or time_str is None:
        time_time = time_str
        return time_time
    else:
        raise ValueError(
            "Начальное и конечное время запуска поддерживает только строковые аргументы и dummy-переменную OneShotRun"
        )


@define(slots=False)
class BaseCondition(ABC, YassAttr):
    @abstractmethod
    def is_able(self):
        """Метод, фиксирующий выполнение условия. Содержит непосредственно логику проверки"""

    @abstractmethod
    async def pending(self):
        """Метод, который запускается в цикл событий для проверки условий. Может содержать синхронную функцию, которая
        выполняется в отдельном потоке"""

    @abstractmethod
    def reset(self):
        """Метод сбрасывает статус класса до исходного перед очередной проверкой условий"""

    def __hash__(self) -> int:
        return hash(id(self))

    def __init_subclass__(cls, condition_name):
        if condition_name is None:
            raise ValueError(
                "У подклассов BaseCondition обязательно нужно имя для регистрации в фабрике классов"
            )
        conditions_set[condition_name] = cls


@define(slots=False)
class TimeCondition(BaseCondition, condition_name="time_condition"):
    period: Iterable = field(
        validator=[validators.instance_of((int, tuple, Iterator))], repr=True
    )
    # любая строка, которая может быть интепретирована как время. Датой считается текущая дата
    start_time: time = field(
        converter=time_string_converter, repr=True, eq=True, order=True
    )
    end_time: time | OneShotRun = field(
        converter=time_string_converter,
        init=False,
        default=None,
        repr=True,
        eq=True,
        order=True,
    )
    frequency: float = field(
        validator=validators.instance_of((float, int)),
        default=0,
        repr=True,
        eq=True,
        order=True,
    )
    # как указать что тут класс класса датетайм
    next_launch: datetime = field(init=False, default=None)

    # TODO: определить слоты для атрибутов

    def __attrs_post_init__(self):
        if not self.end_time:
            self.end_time = OneShotRun()
        # TODO: меня бесит эта фигня, что нужно передавать self; посмотреть что можно сделать
        self.period_to_cycle(self.get_period)
        self.set_next_launch()

    def set_next_launch(self):
        """Generic метод, определяется в рантайме в зависимости от типа аргумента period"""

    def is_able(self):
        print("Start check condition")
        print(f"Next launch is {self.next_launch}")
        current_datetime = datetime.now()
        if self.end_time > current_datetime.time():
            if current_datetime >= self.next_launch:
                # TODO: а нужен ли здесь кондишн статус? Мне кажется на самом деле не особо, можно просто бул возвращать
                if hasattr(self.end_time, "done_shot"):
                    self.end_time.set()
                return True
            return False

    async def pending(self):
        # TODO: нужно все таки понять подходит ли здесь asyncio condition
        while not self.is_able():
            print("Sleeping")
            delta = self.get_delay()
            print(f"Condition was sleep above {delta} seconds")
            await asyncio.sleep(delta.total_seconds())
        self.reset()
        return self

    def period_to_cycle(self, period):
        if isinstance(period, tuple):
            validate_tuple_period(period)
            it = deepcopy(period)
            it = sorted(it)
            self.period = cycle(it)
            self.set_next_launch = MethodType(np_by_weekday, self)
        elif isinstance(period, int):
            validate_int_period(period)
            it = list()
            it.append(period)
            self.period = cycle(it)
            self.set_next_launch = MethodType(np_by_day_interval, self)
        else:
            raise ValueError(
                "Период указывается как целое число или кортеж с днями недели в формате ISO"
            )

    def get_delay(self):
        current_datetime = datetime.now()
        delta = self.next_launch - current_datetime
        if delta.total_seconds() > 0:
            return delta
        else:
            raise RuntimeError(
                "Возникла непредвиденная ошибка: событие не зафиксировано несмотря на превышение временного порога"
            )

    def reset(self):
        print("Reset all config to the next cycle")
        if self.frequency:
            self.next_launch += timedelta(hours=self.frequency)
        if isinstance(self.end_time, OneShotRun):
            self.end_time.reset()
        if self.next_launch.time() > self.end_time:
            self.set_next_launch()

    @property
    def get_period(self):
        return self.period

    def __hash__(self) -> int:
        return super().__hash__()


if __name__ == "__main__":
    time_cond = TimeCondition(period=1, start_time="9:33")

    def is_able(end_time):
        current_datetime = datetime.now()
        print("Check time")
        if end_time > current_datetime.time():
            return True
        return False

    work_time = datetime.now() + timedelta(minutes=5)

    work_cond = Condition()

    async def class_timing():
        key = await time_cond.pending()
        print(key)

    async def timing(cond, work_time):
        async with cond:
            await cond.wait_for(is_able(work_time))
            print("Okay")

    # asyncio.run(class_timing())
