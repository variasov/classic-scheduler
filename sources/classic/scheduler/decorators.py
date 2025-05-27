from datetime import timedelta
from typing import TypeVar

from classic.components import add_extra_annotation

from .scheduler import Scheduler


Method = TypeVar('Method')
PeriodValue = int | float | timedelta


def by_cron(schedule: str | list[str]) -> Method:
    """
    Помечает метод для автоматической регистрации в планировщике.
    Планировщик будет выполнять метод с указанным расписанием.
    Расписание можно указать одно, а можно сразу несколько.
    """

    def decorator(fn: Method):
        fn.__by_cron__ = [schedule] if isinstance(schedule, str) else schedule
        add_extra_annotation(fn, 'scheduler', Scheduler)
        return fn
    return decorator



def by_period(period: PeriodValue | list[PeriodValue]) -> Method:
    """
    Помечает метод для автоматической регистрации в планировщике.
    Планировщик будет выполнять метод с указанным периодом.
    Период можно указать один, а можно сразу несколько.
    """

    def decorator(fn: Method):
        fn.__by_period__ = (
            [period] if isinstance(period, (int, float, timedelta)) else period
        )
        add_extra_annotation(fn, 'scheduler', Scheduler)
        return fn

    return decorator
