import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from croniter import croniter


class Task(ABC):
    """
    Задача, выполняемая в потоке планировщика.
    """

    def __init__(
        self,
        job: Callable[[...], None],
        args: tuple = None,
        kwargs: dict = None,
        name: Optional[str] = None,
    ) -> None:
        self._job = job
        self._args = args or ()
        self._kwargs = kwargs or {}
        self._name = name or job.__qualname__
        self._next_run_time = None  # время следующего запуска задачи

    @property
    def name(self) -> str:
        return self._name

    @property
    def next_run_time(self) -> datetime:
        """
        Свойство возвращающее время следующего запуска.

        Returns:
            datetime: Время следующего запуска.
        """
        return self._next_run_time

    @abstractmethod
    def set_next_run_time(self) -> None:
        """
        Метод рассчитывающий у устанавливающий время следующего запуска.
        Переопределяется в дочерних классах.
        """

    def run_job(self) -> None:
        """
        Вызывает переданный Callable объект со всем аргументами
        в потоке планировщика.
        """
        logging.info(
            'Task [%s] started with schedule [%s]',
            self._name,
            self._next_run_time,
        )

        try:
            self._job(*self._args, **self._kwargs)
        except Exception as ex:
            logging.exception(
                'Unexpected error occurred in task [%s]: "%s".',
                self._name,
                ex,
            )
        else:
            logging.info('Task completed [%s]', self._name)

    def __lt__(self, other: 'Task') -> bool:
        return self._next_run_time < other._next_run_time

    def __gt__(self, other: 'Task') -> bool:
        return self._next_run_time > other._next_run_time


class OneTimeTask(Task):
    """
    Одноразовая задача, которая будет выполнена ровно один раз.
    """

    def __init__(self, date: datetime, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._next_run_time = date

    def set_next_run_time(self) -> None:
        self._next_run_time = None


class CronTask(Task):
    """
    Задача, которая выполняется по CRON расписанию.
    """

    def __init__(self, schedule: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._schedule = croniter(schedule, datetime.now(timezone.utc))
        self._next_run_time = self._schedule.get_next(datetime)

    def set_next_run_time(self) -> None:
        self._next_run_time = self._schedule.get_next(datetime)


class PeriodicTask(Task):
    """
    Задача, которая будет выполняться периодически.
    """

    def __init__(self, period: timedelta, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._period = period
        self._next_run_time = datetime.now(timezone.utc)

    def set_next_run_time(self) -> None:
        self._next_run_time = datetime.now(timezone.utc) + self._period
