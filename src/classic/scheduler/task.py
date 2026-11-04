"""Задачи, выполняемые планировщиком."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any

from croniter import croniter


class Task(ABC):
    """Задача, выполняемая в потоке планировщика."""

    def __init__(
        self,
        job: Callable[..., Any],
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> None:
        """Создаёт задачу.

        Args:
            job: Вызываемая в задаче функция.
            args: Позиционные аргументы функции.
            kwargs: Именованные аргументы функции.
            name: Имя задачи. По умолчанию - имя функции.

        """
        self._job = job
        self._args = args or ()
        self._kwargs = kwargs or {}
        self._name = name or job.__qualname__
        # время следующего запуска задачи
        self._next_run_time: datetime | None = None  # pragma: no mutate

    @property
    def name(self) -> str:
        """Имя задачи."""
        return self._name

    @property
    def next_run_time(self) -> datetime | None:
        """Свойство возвращающее время следующего запуска.

        Returns:
            datetime | None: Время следующего запуска,
                None - задача больше не будет запускаться.

        """
        return self._next_run_time

    @abstractmethod
    def set_next_run_time(self) -> None:
        """Метод рассчитывающий у устанавливающий время следующего запуска.
        Переопределяется в дочерних классах.
        """

    def run_job(self) -> None:
        """Вызывает переданный Callable объект со всем аргументами
        в потоке планировщика.
        """
        logging.info(  # pragma: no mutate
            "Task [%s] started with schedule [%s]",  # pragma: no mutate
            self._name,  # pragma: no mutate
            self._next_run_time,  # pragma: no mutate
        )  # pragma: no mutate

        try:
            self._job(*self._args, **self._kwargs)
        except Exception as ex:
            logging.exception(  # pragma: no mutate
                'Unexpected error occurred in task [%s]: "%s".',  # pragma: no mutate
                self._name,  # pragma: no mutate
                ex,  # pragma: no mutate
            )  # pragma: no mutate
        else:
            logging.info("Task completed [%s]", self._name)  # pragma: no mutate

    def _sort_key(self) -> datetime:
        if self._next_run_time is None:
            msg = "Task has no next run time"  # pragma: no mutate
            raise TypeError(msg)  # pragma: no mutate
        return self._next_run_time

    def __lt__(self, other: "Task") -> bool:
        """Сравнивает задачи по времени следующего запуска."""
        return self._sort_key() < other._sort_key()  # pragma: no mutate

    def __gt__(self, other: "Task") -> bool:
        """Сравнивает задачи по времени следующего запуска."""
        return self._sort_key() > other._sort_key()  # pragma: no mutate


class OneTimeTask(Task):
    """Одноразовая задача, которая будет выполнена ровно один раз."""

    def __init__(self, date: datetime, *args: Any, **kwargs: Any) -> None:
        """Создаёт задачу, выполняемую в момент date."""
        super().__init__(*args, **kwargs)
        self._next_run_time = date

    def set_next_run_time(self) -> None:
        """Сбрасывает время запуска: повторно задача не выполняется."""
        self._next_run_time = None


class CronTask(Task):
    """Задача, которая выполняется по CRON расписанию.

    Расписание считается в часовом поясе tz. Если tz не указан,
    используется локальный часовой пояс процесса (с учётом перехода
    на летнее время).
    """

    def __init__(
        self,
        schedule: str,
        *args: Any,
        tz: tzinfo | None = None,
        **kwargs: Any,
    ) -> None:
        """Создаёт задачу с расписанием schedule в часовом поясе tz."""
        super().__init__(*args, **kwargs)
        self._schedule = croniter(schedule, datetime.now(tz))
        self._next_run_time = self._get_next()

    def _get_next(self) -> datetime:
        next_run_time = self._schedule.get_next(datetime)
        # Наивное время - локальное, astimezone() делает его осведомлённым
        return next_run_time.astimezone()

    def set_next_run_time(self) -> None:
        """Вычисляет следующее время запуска по расписанию."""
        self._next_run_time = self._get_next()


class PeriodicTask(Task):
    """Задача, которая будет выполняться периодически."""

    def __init__(self, period: timedelta, *args: Any, **kwargs: Any) -> None:
        """Создаёт задачу, выполняемую каждые period, первый раз сразу."""
        super().__init__(*args, **kwargs)
        self._period = period
        self._next_run_time = datetime.now(timezone.utc)

    def set_next_run_time(self) -> None:
        """Назначает следующий запуск через period от текущего момента."""
        self._next_run_time = datetime.now(timezone.utc) + self._period
