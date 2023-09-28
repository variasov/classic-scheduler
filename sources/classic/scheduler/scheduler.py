import heapq
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Union

from classic.actors.actor import Actor

from .task import CronTask, OneTimeTask, PeriodicTask, Task


class Scheduler(Actor):
    """
    Планировщик задач. Вызывает переданные ему Callable объекты в своем потоке
    в соответствии с расписанием.
    """

    def __init__(self) -> None:
        super().__init__()
        self._tasks: list[Task] = []

    @Actor.method
    def with_delay(
        self,
        delay: Union[timedelta, float],
        job: Callable[[], Any],
        args: tuple = None,
        kwargs: dict = None,
        task_name: str = None,
    ) -> None:
        """
        Добавляет отложенную задачу в планировщик.
        Задача будет выполнена ровно один раз.

        Args:
            delay (Union[timedelta, float]): Отсрочка выполнения задачи.
            job (Callable[[], Any]): Вызываемая в задаче функция.
            args (tuple, optional): Аргументы. По умолчанию None.
            kwargs (dict, optional): Аргументы. По умолчанию None.
            task_name (str, optional): Имя таски. По умолчанию None.
        """
        delta = delay if isinstance(delay, timedelta) else timedelta(
            seconds=delay)
        date = datetime.now(timezone.utc) + delta
        task = OneTimeTask(
            date=date,
            job=job,
            args=args,
            kwargs=kwargs,
            name=task_name,
        )
        heapq.heappush(self._tasks, task)

    @Actor.method
    def by_cron(
        self,
        schedule: str,
        job: Callable[[], Any],
        args: tuple = None,
        kwargs: dict = None,
        task_name: str = None,
    ) -> None:
        """
        Добавляет задачу которая будет выполнятся по CRON расписанию.

        Args:
            schedule (str): Расписание задачи в CRON формате.
            job (Callable[[], Any]): Вызываемая в задаче функция.
            args (tuple, optional): Аргументы. По умолчанию None.
            kwargs (dict, optional): Аргументы. По умолчанию None.
            task_name (str, optional): Имя таски. По умолчанию None.
        """
        task = CronTask(
            schedule=schedule,
            job=job,
            args=args,
            kwargs=kwargs,
            name=task_name,
        )
        heapq.heappush(self._tasks, task)

    @Actor.method
    def by_period(
        self,
        period: Union[timedelta, float],
        job: Callable[[], Any],
        args: tuple = None,
        kwargs: dict = None,
        task_name: str = None,
    ) -> None:
        """
        Добавляет задачу которая будет выполняется переодически через
        указанный интервал времени.

        Args:
            period (Union[timedelta, float]): Период выполнения задачи.
            job (Callable[[], Any]): Вызываемая в задаче функция.
            args (tuple, optional): Аргументы. По умолчанию None.
            kwargs (dict, optional): Аргументы. По умолчанию None.
            task_name (_type_, optional): Имя таски. По умолчанию None.
        """
        delta = period if isinstance(period, timedelta) else timedelta(
            seconds=period)
        task = PeriodicTask(
            period=delta,
            job=job,
            args=args,
            kwargs=kwargs,
            name=task_name,
        )
        heapq.heappush(self._tasks, task)

    @Actor.method
    def cancel(self, task_name: str) -> None:
        """
        Отменяет выполнение запланированной задачи.

        Args:
            task_name (str): Имя задачи для отмены.
        """
        for t in self._tasks:
            if t.name == task_name:
                del t
                heapq.heapify(self._tasks)
                break

    def _get_timeout(self) -> float:
        """
        Возвращает время ожидания поступлений сообщений
        в основном рабочем цикле.

        Returns:
            float: тайм-аут ожидания сообщений в inbox.
        """
        if len(self._tasks) < 1:
            return 60

        dt_next_task = self._tasks[0].next_run_time
        dt_current = datetime.now(timezone.utc)
        if dt_next_task < dt_current:
            return 0
        else:
            return (dt_next_task - dt_current).seconds

    def _on_timeout(self) -> None:
        """
        Обрабатывает истечение времени ожидания новых сообщений в inbox.
        """
        if self._tasks:
            # получаем ближайшую задачу из кучи
            task: Task = heapq.heappop(self._tasks)
            task.run_job()
            # высчитываем следующее время запуска задачи и добавляем ее в кучу
            task.set_next_run_time()
            if task.next_run_time:
                heapq.heappush(self._tasks, task)
