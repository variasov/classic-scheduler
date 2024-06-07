import heapq
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from threading import Thread
from typing import Any, Callable, Union

from .task import (STOP, CronTask, OneTimeTask, PeriodicTask,
                   ScheduleCancellation, Task)


class BaseScheduler(ABC):
    """
    Планировщик задач. Вызывает переданные ему Callable объекты в соответствии
    с расписанием.
    """
    _stopped: bool

    def __init__(self) -> None:
        super().__init__()
        self._thread = Thread(target=self.loop)
        self._tasks = []
        self._inbox = Queue()

    def _get_timeout(self) -> float:
        """
        Возвращает время ожидания выполнения следующей задачи.

        Returns:
            float: тайм-аут ожидания задачи.
        """
        if len(self._tasks) < 1:
            return 60

        dt_next_task = self._tasks[0].next_run_time
        dt_current = datetime.now(timezone.utc)
        if dt_next_task < dt_current:
            return 0
        else:
            return (dt_next_task - dt_current).total_seconds()

    def _schedule(self, task: Task) -> None:
        heapq.heappush(self._tasks, task)

    def _unshedule(self, task_name: str) -> None:
        """
        Отменяет выполнение запланированной задачи.

        Args:
            task_name (str): Имя задачи для отмены.
        """
        tasks = [t for t in self._tasks if t.name != task_name]
        if len(self._tasks) != len(tasks):
            heapq.heapify(tasks)
            self._tasks = tasks

    @abstractmethod
    def _execute(self, task: Task) -> None:
        """
        Выполняет текущую задачу.

        Args:
            task (Task): Задача, которую необходимо выполнить.
        """
        pass

    def _stop(self) -> None:
        self._stopped = True

    def loop(self) -> None:
        """
        Главный цикл выполнения планировщика.
        """
        self._stopped = False
        while not self._stopped:
            try:
                timeout = self._get_timeout()
                if timeout > 0:
                    try:
                        command = self._inbox.get(block=True, timeout=timeout)
                    except Empty:
                        continue

                    if isinstance(command, Task):
                        self._schedule(command)
                    elif isinstance(command, ScheduleCancellation):
                        self._unshedule(command.task_name)
                    elif command is STOP:
                        break
                else:
                    task: Task = heapq.heappop(self._tasks)
                    self._execute(task)

                    task.set_next_run_time()
                    if task.next_run_time:
                        self._schedule(task)
            except KeyboardInterrupt:
                break
        self._stop()

    def with_delay(
        self,
        delay: Union[timedelta, float],
        job: Callable[[], None],
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
        if not isinstance(delay, timedelta):
            delay = timedelta(seconds=delay)

        date = datetime.now(timezone.utc) + delay
        task = OneTimeTask(
            date=date,
            job=job,
            args=args,
            kwargs=kwargs,
            name=task_name,
        )
        self._inbox.put(task)

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
        self._inbox.put(task)

    def by_period(
        self,
        period: Union[timedelta, float],
        job: Callable[[], Any],
        args: tuple = None,
        kwargs: dict = None,
        task_name: str = None,
    ) -> None:
        """
        Добавляет задачу которая будет выполняется периодически через
        указанный интервал времени.

        Args:
            period (Union[timedelta, float]): Период выполнения задачи.
            job (Callable[[], Any]): Вызываемая в задаче функция.
            args (tuple, optional): Аргументы. По умолчанию None.
            kwargs (dict, optional): Аргументы. По умолчанию None.
            task_name (_type_, optional): Имя таски. По умолчанию None.
        """
        if not isinstance(period, timedelta):
            period = timedelta(seconds=period)

        task = PeriodicTask(
            period=period,
            job=job,
            args=args,
            kwargs=kwargs,
            name=task_name,
        )
        self._inbox.put(task)

    def unshedule(self, task_name: str) -> None:
        """
        Отменяет выполнение запланированной задачи.

        Args:
            task_name (str): Имя задачи для отмены.
        """
        cancellation = ScheduleCancellation(task_name=task_name)
        self._inbox.put(cancellation)

    def start(self) -> None:
        """
        Запускает выполнение планировщика.
        """
        self._thread.start()

    def stop(self) -> None:
        """
        Завершает выполнение планировщика.
        """
        self._inbox.put(STOP)


class SimpleScheduler(BaseScheduler):
    """
    Планировщик задач. Вызывает переданные ему Callable объекты в своем потоке
    в соответствии с расписанием. Поскольку поток один, то не допускается
    параллельное выполнение задач.
    """

    def _execute(self, task: Task) -> None:
        task.run_job()


class ThreadPoolScheduler(BaseScheduler):
    """
    Планировщик задач. Вызывает переданные ему Callable объекты в пуле потоков
    в соответствии с расписанием.

    Данный планировщик не предоставляет гарантий
    того, что одна и та же задача не будет работать в разных потоках
    одновременно.
    """

    def __init__(self, workers_count: int = 5):
        super().__init__()
        self._thread_pool = ThreadPoolExecutor(max_workers=workers_count)

    def _execute(self, task: Task) -> None:
        self._thread_pool.submit(task.run_job)

    def _stop(self) -> None:
        super()._stop()
        self._thread_pool.shutdown(wait=True)
