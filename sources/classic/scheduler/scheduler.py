import heapq
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from threading import Thread
from typing import Any, Callable, Union

from .task import (CronTask, OneTimeTask, PeriodicTask, ScheduleCancellation,
                   Task)


class BaseScheduler(ABC):
    """
    Планировщик задач. Вызывает переданные ему Callable объекты в соответствии
    с расписанием.
    """

    def __init__(self) -> None:
        super().__init__()
        self._executor = Thread(target=self._run)
        self._tasks = []
        self._inbox = Queue()
        self._stopped = False

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

    def _unshedule(self, task_name: str) -> None:
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

    @abstractmethod
    def _execute(self, task: Task) -> None:
        """
        Выполняет текущую задачу.

        Args:
            task (Task): Задача, которую необходимо выполнить.
        """
        pass

    def _run(self) -> None:
        """
        Главный цикл выполнения планировщика.
        """
        while not self._stopped:
            timeout = self._get_timeout()
            if timeout > 0:
                try:
                    command = self._inbox.get(block=True, timeout=timeout)
                except Empty:
                    pass
                else:
                    if isinstance(command, Task):
                        heapq.heappush(self._tasks, command)
                    elif isinstance(command, ScheduleCancellation):
                        self._unshedule(command.task_name)
            else:
                # получаем и выполняем ближайшую задачу из кучи
                task: Task = heapq.heappop(self._tasks)
                self._execute(task)

                # высчитываем следующее время запуска задачи и добавляем ее в
                # кучу
                task.set_next_run_time()
                if task.next_run_time:
                    heapq.heappush(self._tasks, task)

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

    def start(self, wait: bool = True) -> None:
        """
        Запускает выполнение планировщика.

        Args:
            wait (bool): Ожидает завершение выполнения планировщика в случае
                         значения True.
        """
        self._executor.start()
        if wait:
            self._executor.join()

    def stop(self, wait: bool = True) -> None:
        """
        Завершает выполнение планировщика.

        Args:
            wait (bool): Ожидает завершение выполнения планировщика в случае
                         значения True.
        """
        self._stopped = True
        if wait:
            self._executor.join()


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

    def stop(self, wait: bool = True) -> None:
        super(ThreadPoolScheduler, self).stop(wait=wait)
        self._thread_pool.shutdown(wait=wait)
