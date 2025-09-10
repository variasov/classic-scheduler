import heapq
import inspect
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from threading import Thread
from typing import Any, Callable, Union, Optional

from classic.components import Registry

from .task import CronTask, OneTimeTask, PeriodicTask, Task


Stop = object()


@dataclass
class CancelTask:
    """
    Команда отмены запланированной задачи.
    """
    task_name: str


class Scheduler(Registry):
    """
    Планировщик задач. Вызывает переданные ему Callable объекты в соответствии
    с расписанием.
    """
    _stopped: bool
    _thread: Optional[Thread]
    _thread_pool: Optional[ThreadPoolExecutor]

    def __init__(self, workers_num: int = 1) -> None:
        super().__init__()
        self._thread = None
        self._tasks = []
        self._inbox = Queue()
        if workers_num:
            self._thread_pool = ThreadPoolExecutor(max_workers=workers_num)
        else:
            self._thread_pool = None

    def _calc_next_task_execute_time(self) -> float:
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

    def _cancel(self, task_name: str) -> None:
        """
        Отменяет выполнение запланированной задачи.

        Args:
            task_name (str): Имя задачи для отмены.
        """
        tasks = [t for t in self._tasks if t.name != task_name]
        if len(self._tasks) != len(tasks):
            heapq.heapify(tasks)
            self._tasks = tasks

    def _execute_task(self, task: Task) -> None:
        """
        Выполняет текущую задачу.

        Args:
            task (Task): Задача, которую необходимо выполнить.
        """
        if self._thread_pool:
            self._thread_pool.submit(task.run_job)
        else:
            task.run_job()

    def _stop(self) -> None:
        self._stopped = True

    def run(self) -> None:
        """
        Главный цикл выполнения планировщика.
        """
        self._stopped = False
        while not self._stopped:
            try:
                timeout = self._calc_next_task_execute_time()
                if timeout > 0:
                    try:
                        command = self._inbox.get(block=True, timeout=timeout)
                    except Empty:
                        continue

                    if isinstance(command, Task):
                        self._schedule(command)
                    elif isinstance(command, CancelTask):
                        self._cancel(command.task_name)
                    elif command is Stop:
                        self._stop()
                else:
                    task: Task = heapq.heappop(self._tasks)
                    task.set_next_run_time()
                    if task.next_run_time:
                        self._schedule(task)
                    self._execute_task(task)
            except KeyboardInterrupt:
                self._stop()

        if self._thread_pool:
            self._thread_pool.shutdown(wait=True, cancel_futures=True)

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

        self._inbox.put(OneTimeTask(
            date=datetime.now(timezone.utc) + delay,
            job=job,
            args=args,
            kwargs=kwargs,
            name=task_name,
        ))

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
        self._inbox.put(CronTask(
            schedule=schedule,
            job=job,
            args=args,
            kwargs=kwargs,
            name=task_name,
        ))

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

        self._inbox.put(PeriodicTask(
            period=period,
            job=job,
            args=args,
            kwargs=kwargs,
            name=task_name,
        ))

    def cancel(self, task_name: str) -> None:
        """
        Отменяет выполнение запланированной задачи.

        Args:
            task_name (str): Имя задачи для отмены.
        """
        cancellation = CancelTask(task_name=task_name)
        self._inbox.put(cancellation)

    def start(self) -> None:
        """
        Запускает выполнение планировщика.
        """
        if not self._thread or not self._thread.is_alive():
            # TODO: Без deamon=True потоки не останавливались до завершения
            # выполнения задачи. Нужно найти аналогичное решение для
            # корректной остановки потока.
            self._thread = Thread(target=self.run, daemon=True)
            self._thread.start()

    def stop(self, block: bool = True, timeout: float = None) -> None:
        """
        Завершает выполнение планировщика.
        """
        if self._thread and self._thread.is_alive():
            self._inbox.put(Stop)
            if block:
                self._thread.join(timeout)

    def join(self, timeout: float = None):
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout)

    @staticmethod
    def _task_name_for_method(
        method: Callable,
        schedule: str | float | timedelta,
    ) -> str:
        # TODO: Ключи однозначно идентифицируют методы классов
        # в привязке к расписанию, но выглядят нечитаемо
        return f'{id(method)}.{schedule}'

    def register(self, obj: Any) -> None:
        for name, param in inspect.getmembers(obj):
            cron_schedules = getattr(param, '__by_cron__', None)
            if cron_schedules:
                for schedule in cron_schedules:
                    self.by_cron(
                        schedule, param,
                        task_name=self._task_name_for_method(obj, schedule),
                    )

            periodic_schedules = getattr(param, '__by_period__', None)
            if periodic_schedules:
                for schedule in periodic_schedules:
                    self._task_name_for_method(obj, schedule)
                    self.by_period(
                        schedule, param,
                        task_name=self._task_name_for_method(obj, schedule),
                    )

    def unregister(self, obj: Any) -> None:
        for name, param in inspect.getmembers(obj):
            cron_schedules = getattr(param, '__by_cron__', None)
            if cron_schedules:
                for schedule in cron_schedules:
                    self.cancel(self._task_name_for_method(obj, schedule))

            periodic_schedules = getattr(param, '__by_period__', None)
            if periodic_schedules:
                for schedule in periodic_schedules:
                    self._task_name_for_method(obj, schedule)
                    self.cancel(self._task_name_for_method(obj, schedule))
