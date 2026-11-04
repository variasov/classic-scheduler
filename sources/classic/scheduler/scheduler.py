import heapq
import inspect
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any, Callable, Union, Optional

from classic.components import Registry

from .task import CronTask, OneTimeTask, PeriodicTask, Task


@dataclass
class CancelTask:
    """
    Команда отмены запланированной задачи.
    """
    task_name: str


@dataclass
class Stop:
    """
    Команда остановки планировщика.
    """


class Scheduler(Registry):
    """
    Планировщик задач. Вызывает переданные ему Callable объекты в соответствии
    с расписанием.

    Запускается в текущем потоке методом run() или в отдельном потоке
    методом start(). Останавливается методом stop() в обоих случаях.
    """
    _thread: Optional[Thread]
    _thread_pool: Optional[ThreadPoolExecutor]

    def __init__(self, workers_num: int = 1) -> None:
        super().__init__()
        self._workers_num = workers_num
        self._thread = None
        self._thread_pool = None
        self._running = Event()
        self._tasks = []
        self._inbox = Queue()

    def _calc_next_task_execute_time(self) -> Optional[float]:
        """
        Возвращает время ожидания выполнения следующей задачи.

        Returns:
            Optional[float]: тайм-аут ожидания задачи,
                None - задач нет, ждать до следующей команды.
        """
        if not self._tasks:
            return None

        dt_next_task = self._tasks[0].next_run_time
        dt_current = datetime.now(timezone.utc)
        if dt_next_task < dt_current:
            return 0
        else:
            return (dt_next_task - dt_current).total_seconds()

    def _schedule(self, task: Task) -> None:
        heapq.heappush(self._tasks, task)

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

    def _handle(self, command: Any) -> None:
        if isinstance(command, Task):
            self._schedule(command)
        elif isinstance(command, CancelTask):
            self._cancel(command.task_name)
        elif isinstance(command, Stop):
            self._running.clear()

    def _run_due_tasks(self) -> None:
        now = datetime.now(timezone.utc)
        while self._tasks and self._tasks[0].next_run_time <= now:
            task: Task = heapq.heappop(self._tasks)
            task.set_next_run_time()
            if task.next_run_time:
                self._schedule(task)
            self._execute_task(task)

    def _loop(self) -> None:
        """
        Одна итерация цикла: ожидание команды или выполнение задач,
        время которых наступило.
        """
        timeout = self._calc_next_task_execute_time()
        if timeout is None or timeout > 0:
            try:
                command = self._inbox.get(block=True, timeout=timeout)
            except Empty:
                self._run_due_tasks()
            else:
                self._handle(command)
        else:
            self._run_due_tasks()

    def _run_loop(self) -> None:
        if self._workers_num:
            self._thread_pool = ThreadPoolExecutor(
                max_workers=self._workers_num,
            )
        try:
            while self._running.is_set():
                self._loop()
        finally:
            if self._thread_pool:
                self._thread_pool.shutdown(wait=True, cancel_futures=True)
                self._thread_pool = None
            self._drain_inbox()
            self._running.clear()

    def _drain_inbox(self) -> None:
        """
        Разбирает команды, пришедшие после остановки: задачи и отмены
        сохраняются до следующего запуска, лишние Stop отбрасываются.
        """
        while True:
            try:
                command = self._inbox.get(block=False)
            except Empty:
                return
            if not isinstance(command, Stop):
                self._handle(command)

    def run(self) -> None:
        """
        Выполняет планировщик в текущем потоке до вызова stop()
        или KeyboardInterrupt.
        """
        if self._running.is_set():
            raise RuntimeError('Scheduler is already running')
        self._running.set()
        try:
            self._run_loop()
        except KeyboardInterrupt:
            pass

    def start(self, daemon: bool = False) -> None:
        """
        Запускает планировщик в отдельном потоке.

        Args:
            daemon (bool, optional): Сделать поток daemon. Такой поток
                не мешает завершению процесса, но выполняемые задачи
                при этом обрываются. По умолчанию False.
        """
        if self._running.is_set():
            return
        self._running.set()
        self._thread = Thread(
            target=self._run_loop,
            name='classic-scheduler',
            daemon=daemon,
        )
        self._thread.start()

    def stop(self, block: bool = False, timeout: Optional[float] = None) -> None:
        """
        Останавливает планировщик, запущенный через run() или start().
        Выполняемые задачи дорабатывают, ещё не начатые отменяются.
        Если планировщик не запущен, ничего не делает.

        Args:
            block (bool, optional): Дождаться остановки потока,
                запущенного через start(). По умолчанию False.
            timeout (Optional[float], optional): Время ожидания
                остановки в секундах. По умолчанию None - без ограничения.
        """
        if self._running.is_set():
            self._inbox.put(Stop())
        if block:
            self.join(timeout)

    def join(self, timeout: Optional[float] = None) -> None:
        """
        Ждёт завершения потока, запущенного через start().

        Args:
            timeout (Optional[float], optional): Время ожидания в секундах.
                По умолчанию None - без ограничения.
        """
        if self._thread is not None:
            self._thread.join(timeout)

    def is_alive(self) -> bool:
        """
        Возвращает True, пока планировщик работает,
        запущен ли он через run() или через start().
        """
        return self._running.is_set()

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
