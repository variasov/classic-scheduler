"""Планировщик задач."""

import contextlib
import heapq
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any

from .task import CronTask, OneTimeTask, PeriodicTask, Task

logger = logging.getLogger(__name__)


@dataclass
class CancelTask:
    """Команда отмены запланированной задачи."""

    task: Task


@dataclass
class Stop:
    """Команда остановки планировщика."""


class ScheduledJob:
    """Дескриптор запланированной задачи.

    Возвращается методами ``Scheduler.with_delay()``, ``by_period()`` и
    ``by_cron()``. Позволяет отменить именно эту задачу: две задачи с одной
    и той же функцией (или lambda) отменяются независимо друг от друга.

    Example:
        ::

            job = scheduler.by_period(60, collect_metrics)
            ...
            job.cancel()

    """

    def __init__(self, task: Task, inbox: Queue[object]) -> None:
        """Создаёт дескриптор. Используется планировщиком, не пользователем."""
        self._task = task
        self._inbox = inbox

    def __repr__(self) -> str:
        """Возвращает читаемое представление с именем функции задачи."""
        return f"<ScheduledJob {self._task.name}>"

    def cancel(self) -> None:
        """Отменяет эту задачу.

        Асинхронно: команда передаётся планировщику и применяется его
        циклом. Уже идущий запуск не прерывается. Повторный вызов, вызов
        после выполнения одноразовой задачи и вызов при остановленном
        планировщике безопасны: в последнем случае отмена применится при
        следующем запуске планировщика.

        Example:
            ::

                job = scheduler.with_delay(30, send_report)
                job.cancel()
                job.cancel()  # повторная отмена безопасна

        """
        self._inbox.put(CancelTask(task=self._task))


def _is_due(task: Task, now: datetime) -> bool:
    next_run_time = task.next_run_time
    if next_run_time is None:
        msg = "Scheduled task has no next run time"  # pragma: no mutate
        raise TypeError(msg)  # pragma: no mutate
    return next_run_time <= now  # pragma: no mutate


class Scheduler:
    """Планировщик задач внутри процесса.

    Вызывает переданные ``Callable`` объекты в соответствии с расписанием.
    Поддерживаются три типа задач: одноразовая через отсрочку
    (``with_delay``), периодическая (``by_period``) и по CRON-расписанию
    (``by_cron``). Каждый из этих методов возвращает ``ScheduledJob``,
    у которого есть ``cancel()`` для отмены именно этой задачи.

    Задачи можно добавлять до запуска и после него, из любого потока:
    команды передаются через внутреннюю очередь, а кучей задач владеет
    только поток планировщика. Задачи выполняются в пуле из ``workers_num``
    потоков; при ``workers_num=0`` - последовательно в самом потоке
    планировщика (долгая задача при этом задерживает остальные). Периодическая
    и CRON-задача не запускается повторно, пока не завершился её предыдущий
    запуск в пуле: такой запуск пропускается и не наверстывается; другие
    задачи это не затрагивает. Запланированные задачи сохраняются между
    ``stop()`` и последующим ``start()``/``run()``.

    Логирование: используются именованные логгеры иерархии
    ``classic.scheduler`` (``classic.scheduler.task``,
    ``classic.scheduler.scheduler``), корневой логгер не используется.
    Пропуск запуска - WARNING; исключение в задаче - ERROR с traceback
    (планировщик продолжает работу); начало и завершение запуска - DEBUG.
    Настраивается стандартным ``logging`` по имени родителя.

    Example:
        Блокирующий запуск в главном потоке (остановка по Ctrl+C)::

            from datetime import timedelta

            from classic.scheduler import Scheduler

            def ping() -> None:
                print("ping")

            if __name__ == "__main__":
                scheduler = Scheduler(workers_num=2)
                scheduler.with_delay(5, ping)
                scheduler.by_period(timedelta(minutes=1), ping)
                scheduler.by_cron("0 3 * * *", ping)
                scheduler.run()

        Настройка логирования (например, видеть начало и конец запусков)::

            import logging

            logging.getLogger("classic.scheduler").setLevel(logging.DEBUG)

        Запуск в фоновом потоке::

            scheduler = Scheduler()
            scheduler.start()
            try:
                job = scheduler.by_period(10, ping)
                ...
                job.cancel()  # отмена конкретной задачи
            finally:
                scheduler.stop(block=True)

    """

    _thread: Thread | None
    _thread_pool: ThreadPoolExecutor | None

    def __init__(
        self,
        workers_num: int = 1,
        tz: tzinfo | None = None,
    ) -> None:
        """Создаёт планировщик. Сам он не запускается: см. ``run()``/``start()``.

        Args:
            workers_num: Число потоков для выполнения задач. 0 - задачи
                выполняются последовательно в потоке планировщика. По
                умолчанию 1.
            tz: Часовой пояс по умолчанию для CRON-расписаний. None -
                локальный часовой пояс процесса.

        """
        self._workers_num = workers_num
        self._tz = tz
        self._thread = None  # pragma: no mutate
        self._thread_pool = None  # pragma: no mutate
        self._running = Event()
        self._tasks: list[Task] = []
        self._inbox: Queue[object] = Queue()

    def _add(self, task: Task) -> ScheduledJob:
        self._inbox.put(task)
        return ScheduledJob(task, self._inbox)

    def _calc_next_task_execute_time(self) -> float | None:
        """Возвращает время ожидания выполнения следующей задачи.

        Returns:
            Optional[float]: тайм-аут ожидания задачи,
                None - задач нет, ждать до следующей команды.

        """
        if not self._tasks:
            return None

        dt_next_task = self._tasks[0].next_run_time
        if dt_next_task is None:
            msg = "Scheduled task has no next run time"  # pragma: no mutate
            raise TypeError(msg)  # pragma: no mutate
        dt_current = datetime.now(timezone.utc)
        if dt_next_task < dt_current:  # pragma: no mutate
            return 0
        return (dt_next_task - dt_current).total_seconds()

    def _schedule(self, task: Task) -> None:
        heapq.heappush(self._tasks, task)

    def _execute_task(self, task: Task) -> None:
        """Выполняет текущую задачу.

        Args:
            task (Task): Задача, которую необходимо выполнить.

        """
        if self._thread_pool:
            previous = task.last_future
            if previous is not None and not previous.done():
                logger.warning(
                    "Task [%s] skipped: previous run in progress",  # pragma: no mutate
                    task.name,
                )
                return
            task.last_future = self._thread_pool.submit(task.run_job)
        else:
            task.run_job()

    def _handle(self, command: object) -> None:
        if isinstance(command, Task):
            self._schedule(command)
        elif isinstance(command, CancelTask):
            self._cancel(command.task)
        elif isinstance(command, Stop):
            self._running.clear()

    def _run_due_tasks(self) -> None:
        now = datetime.now(timezone.utc)
        while self._tasks and _is_due(self._tasks[0], now):
            task: Task = heapq.heappop(self._tasks)
            task.set_next_run_time()
            if task.next_run_time:
                self._schedule(task)
            self._execute_task(task)

    def _loop(self) -> None:
        """Одна итерация цикла: ожидание команды или выполнение задач,
        время которых наступило.
        """
        timeout = self._calc_next_task_execute_time()
        if timeout is None or timeout > 0:  # pragma: no mutate
            try:
                command = self._inbox.get(timeout=timeout)  # pragma: no mutate
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
                self._thread_pool.shutdown(cancel_futures=True)
                self._thread_pool = None  # pragma: no mutate
            self._drain_inbox()
            self._running.clear()

    def _drain_inbox(self) -> None:
        """Разбирает команды, пришедшие после остановки: задачи и отмены
        сохраняются до следующего запуска, лишние Stop отбрасываются.
        """
        while True:
            try:
                command = self._inbox.get(block=False)  # pragma: no mutate
            except Empty:
                return
            if not isinstance(command, Stop):
                self._handle(command)

    def run(self) -> None:
        """Запускает планировщик в текущем потоке и блокирует его.

        Возвращает управление после вызова ``stop()`` (из другого потока или
        из самой задачи) или по Ctrl+C: ``KeyboardInterrupt`` подавляется,
        планировщик останавливается штатно.

        Raises:
            RuntimeError: Планировщик уже запущен.

        Example:
            ::

                if __name__ == "__main__":
                    scheduler.run()

        """
        if self._running.is_set():
            raise RuntimeError("Scheduler is already running")  # pragma: no mutate
        self._running.set()
        with contextlib.suppress(KeyboardInterrupt):
            self._run_loop()

    def start(self, daemon: bool = False) -> None:
        """Запускает планировщик в отдельном потоке и сразу возвращает управление.

        Повторный вызов при работающем планировщике ничего не делает.

        Args:
            daemon: Сделать поток daemon. По умолчанию False: процесс не
                завершится, пока не вызван ``stop()``. При True процесс может
                завершиться в любой момент, а выполняющиеся задачи при этом
                обрываются.

        Example:
            ::

                scheduler.start()
                try:
                    ...
                finally:
                    scheduler.stop(block=True)

        """
        if self._running.is_set():
            return
        self._running.set()
        self._thread = Thread(
            target=self._run_loop,
            name="classic-scheduler",  # pragma: no mutate
            daemon=daemon,
        )
        self._thread.start()

    def stop(self, block: bool = False, timeout: float | None = None) -> None:
        """Останавливает планировщик, запущенный через ``run()`` или ``start()``.

        Выполняющиеся задачи дорабатывают, ещё не начатые задачи в пуле
        отменяются. Если планировщик не запущен, ничего не делает.
        Остановка асинхронная: без ``block=True`` метод возвращается сразу.

        Args:
            block: Дождаться завершения потока. Ожидает только поток,
                созданный через ``start()``; для ``run()`` не ждёт.
                По умолчанию False.
            timeout: Максимальное время ожидания в секундах при
                ``block=True``. None - без ограничения.

        """
        if self._running.is_set():
            self._inbox.put(Stop())
        if block:
            self.join(timeout)

    def join(self, timeout: float | None = None) -> None:
        """Ждёт завершения потока, запущенного через ``start()``.

        Аналог ``threading.Thread.join()``. Если поток не создавался, ничего
        не делает. Для планировщика, запущенного через ``run()``, не ждёт.

        Args:
            timeout: Время ожидания в секундах. None - без ограничения.

        """
        if self._thread is not None:
            self._thread.join(timeout)

    def is_alive(self) -> bool:
        """Проверяет, работает ли планировщик.

        Аналог ``threading.Thread.is_alive()``.

        Returns:
            True, пока планировщик работает, запущен ли он через ``run()``
            или через ``start()``.

        """
        return self._running.is_set()

    def with_delay(
        self,
        delay: timedelta | float,
        job: Callable[..., Any],
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> ScheduledJob:
        """Добавляет одноразовую задачу, выполняемую через заданную отсрочку.

        Задача будет выполнена ровно один раз. Отсрочка отсчитывается от
        момента вызова метода. Можно вызывать до запуска и после него, из
        любого потока.

        Args:
            delay: Отсрочка: число секунд или ``timedelta``.
            job: Вызываемый объект, который будет выполнен.
            args: Позиционные аргументы, с которыми вызывается ``job``.
            kwargs: Именованные аргументы, с которыми вызывается ``job``.

        Returns:
            Дескриптор задачи; ``cancel()`` отменяет именно её.

        Example:
            ::

                job = scheduler.with_delay(30, send_report, kwargs={"to": "ops"})
                job.cancel()  # если отчёт больше не нужен

        """
        if not isinstance(delay, timedelta):
            delay = timedelta(seconds=delay)

        return self._add(
            OneTimeTask(
                date=datetime.now(timezone.utc) + delay,
                job=job,
                args=args,
                kwargs=kwargs,
            )
        )

    def by_cron(
        self,
        schedule: str,
        job: Callable[..., Any],
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        tz: tzinfo | None = None,
    ) -> ScheduledJob:
        """Добавляет задачу, выполняемую по CRON-расписанию.

        Расписание разбирает ``croniter``: стандартные 5 полей (минута, час,
        день месяца, месяц, день недели). Необязательное 6-е поле в конце -
        секунды (например, ``"* * * * * *"`` - каждую секунду). Часовой пояс:
        аргумент ``tz``, иначе ``tz`` планировщика, иначе локальный пояс
        процесса. Для расписаний с корректным переходом на летнее время
        используйте ``zoneinfo.ZoneInfo``: фиксированные смещения
        (``timezone(timedelta(...))``) за переходом не следуют.

        Если предыдущий запуск ещё не завершился, очередной запуск
        пропускается с предупреждением в лог; следующий выполняется по
        расписанию, пропущенные не наверстываются.

        Args:
            schedule: Расписание в CRON-формате.
            job: Вызываемый объект, который будет выполнен.
            args: Позиционные аргументы, с которыми вызывается ``job``.
            kwargs: Именованные аргументы, с которыми вызывается ``job``.
            tz: Часовой пояс расписания. None - часовой пояс планировщика.

        Returns:
            Дескриптор задачи; ``cancel()`` отменяет именно её.

        Example:
            ::

                from zoneinfo import ZoneInfo

                job = scheduler.by_cron(
                    "30 9 * * 1-5",
                    send_report,
                    tz=ZoneInfo("Europe/Moscow"),
                )

        """
        return self._add(
            CronTask(
                schedule=schedule,
                tz=tz or self._tz,
                job=job,
                args=args,
                kwargs=kwargs,
            )
        )

    def by_period(
        self,
        period: timedelta | float,
        job: Callable[..., Any],
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> ScheduledJob:
        """Добавляет периодическую задачу.

        Первый запуск происходит сразу после добавления, каждый следующий
        назначается через ``period`` после момента, когда предыдущий запуск
        был отправлен на выполнение (а не после его завершения). Если
        предыдущий запуск ещё не завершился (задача работает дольше периода),
        очередной запуск пропускается с предупреждением в лог, а следующий
        назначается через ``period``; пропущенные запуски не накапливаются.
        Запуски одной задачи не пересекаются (при ``workers_num=0`` они и так
        последовательны).

        Args:
            period: Период: число секунд или ``timedelta``.
            job: Вызываемый объект, который будет выполнен.
            args: Позиционные аргументы, с которыми вызывается ``job``.
            kwargs: Именованные аргументы, с которыми вызывается ``job``.

        Returns:
            Дескриптор задачи; ``cancel()`` отменяет именно её.

        Example:
            ::

                job = scheduler.by_period(60, collect_metrics)
                ...
                job.cancel()

        """
        if not isinstance(period, timedelta):
            period = timedelta(seconds=period)

        return self._add(
            PeriodicTask(
                period=period,
                job=job,
                args=args,
                kwargs=kwargs,
            )
        )

    def _cancel(self, task: Task) -> None:
        """Убирает из кучи ровно эту задачу (по идентичности объекта).

        Args:
            task (Task): Задача для отмены.

        """
        tasks = [t for t in self._tasks if t is not task]
        if len(self._tasks) != len(tasks):
            heapq.heapify(tasks)
            self._tasks = tasks
