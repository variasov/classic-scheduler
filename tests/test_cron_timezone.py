from datetime import datetime, timedelta, timezone
from threading import Event

from classic.scheduler import Scheduler
from classic.scheduler.task import CronTask, Task

TIMEOUT = 10

PLUS_5 = timezone(timedelta(hours=5))
PLUS_7 = timezone(timedelta(hours=7))


def job() -> None:
    pass


def next_run_time(task: CronTask) -> datetime:
    value = task.next_run_time
    assert value is not None
    return value


def test__cron__local_timezone_by_default() -> None:
    task = CronTask("0 9 * * *", job=job)

    assert next_run_time(task).astimezone().hour == 9
    assert next_run_time(task).astimezone().minute == 0


def test__cron__explicit_timezone() -> None:
    task = CronTask("0 9 * * *", job=job, tz=PLUS_5)

    assert next_run_time(task).astimezone(PLUS_5).hour == 9
    assert next_run_time(task).astimezone(timezone.utc).hour == 4


def test__cron__next_run_time_in_timezone() -> None:
    task = CronTask("0 9 * * *", job=job, tz=PLUS_7)
    first = next_run_time(task)

    task.set_next_run_time()

    assert next_run_time(task) - first == timedelta(days=1)
    assert next_run_time(task).astimezone(PLUS_7).hour == 9


def _scheduled_cron_task(scheduler: Scheduler) -> Task:
    """Достаёт единственную поставленную в очередь задачу.

    Единственное место с доступом к внутренностям: у планировщика нет
    публичного способа узнать часовой пояс расписания, не дожидаясь
    срабатывания cron (до суток).
    """
    task = scheduler._inbox.get_nowait()
    assert isinstance(task, Task)
    assert scheduler._inbox.empty()
    return task


def _hour_in(task: Task, tz: timezone) -> int:
    value = task.next_run_time
    assert value is not None
    return value.astimezone(tz).hour


def test__by_cron__uses_scheduler_timezone() -> None:
    scheduler = Scheduler(tz=PLUS_5)

    scheduler.by_cron("0 9 * * *", job)

    task = _scheduled_cron_task(scheduler)
    assert isinstance(task, CronTask)
    assert _hour_in(task, PLUS_5) == 9


def test__by_cron__explicit_timezone_overrides_scheduler() -> None:
    scheduler = Scheduler(tz=PLUS_5)

    scheduler.by_cron("0 9 * * *", job, tz=PLUS_7)

    assert _hour_in(_scheduled_cron_task(scheduler), PLUS_7) == 9


def test__by_cron__explicit_timezone_without_scheduler_timezone() -> None:
    scheduler = Scheduler()

    scheduler.by_cron("0 9 * * *", job, tz=PLUS_7)

    assert _hour_in(_scheduled_cron_task(scheduler), PLUS_7) == 9


def test__by_cron__passes_arguments_and_name() -> None:
    scheduler = Scheduler()
    result: list[tuple[int, int]] = []
    executed = Event()

    def target(a: int, b: int = 0) -> None:
        result.append((a, b))
        # Отмена по имени: больше запусков быть не должно.
        scheduler.cancel("n")
        executed.set()

    scheduler.by_cron("* * * * * *", target, args=(1,), kwargs={"b": 2}, task_name="n")
    scheduler.start()
    assert executed.wait(TIMEOUT)
    # Следующий запуск наступил бы не позже чем через секунду.
    Event().wait(1.5)
    scheduler.stop(block=True, timeout=TIMEOUT)

    assert result == [(1, 2)]
