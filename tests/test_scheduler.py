import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from functools import partial
from threading import Event, Thread

import pytest

from classic.scheduler import ScheduledJob, Scheduler
from classic.scheduler.task import CronTask, OneTimeTask, PeriodicTask

# Верхняя граница ожидания события. Тесты не опираются на точные интервалы:
# они ждут события с щедрым тайм-аутом и проверяют порядок и количество
# вызовов, а также нижние границы времени (задача не выполнена раньше срока).
TIMEOUT = 10


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _wait_subsecond_window(max_fraction: float = 0.4) -> None:
    """Ждёт начала секунды, чтобы до следующей границы секунды оставалось
    не меньше ``1 - max_fraction`` секунды.
    """
    while _now().microsecond / 1_000_000 > max_fraction:
        time.sleep(0.01)


@pytest.fixture
def stopped_scheduler() -> Scheduler:
    return Scheduler()


def test__scheduler__with_delay(scheduler: Scheduler) -> None:
    delay = 0.3
    result: list[int] = []
    executed = Event()
    executed_at: list[datetime] = []

    def task(a: int, b: int) -> None:
        result.append(a + b)
        executed_at.append(_now())
        executed.set()

    scheduled_at = _now()
    scheduler.with_delay(delay, task, args=(1,), kwargs={"b": 2})

    assert executed.wait(TIMEOUT)
    assert result == [3]
    # Задача не выполнена раньше срока
    assert executed_at[0] - scheduled_at >= timedelta(seconds=delay)


def test__scheduler__cancel(scheduler: Scheduler) -> None:
    executed_tasks: list[str] = []
    first_period_run = Event()
    sentinel = Event()

    def task(task_name_: str) -> None:
        executed_tasks.append(task_name_)
        if task_name_ == "by_period":
            first_period_run.set()

    # До границы секунды остаётся запас, чтобы cron-задача не успела
    # сработать до обработки отмены.
    _wait_subsecond_window()

    # Задача "by_period" выполняется сразу же, затем отменяется
    period_job = scheduler.by_period(1, task, args=("by_period",))
    assert first_period_run.wait(TIMEOUT)
    period_job.cancel()

    cron_job = scheduler.by_cron("* * * * * *", task, args=("by_cron",))
    cron_job.cancel()
    cron_job.cancel()  # повторная отмена безопасна

    scheduler.with_delay(1, task, args=("with_delay",)).cancel()

    # Команды обрабатываются по порядку: когда сработал маркер, все отмены
    # уже применены, а сроки отменённых задач (1 с, граница секунды, период
    # 1 с) наступили бы раньше.
    scheduler.with_delay(1.6, sentinel.set)
    assert sentinel.wait(TIMEOUT)

    assert executed_tasks == ["by_period"]


def test__scheduled_job__cancel_removes_only_this_job(
    scheduler: Scheduler,
) -> None:
    runs: list[str] = []
    sentinel = Event()

    def make(tag: str) -> Callable[[], None]:
        def job() -> None:
            runs.append(tag)

        return job

    # Обе задачи имеют одинаковый __qualname__ (make.<locals>.job).
    first = scheduler.with_delay(0.2, make("a"))
    scheduler.with_delay(0.2, make("b"))
    first.cancel()

    scheduler.with_delay(0.6, sentinel.set)
    assert sentinel.wait(TIMEOUT)

    assert runs == ["b"]


def test__scheduled_job__cancel_same_function_keeps_other(
    scheduler: Scheduler,
) -> None:
    count: list[int] = []
    sentinel = Event()

    def job() -> None:
        count.append(1)

    cancelled = scheduler.with_delay(0.2, job)
    scheduler.with_delay(0.2, job)
    cancelled.cancel()

    scheduler.with_delay(0.6, sentinel.set)
    assert sentinel.wait(TIMEOUT)

    assert count == [1]


def test__scheduled_job__cancel_is_idempotent(scheduler: Scheduler) -> None:
    executed = Event()
    sentinel = Event()

    job = scheduler.with_delay(0.2, executed.set)
    for _ in range(3):
        job.cancel()

    scheduler.with_delay(0.6, sentinel.set)
    assert sentinel.wait(TIMEOUT)

    assert not executed.is_set()


def test__scheduled_job__cancel_after_one_time_job_ran_is_noop(
    scheduler: Scheduler,
) -> None:
    executed = Event()
    other_ran = Event()

    job = scheduler.with_delay(0, executed.set)
    assert executed.wait(TIMEOUT)
    job.cancel()
    job.cancel()

    # Планировщик жив и продолжает работать.
    scheduler.with_delay(0, other_ran.set)
    assert other_ran.wait(TIMEOUT)
    assert scheduler.is_alive()


def test__scheduled_job__cancel_while_stopped_applies_on_restart(
    stopped_scheduler: Scheduler,
) -> None:
    executed = Event()
    sentinel = Event()

    job = stopped_scheduler.with_delay(0.2, executed.set)
    job.cancel()

    stopped_scheduler.start()
    stopped_scheduler.with_delay(0.6, sentinel.set)
    try:
        assert sentinel.wait(TIMEOUT)
        assert not executed.is_set()
    finally:
        stopped_scheduler.stop(block=True, timeout=TIMEOUT)


def test__scheduler__by_methods_return_scheduled_job(
    stopped_scheduler: Scheduler,
) -> None:
    jobs = [
        stopped_scheduler.with_delay(1, print),
        stopped_scheduler.by_period(1, print),
        stopped_scheduler.by_cron("* * * * *", print),
    ]

    assert all(type(job) is ScheduledJob for job in jobs)
    assert all("print" in repr(job) for job in jobs)


def test__scheduler__accepts_callable_without_qualname(
    scheduler: Scheduler,
) -> None:
    executed = Event()

    job = scheduler.with_delay(0, partial(executed.set))

    assert executed.wait(TIMEOUT)
    assert "partial" in repr(job)


def test__scheduler__by_cron(scheduler: Scheduler) -> None:
    results: list[int] = []
    expected_runs = 2
    all_done = Event()

    def task(a: int, b: int) -> None:
        a = results[-1] if results else a
        results.append(a + b)
        if len(results) == expected_runs:
            all_done.set()

    job = scheduler.by_cron("* * * * * *", task, args=(1,), kwargs={"b": 2})

    assert all_done.wait(TIMEOUT)
    job.cancel()

    assert results == [3, 5]


def test__cron_task__six_field_schedule_ticks_every_second() -> None:
    task = CronTask("* * * * * *", lambda: None, tz=timezone.utc)

    first = task.next_run_time
    assert first is not None
    assert first > _now() - timedelta(seconds=1)
    assert first.microsecond == 0

    task.set_next_run_time()

    assert task.next_run_time == first + timedelta(seconds=1)


def test__scheduler__by_period(scheduler: Scheduler) -> None:
    period = 0.3
    results: list[int] = []
    executed_at: list[datetime] = []
    expected_runs = 3
    all_done = Event()

    def task(a: int, b: int) -> None:
        a = results[-1] if results else a
        results.append(a + b)
        executed_at.append(_now())
        if len(results) == expected_runs:
            all_done.set()

    scheduled_at = _now()
    job = scheduler.by_period(period, task, args=(1,), kwargs={"b": 2})

    assert all_done.wait(TIMEOUT)
    job.cancel()

    assert results == [3, 5, 7]
    # Каждый запуск назначается не раньше чем через period после предыдущего
    # (запуск каждый раз стартует после назначения), поэтому третий запуск не
    # может случиться раньше двух периодов с момента добавления.
    elapsed = executed_at[-1] - scheduled_at
    assert elapsed >= timedelta(seconds=period * (expected_runs - 1))


def test__periodic_task__first_run_now_then_every_period() -> None:
    period = timedelta(seconds=5)
    before = _now()
    task = PeriodicTask(period, lambda: None)
    after = _now()

    assert task.next_run_time is not None
    assert before <= task.next_run_time <= after

    task.set_next_run_time()

    assert task.next_run_time is not None
    assert task.next_run_time >= before + period


def test__tasks__ordered_by_next_run_time() -> None:
    now = _now()
    late = OneTimeTask(now + timedelta(seconds=2), lambda: None)
    early = OneTimeTask(now + timedelta(seconds=1), lambda: None)

    assert early < late
    assert late > early

    early.set_next_run_time()

    assert early.next_run_time is None


def test__scheduler__run(stopped_scheduler: Scheduler) -> None:
    executed = Event()

    def stop() -> None:
        stopped_scheduler.stop()

    # Отложенная задача не должна выполниться: планировщик остановлен раньше
    stopped_scheduler.with_delay(60, executed.set)
    stopped_scheduler.with_delay(0, stop)

    thread = Thread(target=stopped_scheduler.run)
    thread.start()
    thread.join(TIMEOUT)

    assert not thread.is_alive()
    assert not stopped_scheduler.is_alive()
    assert not executed.is_set()
