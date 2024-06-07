import time
from datetime import datetime
from threading import Thread

import pytest
from classic.scheduler import (BaseScheduler, SimpleScheduler,
                               ThreadPoolScheduler)


@pytest.fixture(scope='function')
def scheduler() -> SimpleScheduler:
    scheduler = SimpleScheduler()
    scheduler.start()

    yield scheduler

    scheduler.stop()


@pytest.fixture(scope='function')
def stopped_scheduler() -> SimpleScheduler:
    return SimpleScheduler()


@pytest.fixture(scope='function')
def thread_pool_scheduler() -> ThreadPoolScheduler:
    scheduler = ThreadPoolScheduler()
    scheduler.start()

    yield scheduler

    scheduler.stop()


@pytest.fixture(scope='function')
def stopped_thread_pool_scheduler() -> ThreadPoolScheduler:
    return ThreadPoolScheduler()


def test__base_scheduler__with_delay(scheduler: BaseScheduler) -> None:
    result = None

    def task(a: int, b: int) -> None:
        nonlocal result
        result = a + b

    scheduler.with_delay(0.05, task, args=(1, ), kwargs=dict(b=2))

    time.sleep(0.04)
    assert result is None

    time.sleep(0.02)
    assert result == 3


def test__base_scheduler__unshedule(scheduler: BaseScheduler) -> None:

    executed_tasks = []
    expected_tasks = []

    def task(task_name: str) -> None:
        executed_tasks.append(task_name)

    task_name = 'by_cron'
    scheduler.by_cron(
        '* * * * * *',
        task,
        args=(task_name, ),
        task_name=task_name,
    )
    scheduler.unshedule(task_name)
    time.sleep(0.045)
    scheduler.unshedule(task_name)

    task_name = 'with_delay'
    scheduler.with_delay(0.05, task, args=(task_name, ), task_name=task_name)
    time.sleep(0.045)
    scheduler.unshedule(task_name)

    task_name = 'by_period'
    scheduler.by_period(
        0.05,
        task,
        args=(task_name, ),
        task_name=task_name,
    )
    # Задача "by_period" выполняется сразу же
    expected_tasks.append(task_name)
    time.sleep(0.045)
    scheduler.unshedule(task_name)

    time.sleep(1.1)
    assert executed_tasks == expected_tasks


def test__base_scheduler__by_cron(scheduler: BaseScheduler) -> None:
    task_name = 'by_cron'
    all_results = [3, 5]
    results = []
    expected_results = []

    def task(a: int, b: int) -> None:
        a = results[-1] if results else a
        task_result = a + b
        results.append(task_result)

    scheduler.by_cron(
        '* * * * * *',
        task,
        args=(1, ),
        kwargs=dict(b=2),
        task_name=task_name,
    )
    for result in all_results:
        tick = 0.08
        now = datetime.utcnow()
        time_from = now.replace(microsecond=0)
        delta = 1 - (now - time_from).total_seconds()

        delay = max(delta - tick, 0)
        time.sleep(delay)
        assert results == expected_results

        time.sleep(delta + tick)
        expected_results.append(result)
        assert results == expected_results

    scheduler.unshedule(task_name)


def test__base_scheduler__by_period(scheduler: BaseScheduler) -> None:
    task_name = 'by_period'
    all_results = [3, 5, 7]
    results = []
    expected_results = []

    def task(a: int, b: int) -> None:
        a = results[-1] if results else a
        task_result = a + b
        results.append(task_result)

    scheduler.by_period(
        0.05,
        task,
        args=(1, ),
        kwargs=dict(b=2),
        task_name=task_name,
    )

    for result in all_results:
        time.sleep(0.005)
        expected_results.append(result)
        assert results == expected_results

        time.sleep(0.04)
        assert results == expected_results

        time.sleep(0.005)

    scheduler.unshedule(task_name)


def test__base_scheduler__loop(stopped_scheduler: BaseScheduler) -> None:
    result = False

    def run() -> None:
        stopped_scheduler.with_delay(0.05, task)
        time.sleep(0.045)
        stopped_scheduler.stop()

    def task() -> None:
        nonlocal result
        result = True

    thread = Thread(target=run)
    thread.start()

    stopped_scheduler.loop()
    time.sleep(0.05)
    assert not thread.is_alive()
    assert not result


def test__thread_pool_scheduler__with_delay(
    thread_pool_scheduler: ThreadPoolScheduler,
) -> None:
    test__base_scheduler__with_delay(thread_pool_scheduler)


def test__thread_pool_scheduler__unshedule(
    thread_pool_scheduler: ThreadPoolScheduler,
) -> None:
    test__base_scheduler__unshedule(thread_pool_scheduler)


def test__thread_pool_scheduler__by_cron(
    thread_pool_scheduler: ThreadPoolScheduler,
) -> None:
    test__base_scheduler__by_cron(thread_pool_scheduler)


def test__thread_pool_scheduler__by_period(
    thread_pool_scheduler: ThreadPoolScheduler,
) -> None:
    test__base_scheduler__by_period(thread_pool_scheduler)


def test__thread_pool_scheduler__loop(
    thread_pool_scheduler: ThreadPoolScheduler,
) -> None:
    test__base_scheduler__loop(thread_pool_scheduler)
