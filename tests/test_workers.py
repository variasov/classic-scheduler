import threading
from threading import Condition, Event, Thread

import pytest

from classic.scheduler import Scheduler

TIMEOUT = 10
# Время, в течение которого лишняя задача успела бы стартовать, если бы
# воркеров было больше заявленного. Ошибка проверки возможна только в
# сторону пропуска дефекта, но не ложного падения.
GRACE = 0.3


class _Probe:
    """Задача, считающая, сколько её экземпляров выполняется одновременно."""

    def __init__(self) -> None:
        self.release = Event()
        self.active = 0
        self.peak = 0
        self.finished = 0
        self._cond = Condition()

    def __call__(self) -> None:
        with self._cond:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self._cond.notify_all()
        self.release.wait(TIMEOUT)
        with self._cond:
            self.active -= 1
            self.finished += 1
            self._cond.notify_all()

    def wait_active(self, count: int) -> bool:
        with self._cond:
            return self._cond.wait_for(lambda: self.active >= count, TIMEOUT)

    def wait_finished(self, count: int) -> bool:
        with self._cond:
            return self._cond.wait_for(lambda: self.finished >= count, TIMEOUT)


def _check_concurrency(scheduler: Scheduler, expected: int) -> None:
    """Запускает expected + 1 блокирующих задач: одновременно должны идти
    ровно expected, остальная ждёт свободного воркера.
    """
    probe = _Probe()
    scheduler.start()
    try:
        for _ in range(expected + 1):
            scheduler.with_delay(0, probe)

        assert probe.wait_active(expected)
        Event().wait(GRACE)
        assert probe.peak == expected

        probe.release.set()
        assert probe.wait_finished(expected + 1)
    finally:
        probe.release.set()
        scheduler.stop(block=True, timeout=TIMEOUT)


def test__workers_num__default_runs_one_job_at_a_time() -> None:
    _check_concurrency(Scheduler(), 1)


@pytest.mark.parametrize("workers_num", [1, 2, 3])
def test__workers_num__limits_concurrent_jobs(workers_num: int) -> None:
    _check_concurrency(Scheduler(workers_num=workers_num), workers_num)


def test__pool__job_runs_outside_scheduler_thread() -> None:
    scheduler = Scheduler(workers_num=1)
    scheduler.start()
    names: list[str] = []
    done = Event()

    def target() -> None:
        names.append(threading.current_thread().name)
        done.set()

    scheduler.with_delay(0, target)
    assert done.wait(TIMEOUT)
    scheduler.stop(block=True, timeout=TIMEOUT)

    assert names[0] != "classic-scheduler"


def test__no_pool__job_runs_in_scheduler_thread() -> None:
    scheduler = Scheduler(workers_num=0)
    scheduler.start()
    names: list[str] = []
    done = Event()

    def target() -> None:
        names.append(threading.current_thread().name)
        done.set()

    scheduler.with_delay(0, target)
    assert done.wait(TIMEOUT)
    scheduler.stop(block=True, timeout=TIMEOUT)

    assert names == ["classic-scheduler"]


def test__no_pool__job_runs_in_run_caller_thread() -> None:
    scheduler = Scheduler(workers_num=0)
    names: list[str] = []

    def target() -> None:
        names.append(threading.current_thread().name)
        scheduler.stop()

    scheduler.with_delay(0, target)
    thread = Thread(target=scheduler.run, name="caller")
    thread.start()
    thread.join(TIMEOUT)

    assert names == ["caller"]


def test__stop__cancels_jobs_queued_in_pool() -> None:
    scheduler = Scheduler(workers_num=1)
    scheduler.start()
    probe = _Probe()
    second_ran = Event()
    scheduler.with_delay(0, probe)
    assert probe.wait_active(1)
    scheduler.with_delay(0, second_ran.set)
    # Задача b наступила и ушла в очередь пула (воркер занят задачей a).
    Event().wait(GRACE)

    scheduler.stop()
    # is_alive() становится False, когда планировщик принял команду остановки;
    # сразу после этого поток планировщика отменяет очередь пула. Полностью
    # детерминированного публичного признака отмены нет, поэтому даём запас,
    # пока задача a ещё удерживает воркер (при сбое тест лишь пропустит
    # дефект, но не упадёт на корректной реализации).
    while scheduler.is_alive():
        Event().wait(0.01)
    Event().wait(GRACE)
    probe.release.set()
    scheduler.join(TIMEOUT)

    assert probe.finished == 1
    assert not second_ran.is_set()
