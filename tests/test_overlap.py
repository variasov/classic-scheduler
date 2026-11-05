import logging
import time
from collections.abc import Callable
from threading import Condition, Event

import pytest

from classic.scheduler import Scheduler

TIMEOUT = 10
# Запас времени, за который пропущенный запуск успел бы стартовать при
# дефекте. Ошибка возможна только в сторону пропуска дефекта.
GRACE = 0.3


class _Probe:
    """Задача, блокирующаяся на событии и считающая запуски."""

    def __init__(self) -> None:
        self.release = Event()
        self.started = 0
        self.active = 0
        self.peak = 0
        self._cond = Condition()

    def __call__(self) -> None:
        with self._cond:
            self.started += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
            self._cond.notify_all()
        self.release.wait(TIMEOUT)
        with self._cond:
            self.active -= 1

    def wait_started(self, count: int) -> bool:
        with self._cond:
            return self._cond.wait_for(lambda: self.started >= count, TIMEOUT)


def _wait_for(condition: Callable[[], bool]) -> bool:
    deadline = time.monotonic() + TIMEOUT
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def _skip_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "skipped" in r.getMessage()
    ]


def _check_skips_while_running(
    caplog: pytest.LogCaptureFixture,
    add: Callable[[Scheduler, _Probe], None],
) -> None:
    probe = _Probe()
    scheduler = Scheduler(workers_num=2)
    add(scheduler, probe)
    with caplog.at_level(logging.WARNING):
        scheduler.start()
        try:
            assert probe.wait_started(1)
            # Следующее время запуска наступило, а предыдущий запуск идёт.
            assert _wait_for(lambda: bool(_skip_warnings(caplog)))
            assert probe.started == 1
            assert probe.peak == 1
            assert "_Probe" in _skip_warnings(caplog)[0]

            probe.release.set()
            # Следующий запуск состоялся: задача не заблокирована навсегда.
            assert probe.wait_started(2)
            assert probe.peak == 1
        finally:
            probe.release.set()
            scheduler.stop(block=True, timeout=TIMEOUT)


def test__by_period__skips_run_while_previous_in_progress(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def add(scheduler: Scheduler, probe: _Probe) -> None:
        scheduler.by_period(0.05, probe)

    _check_skips_while_running(caplog, add)


def test__by_cron__skips_run_while_previous_in_progress(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Тот же путь кода, что и у by_period, но с реальным CRON-расписанием
    # (шаг 1 с, тест занимает 2-3 с).
    def add(scheduler: Scheduler, probe: _Probe) -> None:
        scheduler.by_cron("* * * * * *", probe)

    _check_skips_while_running(caplog, add)


def test__skip__is_per_task(caplog: pytest.LogCaptureFixture) -> None:
    scheduler = Scheduler(workers_num=2)
    long_job = _Probe()
    other = _Probe()
    other.release.set()
    scheduler.by_period(0.05, long_job)
    scheduler.by_period(0.05, other)
    with caplog.at_level(logging.WARNING):
        scheduler.start()
        try:
            assert long_job.wait_started(1)
            # Вторая задача продолжает запускаться, пока первая занята.
            assert other.wait_started(3)
            assert long_job.started == 1
            assert _skip_warnings(caplog)
        finally:
            long_job.release.set()
            scheduler.stop(block=True, timeout=TIMEOUT)


def test__restart__run_cancelled_by_stop_does_not_block_task() -> None:
    scheduler = Scheduler(workers_num=1)
    blocker = _Probe()
    ran = Event()
    scheduler.with_delay(0, blocker)
    scheduler.start()
    try:
        assert blocker.wait_started(1)
        # Единственный воркер занят: запуск задачи встаёт в очередь пула и
        # будет отменён при остановке.
        scheduler.by_period(0.05, ran.set)
        Event().wait(GRACE)

        scheduler.stop()
        while scheduler.is_alive():
            Event().wait(0.01)
        Event().wait(GRACE)
        blocker.release.set()
        scheduler.join(TIMEOUT)
        assert not ran.is_set()

        scheduler.start()
        assert ran.wait(TIMEOUT)
    finally:
        blocker.release.set()
        scheduler.stop(block=True, timeout=TIMEOUT)


def test__no_pool__runs_are_not_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    scheduler = Scheduler(workers_num=0)
    runs = Event()
    count = [0]

    def job() -> None:
        count[0] += 1
        Event().wait(0.05)
        if count[0] >= 3:
            runs.set()

    scheduler.by_period(0.2, job)
    with caplog.at_level(logging.WARNING):
        scheduler.start()
        try:
            assert runs.wait(TIMEOUT)
        finally:
            scheduler.stop(block=True, timeout=TIMEOUT)
    assert not _skip_warnings(caplog)
