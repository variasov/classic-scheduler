import logging
from threading import Event

import pytest

from classic.scheduler import Scheduler

TIMEOUT = 10
LOGGER = "classic.scheduler"


class _Record(logging.Handler):
    """Хэндлер, выставляющий Event после записи с заданной подстрокой."""

    def __init__(self, event: Event, needle: str) -> None:
        super().__init__()
        self.event = event
        self.needle = needle

    def emit(self, record: logging.LogRecord) -> None:
        if self.needle in record.getMessage():
            self.event.set()


def _run_and_wait_completed(workers_num: int = 0) -> None:
    """Выполняет задачу `job` и ждёт записи о её завершении."""
    done = Event()
    handler = _Record(done, "completed")
    logger = logging.getLogger(LOGGER)
    logger.addHandler(handler)
    scheduler = Scheduler(workers_num=workers_num)
    scheduler.with_delay(0, job)
    scheduler.start()
    try:
        assert done.wait(TIMEOUT)
    finally:
        scheduler.stop(block=True, timeout=TIMEOUT)
        logger.removeHandler(handler)


def job() -> None:
    pass


def test__job_exception__logged_as_error_and_scheduler_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    failed = Event()
    after = Event()
    logger = logging.getLogger(LOGGER)
    handler = _Record(failed, "Unexpected error")
    logger.addHandler(handler)

    def broken() -> None:
        raise ValueError("boom")

    scheduler = Scheduler()
    scheduler.with_delay(0, broken)
    scheduler.with_delay(0.05, after.set)
    try:
        with caplog.at_level(logging.WARNING, logger=LOGGER):
            scheduler.start()
            assert failed.wait(TIMEOUT)
            assert after.wait(TIMEOUT)
            assert scheduler.is_alive()
    finally:
        scheduler.stop(block=True, timeout=TIMEOUT)
        logger.removeHandler(handler)

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    record = errors[0]
    assert record.name == "classic.scheduler.task"
    assert "broken" in record.getMessage()
    assert record.exc_info is not None
    assert record.exc_info[0] is ValueError


def test__skip__logged_as_warning_by_named_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    release = Event()
    started = Event()
    skipped = Event()
    logger = logging.getLogger(LOGGER)
    handler = _Record(skipped, "skipped")
    logger.addHandler(handler)

    def slow() -> None:
        started.set()
        release.wait(TIMEOUT)

    scheduler = Scheduler(workers_num=1)
    scheduler.by_period(0.05, slow)
    try:
        with caplog.at_level(logging.WARNING, logger=LOGGER):
            scheduler.start()
            assert started.wait(TIMEOUT)
            assert skipped.wait(TIMEOUT)
    finally:
        release.set()
        scheduler.stop(block=True, timeout=TIMEOUT)
        logger.removeHandler(handler)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings
    assert warnings[0].name == "classic.scheduler.scheduler"
    assert "slow" in warnings[0].getMessage()


def test__run__start_and_completion_logged_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        _run_and_wait_completed()

    records = [r for r in caplog.records if "job" in r.getMessage()]
    assert [r.levelno for r in records] == [logging.DEBUG, logging.DEBUG]
    assert all(r.name == "classic.scheduler.task" for r in records)
    assert "started" in records[0].getMessage()
    # Разовая задача к моменту запуска уже без следующего времени.
    assert "schedule [None]" in records[0].getMessage()
    assert "completed" in records[1].getMessage()


def test__run__start_and_completion_hidden_at_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    done = Event()
    logger = logging.getLogger(LOGGER)
    handler = _Record(done, "completed")
    logger.addHandler(handler)
    try:
        with caplog.at_level(logging.INFO, logger=LOGGER):
            scheduler = Scheduler()
            scheduler.with_delay(0, job)
            scheduler.start()
            # DEBUG-записи не проходят фильтр уровня, поэтому ждём по факту
            # выполнения задачи через отдельную задачу.
            ran = Event()
            scheduler.with_delay(0.1, ran.set)
            assert ran.wait(TIMEOUT)
            scheduler.stop(block=True, timeout=TIMEOUT)
    finally:
        logger.removeHandler(handler)

    assert not caplog.records


def test__logging__never_uses_root_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    release = Event()
    started = Event()
    skipped = Event()
    logger = logging.getLogger(LOGGER)
    handler = _Record(skipped, "skipped")
    logger.addHandler(handler)

    def slow() -> None:
        started.set()
        release.wait(TIMEOUT)

    def broken() -> None:
        raise ValueError("boom")

    scheduler = Scheduler(workers_num=2)
    scheduler.with_delay(0, broken)
    scheduler.by_period(0.05, slow)
    try:
        with caplog.at_level(logging.DEBUG):
            scheduler.start()
            assert started.wait(TIMEOUT)
            assert skipped.wait(TIMEOUT)
    finally:
        release.set()
        scheduler.stop(block=True, timeout=TIMEOUT)
        logger.removeHandler(handler)

    levels = {r.levelno for r in caplog.records}
    assert {logging.DEBUG, logging.WARNING, logging.ERROR} <= levels
    assert all(r.name.startswith("classic.scheduler.") for r in caplog.records)


def test__periodic_start__logs_next_run_time(
    caplog: pytest.LogCaptureFixture,
) -> None:
    started = Event()
    logger = logging.getLogger(LOGGER)
    handler = _Record(started, "started")
    logger.addHandler(handler)
    scheduler = Scheduler()
    scheduler.by_period(3600, job)
    try:
        with caplog.at_level(logging.DEBUG, logger=LOGGER):
            scheduler.start()
            assert started.wait(TIMEOUT)
    finally:
        scheduler.stop(block=True, timeout=TIMEOUT)
        logger.removeHandler(handler)

    message = next(
        r.getMessage() for r in caplog.records if "started" in r.getMessage()
    )
    assert "schedule [None]" not in message
    assert "20" in message
