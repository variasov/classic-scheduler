from threading import Event, Thread

from classic.scheduler import Scheduler

TIMEOUT = 5


def test__stop__when_started_by_run() -> None:
    scheduler = Scheduler()
    thread = Thread(target=scheduler.run)
    thread.start()

    executed = Event()
    scheduler.with_delay(0, executed.set)
    assert executed.wait(TIMEOUT)
    assert scheduler.is_alive()

    scheduler.stop()
    thread.join(TIMEOUT)

    assert not thread.is_alive()
    assert not scheduler.is_alive()


def test__stop__when_started_by_start() -> None:
    scheduler = Scheduler()
    scheduler.start()
    assert scheduler.is_alive()

    scheduler.stop(block=True, timeout=TIMEOUT)

    assert not scheduler.is_alive()


def test__restart() -> None:
    scheduler = Scheduler()
    scheduler.start()
    scheduler.stop(block=True, timeout=TIMEOUT)

    scheduler.start()
    executed = Event()
    scheduler.with_delay(0, executed.set)

    assert executed.wait(TIMEOUT)
    scheduler.stop(block=True, timeout=TIMEOUT)


def test__stop__before_start_is_ignored() -> None:
    scheduler = Scheduler()
    scheduler.stop()

    scheduler.start()
    executed = Event()
    scheduler.with_delay(0, executed.set)

    assert executed.wait(TIMEOUT)
    assert scheduler.is_alive()
    scheduler.stop(block=True, timeout=TIMEOUT)


def test__tasks_survive_restart() -> None:
    scheduler = Scheduler()
    scheduler.start()
    scheduler.stop(block=True, timeout=TIMEOUT)

    executed = Event()
    scheduler.with_delay(0, executed.set)
    assert not executed.wait(0.1)

    scheduler.start()
    assert executed.wait(TIMEOUT)
    scheduler.stop(block=True, timeout=TIMEOUT)


def test__start__thread_is_not_daemon_by_default() -> None:
    scheduler = Scheduler()
    scheduler.start()
    assert scheduler._thread is not None
    assert not scheduler._thread.daemon
    scheduler.stop(block=True, timeout=TIMEOUT)

    scheduler.start(daemon=True)
    assert scheduler._thread.daemon
    scheduler.stop(block=True, timeout=TIMEOUT)


def test__running_task_finishes_on_stop() -> None:
    scheduler = Scheduler()
    scheduler.start()

    started = Event()
    release = Event()
    finished = Event()

    def task() -> None:
        started.set()
        release.wait(TIMEOUT)
        finished.set()

    scheduler.with_delay(0, task)
    assert started.wait(TIMEOUT)

    scheduler.stop()
    release.set()
    scheduler.join(TIMEOUT)

    assert finished.is_set()
    assert not scheduler.is_alive()
