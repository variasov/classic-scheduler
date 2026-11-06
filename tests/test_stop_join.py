from collections.abc import Callable
from threading import Event, Timer

import pytest

from classic.scheduler import Scheduler

TIMEOUT = 10
# Страховочный таймер: освобождает заблокированную задачу, чтобы тест
# не зависал, если проверяемое поведение сломано.
SAFETY = 2.0


def _safety_timer(release: Event) -> Timer:
    timer = Timer(SAFETY, release.set)
    timer.daemon = True
    timer.start()
    return timer


def _blocking_job(
    started: Event, release: Event, finished: Event
) -> Callable[[], None]:
    def blocking() -> None:
        started.set()
        release.wait(TIMEOUT)
        finished.set()

    return blocking


# --- команды, пришедшие после остановки ---


def _block_scheduler_thread(scheduler: Scheduler) -> Event:
    """Занимает поток планировщика (workers_num=0) и возвращает Event
    для его освобождения: пока поток занят, все команды копятся в очереди и
    разбираются уже после остановки.
    """
    started, release = Event(), Event()

    def blocker() -> None:
        started.set()
        release.wait(TIMEOUT)

    scheduler.start()
    scheduler.with_delay(0, blocker)
    assert started.wait(TIMEOUT)
    return release


def test__stop__keeps_tasks_added_after_stop() -> None:
    scheduler = Scheduler(workers_num=0)
    release = _block_scheduler_thread(scheduler)
    executed = Event()

    scheduler.stop()
    scheduler.with_delay(0, executed.set)
    release.set()
    scheduler.join(TIMEOUT)
    assert not scheduler.is_alive()
    assert not executed.is_set()

    scheduler.start()
    assert executed.wait(TIMEOUT)
    assert scheduler.is_alive()
    scheduler.stop(block=True, timeout=TIMEOUT)


def test__stop__applies_cancel_sent_after_stop() -> None:
    scheduler = Scheduler(workers_num=0)
    cancelled, sentinel = Event(), Event()
    job = scheduler.with_delay(1, cancelled.set)
    release = _block_scheduler_thread(scheduler)

    scheduler.stop()
    job.cancel()
    release.set()
    scheduler.join(TIMEOUT)
    # Срок задачи x наступает, пока планировщик остановлен.
    Event().wait(1.2)
    scheduler.start()
    scheduler.with_delay(0, sentinel.set)

    # Задача x (если бы осталась) просрочена и выполнилась бы раньше sentinel.
    assert sentinel.wait(TIMEOUT)
    assert not cancelled.is_set()
    scheduler.stop(block=True, timeout=TIMEOUT)


def test__stop__extra_stop_commands_do_not_affect_restart() -> None:
    scheduler = Scheduler(workers_num=0)
    release = _block_scheduler_thread(scheduler)
    first, second = Event(), Event()

    scheduler.stop()
    scheduler.stop()
    scheduler.with_delay(0, first.set)
    scheduler.stop()
    release.set()
    scheduler.join(TIMEOUT)
    assert not scheduler.is_alive()

    scheduler.start()
    assert first.wait(TIMEOUT)
    # Оставшийся в очереди Stop остановил бы перезапущенный планировщик
    # до выполнения этой задачи.
    scheduler.with_delay(0, second.set)
    assert second.wait(TIMEOUT)
    assert scheduler.is_alive()
    scheduler.stop(block=True, timeout=TIMEOUT)


# --- run ---


def test__run__suppresses_keyboard_interrupt() -> None:
    scheduler = Scheduler(workers_num=0)

    def interrupt() -> None:
        raise KeyboardInterrupt

    scheduler.with_delay(0, interrupt)

    scheduler.run()

    assert not scheduler.is_alive()


def test__run__already_running_raises() -> None:
    scheduler = Scheduler()
    scheduler.start()

    try:
        with pytest.raises(RuntimeError):
            scheduler.run()
    finally:
        scheduler.stop(block=True, timeout=TIMEOUT)


# --- stop / join ---


def test__stop__does_not_block_by_default() -> None:
    scheduler = Scheduler(workers_num=1)
    scheduler.start()
    started, release, finished = Event(), Event(), Event()
    scheduler.with_delay(0, _blocking_job(started, release, finished))
    assert started.wait(TIMEOUT)
    timer = _safety_timer(release)

    scheduler.stop()

    assert not finished.is_set()
    release.set()
    scheduler.join(TIMEOUT)
    timer.cancel()
    assert finished.is_set()


def test__stop__timeout_limits_waiting() -> None:
    scheduler = Scheduler(workers_num=1)
    scheduler.start()
    started, release, finished = Event(), Event(), Event()
    scheduler.with_delay(0, _blocking_job(started, release, finished))
    assert started.wait(TIMEOUT)
    timer = _safety_timer(release)

    scheduler.stop(block=True, timeout=0.1)

    assert not finished.is_set()
    release.set()
    scheduler.join(TIMEOUT)
    timer.cancel()
    assert finished.is_set()


def test__join__timeout_limits_waiting() -> None:
    scheduler = Scheduler(workers_num=1)
    scheduler.start()
    started, release, finished = Event(), Event(), Event()
    scheduler.with_delay(0, _blocking_job(started, release, finished))
    assert started.wait(TIMEOUT)
    timer = _safety_timer(release)
    scheduler.stop()

    scheduler.join(timeout=0.1)

    assert not finished.is_set()
    release.set()
    scheduler.join(TIMEOUT)
    timer.cancel()
    assert finished.is_set()


def test__join__without_thread_is_noop() -> None:
    Scheduler().join(0.1)


# --- быстрое выполнение просроченных задач ---


def test__overdue_tasks__run_without_waiting() -> None:
    """Просроченная задача выполняется сразу, а не через секунду ожидания.

    Каждое звено цепочки добавляет следующую просроченную задачу; при
    ожидании хотя бы секунды на звено цепочка заняла бы больше границы.
    Граница намеренно щедрая (штатно цепочка занимает миллисекунды), так
    что это единственная проверка с верхней границей времени.
    """
    hops = 5
    scheduler = Scheduler()
    done = Event()
    count = 0

    def hop() -> None:
        nonlocal count
        count += 1
        if count == hops:
            done.set()
        else:
            scheduler.with_delay(-1, hop)

    scheduler.start()
    scheduler.with_delay(-1, hop)

    assert done.wait(hops - 1)
    scheduler.stop(block=True, timeout=TIMEOUT)
