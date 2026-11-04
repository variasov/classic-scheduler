from datetime import timedelta, timezone

from classic.scheduler.task import CronTask


PLUS_5 = timezone(timedelta(hours=5))
PLUS_7 = timezone(timedelta(hours=7))


def job() -> None:
    pass


def test__cron__local_timezone_by_default() -> None:
    task = CronTask('0 9 * * *', job=job)

    assert task.next_run_time.astimezone().hour == 9
    assert task.next_run_time.astimezone().minute == 0


def test__cron__explicit_timezone() -> None:
    task = CronTask('0 9 * * *', job=job, tz=PLUS_5)

    assert task.next_run_time.astimezone(PLUS_5).hour == 9
    assert task.next_run_time.astimezone(timezone.utc).hour == 4


def test__cron__next_run_time_in_timezone() -> None:
    task = CronTask('0 9 * * *', job=job, tz=PLUS_7)
    first = task.next_run_time

    task.set_next_run_time()

    assert task.next_run_time - first == timedelta(days=1)
    assert task.next_run_time.astimezone(PLUS_7).hour == 9
