from datetime import datetime, timedelta, timezone

from classic.scheduler.task import CronTask

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
