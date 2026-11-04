"""Простой планировщик задач, работающий внутри процесса.

Планировщик ``Scheduler`` вызывает переданные функции по расписанию:
один раз через отсрочку (``with_delay``), периодически (``by_period``)
или по CRON-выражению (``by_cron``). Работает в текущем потоке (``run()``)
либо в фоновом (``start()``), задачи выполняются в пуле потоков. Методы
добавления возвращают ``ScheduledJob``, у которого есть ``cancel()``.

Example:
    ::

        from classic.scheduler import Scheduler

        def ping() -> None:
            print("ping")

        if __name__ == "__main__":
            scheduler = Scheduler()
            job = scheduler.by_period(10, ping)
            scheduler.run()  # job.cancel() отменяет задачу

"""

from .scheduler import ScheduledJob, Scheduler

__all__ = ["ScheduledJob", "Scheduler"]
