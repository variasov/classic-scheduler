"""Простой планировщик задач, работающий внутри процесса.

Планировщик ``Scheduler`` вызывает переданные функции по расписанию:
один раз через отсрочку (``with_delay``), периодически (``by_period``)
или по CRON-выражению (``by_cron``). Работает в текущем потоке (``run()``)
либо в фоновом (``start()``), задачи выполняются в пуле потоков.

Example:
    ::

        from classic.scheduler import Scheduler

        def ping() -> None:
            print("ping")

        if __name__ == "__main__":
            scheduler = Scheduler()
            scheduler.by_period(10, ping)
            scheduler.run()

"""

from .scheduler import Scheduler

__all__ = ["Scheduler"]
