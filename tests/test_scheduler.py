import time

from sources.classic.scheduler import Scheduler

a = 2


def test_scheduler():

    def func():
        global a
        a = 3

    assert a == 2

    scheduler = Scheduler()
    scheduler.with_delay(0, func)
    scheduler.run()
    time.sleep(2)
    scheduler.stop()

    assert a == 3
