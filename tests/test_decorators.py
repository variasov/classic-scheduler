import time
from datetime import datetime

from classic.components import component
from classic.scheduler import Scheduler, by_cron, by_period


@component
class SomeComponent:
    periodic_value: datetime = None
    cron_value: datetime = None

    @by_period(1)
    def periodic(self):
        self.periodic_value = datetime.now()

    @by_cron('* * * * * *')
    def cron(self):
        self.cron_value = datetime.now()


def test_autoregister(scheduler: Scheduler) -> None:
    obj = SomeComponent(scheduler=scheduler)

    time.sleep(2)

    assert obj.scheduler is scheduler
    assert obj.periodic_value is not None
    assert obj.cron_value is not None
