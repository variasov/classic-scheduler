import pytest

from classic.scheduler import Scheduler


@pytest.fixture(scope='function')
def scheduler() -> Scheduler:
    scheduler = Scheduler()
    scheduler.start()

    yield scheduler

    scheduler.stop()
