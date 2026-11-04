from collections.abc import Iterator

import pytest
from classic.scheduler import Scheduler


@pytest.fixture
def scheduler() -> Iterator[Scheduler]:
    scheduler = Scheduler()
    scheduler.start()

    yield scheduler

    scheduler.stop(block=True, timeout=5)
