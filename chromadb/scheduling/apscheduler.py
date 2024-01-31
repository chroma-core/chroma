from datetime import datetime, timedelta
from typing import Callable, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from overrides import override

from chromadb.config import System
from chromadb.scheduling import ChromaScheduler


class APScheduler(ChromaScheduler):
    """A scheduler that uses APScheduler to schedule jobs."""
    def __init__(self, system: System):
        super().__init__(system)
        self.scheduler = BackgroundScheduler()

    @override
    def start(self) -> None:
        print("Starting scheduler")
        self.scheduler.start()

    @override
    def stop(self) -> None:
        self.scheduler.shutdown()

    @override
    def schedule_interval(self, *, job: Callable[..., None], interval: int) -> None:
        if interval <= 0:
            raise ValueError("Interval must be positive")
        self.scheduler.add_job(func=job, trigger=IntervalTrigger(seconds=interval))

    @override
    def schedule_cron(self, *, job: Callable[..., None], cron_expr: str) -> None:
        self.scheduler.add_job(func=job, trigger=CronTrigger.from_crontab(cron_expr))

    @override
    def schedule_once(self, *, job: Callable[..., None], delay: Optional[int], date_time: Optional[datetime]) -> None:
        if delay is not None:
            self.scheduler.add_job(func=job, trigger=DateTrigger(run_date=datetime.now() + timedelta(seconds=delay)))
        elif date_time is not None:
            self.scheduler.add_job(func=job, trigger=DateTrigger(run_date=date_time))
        else:
            raise ValueError("Must specify either delay or date_time")

