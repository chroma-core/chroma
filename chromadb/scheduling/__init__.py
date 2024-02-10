from abc import abstractmethod
from datetime import datetime
from typing import Callable, Optional

from chromadb.config import Component, System


class ChromaScheduler(Component):

    def __init__(self, system: System):
        super().__init__(system)

    @abstractmethod
    def schedule_interval(self, *, job: Callable[..., ...], interval: int) -> None:
        """
        Schedule a job to run on an interval. The interval is in seconds.
        """
        ...

    @abstractmethod
    def schedule_cron(self, *, job: Callable[..., ...], cron_expr: str) -> None:
        """
        Schedule a job to run on a cron expression.
        """
        ...

    @abstractmethod
    def schedule_once(self, *, job: Callable[..., ...], delay: Optional[int], date_time: Optional[datetime]) -> None:
        """
        Schedule a job to run once after a delay or at a specific date/time.
        If both delay and date_time are None, raise an error.
        """
        ...
